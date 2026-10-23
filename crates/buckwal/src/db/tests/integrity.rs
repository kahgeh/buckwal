//! Spec item 4: an encrypted bucket's pages, log records, manifests and head carry integrity
//! codes, and a copy, replay or edit in the bucket fails every read with
//! `Corrupt("integrity: <key>")`. Each tamper runs in WAL and in MVCC mode, against the
//! readers that fetch the object (a read-only open with lazy pages, a restore, a takeover).
use super::mvcc::{mvcc_enc, truncate_checkpoint};
use super::*;
use crate::format::Manifest;
use sha2::{Digest, Sha256};

#[derive(Clone, Copy, Debug)]
enum Mode {
    Wal,
    Mvcc,
}

const MODES: [Mode; 2] = [Mode::Wal, Mode::Mvcc];

fn cfg_for(mode: Mode, dir: &Path, k: &TempDir) -> Config {
    match mode {
        Mode::Wal => enc_config(dir, k, &key_hex()),
        Mode::Mvcc => mvcc_enc(dir, k, &key_hex()),
    }
}

/// A row of about 400 bytes, so 45 rows span several pages.
fn put_row(mode: Mode, c: &Connection, id: i64) {
    let sql = "INSERT INTO t(id, v) VALUES (?, ?)";
    let params = [
        Value::from_i64(id),
        Value::build_text(format!("row {id} {}", "x".repeat(400))),
    ];
    match mode {
        Mode::Wal => drop(c.execute(sql, &params).unwrap()),
        Mode::Mvcc => {
            c.execute("BEGIN CONCURRENT", &[]).unwrap();
            c.execute(sql, &params).unwrap();
            c.execute("COMMIT", &[]).unwrap();
        }
    }
}

/// An encrypted writer left running: rows 1..=20, a seal, rows 21..=40 (the same pages again),
/// a second seal, then rows 41..=45 that stay in the log. Its objects are the tamper targets.
fn writer(mode: Mode, store: &Arc<MemStore>, k: &TempDir) -> TempDir {
    let dir = tempdir();
    let db = open_with_store(cfg_for(mode, dir.path(), k), store.clone()).unwrap();
    let c = db.connect().unwrap();
    create(&c);
    for id in 1..=20 {
        put_row(mode, &c, id);
    }
    match mode {
        Mode::Wal => c.checkpoint(CheckpointMode::Truncate).unwrap(),
        Mode::Mvcc => truncate_checkpoint(&c).unwrap(),
    }
    for id in 21..=40 {
        put_row(mode, &c, id);
    }
    match mode {
        Mode::Wal => c.checkpoint(CheckpointMode::Truncate).unwrap(),
        Mode::Mvcc => truncate_checkpoint(&c).unwrap(),
    }
    for id in 41..=45 {
        put_row(mode, &c, id);
    }
    std::mem::forget(c);
    std::mem::forget(db);
    dir
}

/// How a tampered bucket is read.
#[derive(Clone, Copy, Debug)]
enum Reader {
    /// A read-only open of a fresh directory (restore, lazy page reads), reading every row.
    ReadOnly,
    /// `restore_with_store`.
    Restore,
    /// `takeover_with_store`.
    Takeover,
    /// `gc_with_store`: reads the head.
    Gc,
}

/// Rows read (0 for a reader that reads none).
fn read_with(reader: Reader, mode: Mode, store: &Arc<MemStore>, k: &TempDir) -> Result<usize> {
    let t = tempdir();
    let mut c = cfg_for(mode, &fresh(&t), k);
    c.prefetch = false;
    match reader {
        Reader::ReadOnly => {
            let db = open_read_only_with_store(c, store.clone())?;
            let conn = db.connect()?;
            let rows = conn.query("SELECT id, v FROM t ORDER BY id", &[])?;
            Ok(rows.len())
        }
        Reader::Restore => restore_with_store(&c, store.clone(), &fresh(&t)).map(|_| 0),
        Reader::Takeover => crate::takeover_with_store(c, store.clone()).map(|_| 0),
        Reader::Gc => crate::gc_with_store(&c, store.clone()).map(|_| 0),
    }
}

/// The reader fails with exactly `integrity: <key>` for one of `keys`: no page bytes, no code.
fn assert_integrity(r: Result<usize>, keys: &[String], what: &str) {
    match r {
        Err(Error::Corrupt(m)) => {
            let ok = keys.iter().any(|k| m == format!("integrity: {k}"));
            assert!(ok, "{what}: {m:?} is not an integrity error for {keys:?}");
        }
        r => panic!("{what}: expected Corrupt(integrity: ..), got {r:?}"),
    }
}

/// Every page object: (key, generation, page).
fn page_objects(store: &MemStore) -> Vec<(String, u64, u32)> {
    store
        .list(&keys().page_root())
        .unwrap()
        .into_iter()
        .filter_map(|k| keys().parse_page(&k).map(|(g, p)| (k, g, p)))
        .collect()
}

fn body(store: &MemStore, key: &str) -> Vec<u8> {
    store.get(key).unwrap().unwrap().body
}

fn put(store: &MemStore, key: &str, body: &[u8]) {
    store.put(key, body).unwrap();
}

#[test]
fn untouched_encrypted_bucket_restores_and_verifies() {
    for mode in MODES {
        let store = mem();
        let k = tempdir();
        let _w = writer(mode, &store, &k);
        for r in [
            Reader::ReadOnly,
            Reader::Restore,
            Reader::Takeover,
            Reader::Gc,
        ] {
            let rows =
                read_with(r, mode, &store, &k).unwrap_or_else(|e| panic!("{mode:?} {r:?}: {e}"));
            if let Reader::ReadOnly = r {
                assert_eq!(rows, 45, "{mode:?}");
            }
        }
    }
}

/// The newest manifest, decoded as an attacker without the key reads it (the code is not the
/// SHA-256 trailer, so it is replaced by one).
fn current_manifest(store: &MemStore) -> (u64, Manifest) {
    let g = head(store).manifest.expect("sealed");
    let real = body(store, &keys().manifest(g));
    let mut plain = real[..real.len() - 32].to_vec();
    plain.extend(Sha256::digest(&plain));
    (g, Manifest::decode(&plain).unwrap())
}

#[test]
fn swapped_pages_fail_the_read() {
    for mode in MODES {
        let store = mem();
        let k = tempdir();
        let _w = writer(mode, &store, &k);
        // The objects the head's manifest maps its two highest pages (table leaves) to.
        let (_, m) = current_manifest(&store);
        let mut top: Vec<u32> = m.pages.keys().copied().collect();
        top.sort_unstable();
        let (p, q) = (top[top.len() - 1], top[top.len() - 2]);
        assert!(q >= 3, "{mode:?}: only pages {top:?}");
        let (a, b) = (keys().page(m.pages[&p], p), keys().page(m.pages[&q], q));
        let (ba, bb) = (body(&store, &a), body(&store, &b));
        put(&store, &a, &bb);
        put(&store, &b, &ba);
        let what = format!("{mode:?} swap");
        let targets = [a.clone(), b.clone()];
        assert_integrity(
            read_with(Reader::ReadOnly, mode, &store, &k),
            &targets,
            &what,
        );
    }
}

#[test]
fn an_older_generations_page_put_back_fails_the_read() {
    for mode in MODES {
        let store = mem();
        let k = tempdir();
        let _w = writer(mode, &store, &k);
        let pages = page_objects(&store);
        // A page number sealed in two generations: the old object goes over the one the
        // head's manifest maps.
        let (_, m) = current_manifest(&store);
        let (p, ng) = m
            .pages
            .iter()
            .map(|(p, g)| (*p, *g))
            .find(|(p, g)| pages.iter().any(|(_, og, op)| op == p && og < g))
            .unwrap_or_else(|| panic!("{mode:?}: no page sealed twice: {pages:?}"));
        let old = pages
            .iter()
            .filter(|(_, og, op)| *op == p && *og < ng)
            .map(|(k, _, _)| k.clone())
            .next()
            .unwrap();
        let new = keys().page(ng, p);
        let b = body(&store, &old);
        assert_ne!(
            b,
            body(&store, &new),
            "{mode:?}: the two versions are equal"
        );
        put(&store, &new, &b);
        assert_integrity(
            read_with(Reader::ReadOnly, mode, &store, &k),
            std::slice::from_ref(&new),
            &format!("{mode:?} old page put back over {new}"),
        );
    }
}

#[test]
fn a_record_replayed_at_another_seq_fails_the_read() {
    for mode in MODES {
        let store = mem();
        let k = tempdir();
        let _w = writer(mode, &store, &k);
        let g = head(&*store).generation;
        let seqs = {
            let mut s = log_seqs(&*store, g);
            s.sort_unstable();
            s
        };
        assert!(seqs.len() >= 3, "{mode:?}: log/{g} holds {seqs:?}");
        let (first, last) = (seqs[1], *seqs.last().unwrap());
        let (src, dst) = (keys().log(g, first), keys().log(g, last));
        let b = body(&store, &src);
        assert_ne!(b, body(&store, &dst));
        put(&store, &dst, &b);
        let what = format!("{mode:?} record {first} replayed at {last}");
        for r in [Reader::ReadOnly, Reader::Restore, Reader::Takeover] {
            assert_integrity(
                read_with(r, mode, &store, &k),
                std::slice::from_ref(&dst),
                &what,
            );
        }
    }
}

#[test]
fn an_edited_manifest_fails_the_read() {
    for mode in MODES {
        let store = mem();
        let k = tempdir();
        let _w = writer(mode, &store, &k);
        let g = head(&*store).manifest.unwrap();
        let key = keys().manifest(g);
        // An attacker without the key re-encodes it with a plain SHA-256 trailer, page 2
        // pointed at the oldest generation that holds it.
        let (_, mut m) = current_manifest(&store);
        let oldest = page_objects(&store)
            .iter()
            .filter(|(_, _, p)| *p == 2)
            .map(|(_, g, _)| *g)
            .min()
            .unwrap();
        m.pages.insert(2, oldest);
        let forged = m.encode();
        put(&store, &key, &forged);
        let what = format!("{mode:?} manifest");
        for r in [Reader::ReadOnly, Reader::Restore, Reader::Takeover] {
            assert_integrity(
                read_with(r, mode, &store, &k),
                std::slice::from_ref(&key),
                &what,
            );
        }
    }
}

#[test]
fn an_edited_head_fails_the_read() {
    for mode in MODES {
        let store = mem();
        let k = tempdir();
        let _w = writer(mode, &store, &k);
        let key = keys().head();
        let real = String::from_utf8(body(&store, &key)).unwrap();
        let epoch = head(&*store).epoch;
        let edited = real.replace(
            &format!("\"epoch\":{epoch}"),
            &format!("\"epoch\":{}", epoch + 1),
        );
        assert_ne!(edited, real);
        put(&store, &key, edited.as_bytes());
        let what = format!("{mode:?} head");
        for r in [
            Reader::ReadOnly,
            Reader::Restore,
            Reader::Takeover,
            Reader::Gc,
        ] {
            assert_integrity(
                read_with(r, mode, &store, &k),
                std::slice::from_ref(&key),
                &what,
            );
        }
    }
}

#[test]
fn a_page_object_shorter_than_a_code_fails_without_a_panic() {
    for mode in MODES {
        let store = mem();
        let k = tempdir();
        let _w = writer(mode, &store, &k);
        let (_, m) = current_manifest(&store);
        let key = keys().page(m.pages[&1], 1);
        for len in [0usize, 10, 31] {
            put(&store, &key, &vec![7u8; len]);
            assert_integrity(
                read_with(Reader::Restore, mode, &store, &k),
                std::slice::from_ref(&key),
                &format!("{mode:?} page of {len} bytes"),
            );
        }
    }
}

/// An encrypted database under a format 1 or 2 head: refused by this build.
#[test]
fn an_encrypted_open_refuses_a_head_written_before_integrity_codes() {
    for mode in MODES {
        let store = mem();
        let k = tempdir();
        let _w = writer(mode, &store, &k);
        let key = keys().head();
        let h = head(&*store);
        for format in [1u32, 2] {
            let old = Head {
                format,
                journal_mode: if format == 1 {
                    JournalMode::Wal
                } else {
                    JournalMode::Mvcc
                },
                ..h.clone()
            };
            put(&store, &key, &old.encode());
            match read_with(Reader::Restore, mode, &store, &k) {
                Err(Error::Corrupt(m)) => {
                    assert_eq!(m, "integrity: bucket written before integrity codes")
                }
                r => panic!("{mode:?} format {format}: {r:?}"),
            }
        }
    }
}

/// The encrypted bucket's head is format 3 in both modes, naming the mode; a plaintext open
/// of it is refused.
#[test]
fn an_encrypted_bucket_has_a_format_3_head_and_a_keyless_open_is_refused() {
    for mode in MODES {
        let store = mem();
        let k = tempdir();
        let _w = writer(mode, &store, &k);
        let h = head(&*store);
        assert_eq!(h.format, 3, "{mode:?}");
        assert_eq!(
            h.journal_mode,
            match mode {
                Mode::Wal => JournalMode::Wal,
                Mode::Mvcc => JournalMode::Mvcc,
            }
        );
        let t = tempdir();
        let mut c = config(&fresh(&t));
        c.journal_mode = h.journal_mode;
        let r = restore_with_store(&c, store.clone(), &fresh(&t));
        match r {
            Err(Error::Encryption(m)) => {
                assert!(
                    m.contains("format 3") && m.contains("BUCKWAL_ENCRYPTION_KEY_FILE"),
                    "{m}"
                )
            }
            r => panic!("{mode:?}: {r:?}"),
        }
    }
}
