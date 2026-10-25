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
    cfg_with(mode, dir, k, &key_hex())
}

fn cfg_with(mode: Mode, dir: &Path, k: &TempDir, hex: &str) -> Config {
    match mode {
        Mode::Wal => enc_config(dir, k, hex),
        Mode::Mvcc => mvcc_enc(dir, k, hex),
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
    writer_at(mode, PREFIX, store, k)
}

/// `writer` for the database at bucket prefix `prefix`.
fn writer_at(mode: Mode, prefix: &str, store: &Arc<MemStore>, k: &TempDir) -> TempDir {
    let dir = tempdir();
    let mut c = cfg_for(mode, dir.path(), k);
    c.prefix = prefix.to_string();
    let db = open_with_store(c, store.clone()).unwrap();
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
    /// `gc_with_store`: reads the head, and the manifest it trims against.
    Gc,
    /// What `buckwal verify` does: restore into a directory, then a read-only open of it, a
    /// full read and `PRAGMA integrity_check`.
    Verify,
}

/// Rows read (0 for a reader that reads none).
fn read_with(reader: Reader, mode: Mode, store: &Arc<MemStore>, k: &TempDir) -> Result<usize> {
    read_with_key(reader, mode, store, k, &key_hex())
}

/// `read_with` with the key `hex` (written to `k/key`).
fn read_with_key(
    reader: Reader,
    mode: Mode,
    store: &Arc<MemStore>,
    k: &TempDir,
    hex: &str,
) -> Result<usize> {
    read_at(reader, mode, PREFIX, store, k, hex)
}

/// `read_with_key` for the database at bucket prefix `prefix`.
fn read_at(
    reader: Reader,
    mode: Mode,
    prefix: &str,
    store: &Arc<MemStore>,
    k: &TempDir,
    hex: &str,
) -> Result<usize> {
    let t = tempdir();
    let at = |dir: &Path| {
        let mut c = cfg_with(mode, dir, k, hex);
        c.prefix = prefix.to_string();
        c
    };
    let mut c = at(&fresh(&t));
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
        Reader::Verify => {
            let dir = fresh(&t);
            restore_with_store(&c, store.clone(), &dir)?;
            let db = open_read_only_with_store(at(&dir), store.clone())?;
            let conn = db.connect()?;
            let rows = conn.query("SELECT id, v FROM t ORDER BY id", &[])?;
            conn.query("PRAGMA integrity_check", &[])?;
            Ok(rows.len())
        }
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
    // A seal test of formats 1 to 4 (`bucket::legacy`).
    let _legacy = crate::format::LegacySeal::on();
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
    // A seal test of formats 1 to 4 (`bucket::legacy`).
    let _legacy = crate::format::LegacySeal::on();
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
    // A seal test of formats 1 to 4 (`bucket::legacy`).
    let _legacy = crate::format::LegacySeal::on();
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
    // A seal test of formats 1 to 4 (`bucket::legacy`).
    let _legacy = crate::format::LegacySeal::on();
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

/// The encrypted bucket's head is format 4 in both modes, naming the mode; a plaintext open
/// of it is refused.
#[test]
fn an_encrypted_bucket_has_a_format_4_head_and_a_keyless_open_is_refused() {
    // A seal test of formats 1 to 4 (`bucket::legacy`).
    let _legacy = crate::format::LegacySeal::on();
    for mode in MODES {
        let store = mem();
        let k = tempdir();
        let _w = writer(mode, &store, &k);
        let h = head(&*store);
        assert_eq!(h.format, 4, "{mode:?}");
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
                    m.contains("format 4") && m.contains("BUCKWAL_ENCRYPTION_KEY_FILE"),
                    "{m}"
                )
            }
            r => panic!("{mode:?}: {r:?}"),
        }
    }
}

/// `buckwal verify` over a swapped page and a replayed record fails like the open it wraps.
#[test]
fn verify_fails_over_tampered_objects() {
    // A seal test of formats 1 to 4 (`bucket::legacy`).
    let _legacy = crate::format::LegacySeal::on();
    for mode in MODES {
        let store = mem();
        let k = tempdir();
        let _w = writer(mode, &store, &k);
        assert_eq!(read_with(Reader::Verify, mode, &store, &k).unwrap(), 45);
        // A swap of the two highest pages.
        let (_, m) = current_manifest(&store);
        let mut top: Vec<u32> = m.pages.keys().copied().collect();
        top.sort_unstable();
        let (p, q) = (top[top.len() - 1], top[top.len() - 2]);
        let (a, b) = (keys().page(m.pages[&p], p), keys().page(m.pages[&q], q));
        let (ba, bb) = (body(&store, &a), body(&store, &b));
        put(&store, &a, &bb);
        put(&store, &b, &ba);
        assert_integrity(
            read_with(Reader::Verify, mode, &store, &k),
            &[a.clone(), b.clone()],
            &format!("{mode:?} verify swap"),
        );
        put(&store, &a, &ba);
        put(&store, &b, &bb);
        assert_eq!(read_with(Reader::Verify, mode, &store, &k).unwrap(), 45);
        // A record replayed at another seq.
        let g = head(&*store).generation;
        let mut seqs = log_seqs(&*store, g);
        seqs.sort_unstable();
        let (src, dst) = (keys().log(g, seqs[1]), keys().log(g, *seqs.last().unwrap()));
        let b = body(&store, &src);
        put(&store, &dst, &b);
        assert_integrity(
            read_with(Reader::Verify, mode, &store, &k),
            std::slice::from_ref(&dst),
            &format!("{mode:?} verify record"),
        );
    }
}

/// GC trims against a manifest once it is old enough, so it reads and checks that manifest.
#[test]
fn gc_over_a_tampered_manifest_fails() {
    // A seal test of formats 1 to 4 (`bucket::legacy`).
    let _legacy = crate::format::LegacySeal::on();
    for mode in MODES {
        let store = mem();
        let k = tempdir();
        let w = writer(mode, &store, &k);
        let g = head(&*store).manifest.unwrap();
        let key = keys().manifest(g);
        let (_, mut m) = current_manifest(&store);
        let oldest = page_objects(&store)
            .iter()
            .map(|(_, g, _)| *g)
            .min()
            .unwrap();
        m.pages.insert(2, oldest);
        // Untouched first: a pass over an old enough manifest reads it and passes.
        let min_age = cfg_for(mode, w.path(), &k)
            .engine(crate::io::IoMode::Writer)
            .gc_min_age();
        store.advance_clock(min_age + Duration::from_secs(1));
        crate::gc_with_store(&cfg_for(mode, w.path(), &k), store.clone()).unwrap();
        put(&store, &key, &m.encode());
        // The forged object is new: let it come of age.
        store.advance_clock(min_age + Duration::from_secs(1));
        assert_integrity(
            read_with(Reader::Gc, mode, &store, &k),
            std::slice::from_ref(&key),
            &format!("{mode:?} gc manifest"),
        );
    }
}

/// A format 3 head whose integrity field is removed is not an unchecked head.
#[test]
fn a_format_3_head_without_its_integrity_field_fails_the_read() {
    for mode in MODES {
        let store = mem();
        let k = tempdir();
        let _w = writer(mode, &store, &k);
        let key = keys().head();
        let real = String::from_utf8(body(&store, &key)).unwrap();
        let at = real
            .find(",\"integrity\"")
            .expect("a format 3 head is coded");
        let stripped = format!("{}}}", &real[..at]);
        put(&store, &key, stripped.as_bytes());
        for r in [
            Reader::ReadOnly,
            Reader::Restore,
            Reader::Takeover,
            Reader::Gc,
            Reader::Verify,
        ] {
            assert_integrity(
                read_with(r, mode, &store, &k),
                std::slice::from_ref(&key),
                &format!("{mode:?} stripped head, {r:?}"),
            );
        }
    }
}

// A wrong key is not tampering (spec rev 7): `key_check` says which key the bucket was written
// with, and is compared at every open before any integrity code.

const WRONG_KEY: &str =
    "the key in BUCKWAL_ENCRYPTION_KEY_FILE is not the key this bucket was written with";
const NO_CODES: &str =
    "the bucket has no integrity codes: it is plaintext, or was encrypted before this version";

const ALL_READERS: [Reader; 5] = [
    Reader::ReadOnly,
    Reader::Restore,
    Reader::Takeover,
    Reader::Gc,
    Reader::Verify,
];

/// The `integrity check failed` records emitted on this thread while `f` runs.
fn integrity_records<T>(f: impl FnOnce() -> T) -> (T, usize) {
    use crate::telemetry::testing::Capture;
    let cap = Capture::new();
    let out = tracing::subscriber::with_default(cap.subscriber(), f);
    let n = cap
        .spans()
        .events
        .iter()
        .filter(|e| e.field("message") == Some("integrity check failed"))
        .count();
    (out, n)
}

/// The refusal is `Encryption(want)` and says nothing of any key.
fn assert_encryption(r: Result<usize>, want: &str, what: &str) {
    match r {
        Err(Error::Encryption(m)) => {
            assert_eq!(m, want, "{what}");
            assert!(
                !m.contains(&key_hex()) && !m.contains(&"22".repeat(32)),
                "{what}"
            );
        }
        r => panic!("{what}: expected Encryption({want:?}), got {r:?}"),
    }
}

fn edit_head(store: &MemStore, f: impl Fn(&str) -> String) {
    let key = keys().head();
    let real = String::from_utf8(body(store, &key)).unwrap();
    let edited = f(&real);
    assert_ne!(edited, real, "the edit changed nothing: {real}");
    put(store, &key, edited.as_bytes());
}

/// The head with its `key_check` value's first hex digit changed.
fn flip_key_check(head: &str) -> String {
    let Some(at) = head.find("\"key_check\":\"").map(|i| i + 13) else {
        return head.to_string();
    };
    let c = &head[at..at + 1];
    let flipped = if c == "0" { "1" } else { "0" };
    format!("{}{flipped}{}", &head[..at], &head[at + 1..])
}

#[test]
fn a_different_valid_key_is_the_wrong_key_and_not_tampering() {
    for mode in MODES {
        let store = mem();
        let k = tempdir();
        let _w = writer(mode, &store, &k);
        let other = tempdir();
        for r in ALL_READERS {
            let (got, records) =
                integrity_records(|| read_with_key(r, mode, &store, &other, &"22".repeat(32)));
            let what = format!("{mode:?} {r:?}");
            assert_encryption(got, WRONG_KEY, &what);
            assert_eq!(records, 0, "{what}: a wrong key logged an integrity record");
        }
    }
}

#[test]
fn a_key_on_a_plaintext_bucket_has_no_integrity_codes() {
    let store = mem();
    let a = closed_writer(&store, 10);
    drop(a);
    for takeover in [false, true] {
        let k = tempdir();
        let t = tempdir();
        let cfg = enc_config(&fresh(&t), &k, &key_hex());
        let (got, records) = integrity_records(|| {
            if takeover {
                crate::takeover_with_store(cfg, store.clone()).map(|_| 0)
            } else {
                restore_with_store(&cfg, store.clone(), &fresh(&t)).map(|_| 0)
            }
        });
        assert_encryption(got, NO_CODES, &format!("takeover {takeover}"));
        assert_eq!(records, 0, "takeover {takeover}");
        no_database_files(&fresh(&t));
    }
}

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
            let (got, records) = integrity_records(|| read_with(Reader::Restore, mode, &store, &k));
            assert_encryption(got, NO_CODES, &format!("{mode:?} format {format}"));
            assert_eq!(records, 0);
        }
    }
}

/// Whoever edits `key_check` (or removes it) gets the wrong-key error at open, never an object
/// accepted unchecked, and nothing is restored.
#[test]
fn an_edited_key_check_is_the_wrong_key_error_and_nothing_is_restored() {
    for mode in MODES {
        for strip in [false, true] {
            let store = mem();
            let k = tempdir();
            let _w = writer(mode, &store, &k);
            edit_head(&store, |h| {
                if strip {
                    let at = h
                        .find(",\"key_check\"")
                        .expect("a format 3 head has key_check");
                    let end = h[at + 1..].find(',').map_or(h.len() - 1, |i| at + 1 + i);
                    format!("{}{}", &h[..at], &h[end..])
                } else {
                    flip_key_check(h)
                }
            });
            for r in ALL_READERS {
                let (got, records) = integrity_records(|| read_with(r, mode, &store, &k));
                let what = format!("{mode:?} strip {strip} {r:?}");
                assert_encryption(got, WRONG_KEY, &what);
                assert_eq!(records, 0, "{what}");
            }
            let t = tempdir();
            let dir = fresh(&t);
            let r = restore_with_store(&cfg_for(mode, &dir, &k), store.clone(), &dir);
            assert!(matches!(r, Err(Error::Encryption(_))), "{r:?}");
            no_database_files(&dir);
        }
    }
}

/// After a good open, a head that fails its code or its `key_check` is tampering: `Corrupt`
/// and one record (a later head read is the GC thread's, here a `gc_once` pass).
#[test]
fn after_a_good_open_an_edited_head_or_key_check_is_tampering() {
    for mode in MODES {
        let store = mem();
        let k = tempdir();
        let _w = writer(mode, &store, &k);
        let enc_keys = cfg_for(mode, _w.path(), &k).keys().unwrap();
        let key = keys().head();
        let good = body(&store, &key);
        let pass = || crate::gc::gc_once(&*store, &enc_keys, Duration::ZERO, None);
        pass().unwrap_or_else(|e| panic!("{mode:?}: the untouched head: {e}"));
        for what in ["key_check", "epoch"] {
            put(&store, &key, &good);
            edit_head(&store, |h| match what {
                "epoch" => {
                    let e = head(&*store).epoch;
                    h.replace(&format!("\"epoch\":{e}"), &format!("\"epoch\":{}", e + 1))
                }
                _ => flip_key_check(h),
            });
            let (got, records) = integrity_records(pass);
            match got {
                Err(Error::Corrupt(m)) => assert_eq!(m, format!("integrity: {key}"), "{what}"),
                r => panic!("{mode:?} {what}: expected Corrupt, got {:?}", r.map(|_| ())),
            }
            assert_eq!(records, 1, "{mode:?} {what}");
        }
    }
}

/// Task 5c: after a good open a plaintext-format head is a downgrade, an edited or stripped
/// `key_check` is tampering, each with exactly one record, and a restore refuses all of them.
#[test]
fn after_a_good_open_a_downgraded_or_stripped_head_is_tampering() {
    for mode in MODES {
        let store = mem();
        let k = tempdir();
        let _w = writer(mode, &store, &k);
        let enc_keys = cfg_for(mode, _w.path(), &k).keys().unwrap();
        let key = keys().head();
        let good = body(&store, &key);
        let h = head(&*store);
        let pass = || crate::gc::gc_once(&*store, &enc_keys, Duration::ZERO, None);
        pass().unwrap_or_else(|e| panic!("{mode:?}: the untouched head: {e}"));
        for what in [
            "format 1",
            "format 2",
            "key_check edited",
            "key_check stripped",
        ] {
            put(&store, &key, &good);
            match what {
                "format 1" | "format 2" => {
                    let format = if what == "format 1" { 1 } else { 2 };
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
                }
                "key_check edited" => edit_head(&store, flip_key_check),
                _ => edit_head(&store, |h| {
                    let at = h.find(",\"key_check\"").unwrap();
                    let end = h[at + 1..].find(",\"integrity\"").unwrap() + at + 1;
                    format!("{}{}", &h[..at], &h[end..])
                }),
            }
            let want = if what.starts_with("format") {
                format!("integrity: {key} format downgraded")
            } else {
                format!("integrity: {key}")
            };
            let (got, records) = integrity_records(pass);
            match got {
                Err(Error::Corrupt(m)) => assert_eq!(m, want, "{mode:?} {what}"),
                r => panic!("{mode:?} {what}: expected Corrupt, got {:?}", r.map(|_| ())),
            }
            assert_eq!(records, 1, "{mode:?} {what}");
            let r = read_with(Reader::Restore, mode, &store, &k);
            assert!(r.is_err(), "{mode:?} {what}: restore accepted the head");
        }
        put(&store, &key, &good);
        pass().unwrap_or_else(|e| panic!("{mode:?}: the head put back: {e}"));
    }
}

// ---------------------------------------------------------------------------------------
// bind-objects-to-database: the database id and the database integrity key (format 4).
// ---------------------------------------------------------------------------------------

/// The head at bucket prefix `prefix`, decoded without a key.
fn head_at(store: &MemStore, prefix: &str) -> Head {
    Head::decode(&body(store, &Keys::new(prefix).head())).unwrap()
}

/// The newest manifest at `prefix`, decoded as an attacker without the key reads it.
fn manifest_at(store: &MemStore, prefix: &str) -> (u64, Manifest) {
    let g = head_at(store, prefix).manifest.expect("sealed");
    let real = body(store, &Keys::new(prefix).manifest(g));
    let mut plain = real[..real.len() - 32].to_vec();
    plain.extend(Sha256::digest(&plain));
    (g, Manifest::decode(&plain).unwrap())
}

/// Every key under `prefix/`, sorted.
fn keys_under(store: &MemStore, prefix: &str) -> Vec<String> {
    store.list(&format!("{prefix}/")).unwrap()
}

/// Copy every object under `from/` of `src` to the same place under `to/` of `dst`, the head
/// included: a whole database copied (`aws s3 sync`).
fn copy_database(src: &MemStore, from: &str, dst: &MemStore, to: &str) {
    for key in keys_under(src, from) {
        let rest = key.strip_prefix(&format!("{from}/")).unwrap();
        put(dst, &format!("{to}/{rest}"), &body(src, &key));
    }
}

/// The three kinds of object of a sealed database at `prefix` that one database can hold at
/// the coordinates of another: (what, key).
fn copy_targets(store: &MemStore, prefix: &str) -> Vec<(&'static str, Vec<String>)> {
    let k = Keys::new(prefix);
    let (g, m) = manifest_at(store, prefix);
    // Every page the manifest maps: the read fetches those it needs, and the first copied one
    // it fetches fails.
    let pages: Vec<String> = m.pages.iter().map(|(p, pg)| k.page(*pg, *p)).collect();
    let head_gen = head_at(store, prefix).generation;
    let last_record = keys_under(store, prefix)
        .into_iter()
        .rfind(|x| k.parse_log(x).is_some_and(|(lg, _)| lg == head_gen))
        .expect("a record in the current generation");
    vec![
        ("page", pages),
        ("record", vec![last_record]),
        ("manifest", vec![k.manifest(g)]),
    ]
}

#[test]
fn an_object_copied_from_another_database_with_the_same_key_fails_its_code() {
    // A seal test of formats 1 to 4 (`bucket::legacy`).
    let _legacy = crate::format::LegacySeal::on();
    for mode in MODES {
        // Two databases, one key file, two prefixes of one bucket; and the same prefix in
        // another bucket.
        let store = mem();
        let other_bucket = mem();
        let k = tempdir();
        let _x = writer_at(mode, "x", &store, &k);
        let _y = writer_at(mode, "y", &store, &k);
        let _b = writer_at(mode, "x", &other_bucket, &k);
        for (what, into) in [("prefix", "y"), ("bucket", "x")] {
            let (dst, src_store, src_prefix) = if what == "prefix" {
                (&store, &store, "x")
            } else {
                (&other_bucket, &store, "x")
            };
            // For the bucket case the destination database is the one in `other_bucket`
            // and the source object comes from `store`'s x; for the prefix case it is y.
            let (dst, dst_prefix) = if what == "prefix" {
                (dst, into)
            } else {
                // Copy `store/x` objects into a third database in the other bucket.
                (&other_bucket, "x")
            };
            let reader = |m: Mode| read_at(Reader::ReadOnly, m, dst_prefix, dst, &k, &key_hex());
            assert_eq!(reader(mode).unwrap(), 45, "{mode:?} {what}: untouched");
            for (kind, targets) in copy_targets(dst, dst_prefix) {
                let originals: Vec<Vec<u8>> = targets.iter().map(|key| body(dst, key)).collect();
                for key in &targets {
                    let source_key = key.replacen(dst_prefix, src_prefix, 1);
                    put(dst, key, &body(src_store, &source_key));
                }
                assert_integrity(
                    reader(mode),
                    &targets,
                    &format!("{mode:?} {what}: {kind} copied in"),
                );
                for (key, original) in targets.iter().zip(&originals) {
                    put(dst, key, original);
                }
                assert_eq!(
                    reader(mode).unwrap(),
                    45,
                    "{mode:?} {what}: {kind} put back"
                );
            }
        }
    }
}

#[test]
fn a_whole_database_copied_with_its_head_still_opens_and_restores() {
    for mode in MODES {
        let store = mem();
        let other_bucket = mem();
        let k = tempdir();
        let _x = writer_at(mode, "x", &store, &k);
        copy_database(&store, "x", &store, "z");
        copy_database(&store, "x", &other_bucket, "x");
        let hex = key_hex();
        for (dst, prefix) in [(&store, "z"), (&other_bucket, "x")] {
            for r in [Reader::ReadOnly, Reader::Restore, Reader::Verify] {
                let rows = read_at(r, mode, prefix, dst, &k, &hex)
                    .unwrap_or_else(|e| panic!("{mode:?} {prefix} {r:?}: {e}"));
                if matches!(r, Reader::ReadOnly | Reader::Verify) {
                    assert_eq!(rows, 45, "{mode:?} {prefix} {r:?}");
                }
            }
            assert_eq!(
                head_at(dst, prefix).database_id,
                head_at(&store, "x").database_id,
                "the id travels with the head"
            );
        }
    }
}

/// An encrypted database written and left unsealed: rows in the log, no manifest.
fn unsealed_writer(prefix: &str, store: &Arc<MemStore>, k: &TempDir) -> TempDir {
    let dir = tempdir();
    let mut c = cfg_for(Mode::Wal, dir.path(), k);
    c.prefix = prefix.to_string();
    let db = open_with_store(c, store.clone()).unwrap();
    let conn = db.connect().unwrap();
    create(&conn);
    for id in 1..=3 {
        put_row(Mode::Wal, &conn, id);
    }
    std::mem::forget(conn);
    std::mem::forget(db);
    dir
}

#[test]
fn a_whole_head_copied_over_another_database_fails_at_the_first_object_read() {
    // A seal test of formats 1 to 4 (`bucket::legacy`).
    let _legacy = crate::format::LegacySeal::on();
    let k = tempdir();
    let hex = key_hex();
    // Over a sealed database: the manifest GET fails.
    {
        let store = mem();
        let _x = writer_at(Mode::Wal, "x", &store, &k);
        let _y = writer_at(Mode::Wal, "y", &store, &k);
        let xh = body(&store, &Keys::new("x").head());
        put(&store, &Keys::new("y").head(), &xh);
        let g = head_at(&store, "y")
            .manifest
            .expect("the copied head names a manifest");
        let want = Keys::new("y").manifest(g);
        for r in [Reader::ReadOnly, Reader::Restore] {
            assert_integrity(
                read_at(r, Mode::Wal, "y", &store, &k, &hex),
                std::slice::from_ref(&want),
                &format!("sealed y, {r:?}"),
            );
        }
    }
    // Over a database that holds log records but no manifest: the first record fails.
    {
        let store = mem();
        let _x = unsealed_writer("x", &store, &k);
        let _y = unsealed_writer("y", &store, &k);
        let xh = body(&store, &Keys::new("x").head());
        put(&store, &Keys::new("y").head(), &xh);
        let h = head_at(&store, "y");
        assert_eq!(h.manifest, None);
        let yk = Keys::new("y");
        let first = keys_under(&store, "y")
            .into_iter()
            .find(|x| yk.parse_log(x).is_some_and(|(g, _)| g == h.generation))
            .expect("y holds a log record");
        assert_integrity(
            read_at(Reader::ReadOnly, Mode::Wal, "y", &store, &k, &hex),
            std::slice::from_ref(&first),
            "unsealed y",
        );
    }
    // Over a database with no objects: nothing is read under y's id, the open succeeds and
    // shows an empty database, which is x's now.
    {
        let store = mem();
        let _x = unsealed_writer("x", &store, &k);
        let dir = tempdir();
        let mut c = cfg_for(Mode::Wal, dir.path(), &k);
        c.prefix = "y".to_string();
        let db = open_with_store(c, store.clone()).unwrap();
        std::mem::forget(db);
        // y's own records are removed too: only its head is left.
        for key in keys_under(&store, "y") {
            if key != Keys::new("y").head() {
                store.delete(&key).unwrap();
            }
        }
        let xh = body(&store, &Keys::new("x").head());
        put(&store, &Keys::new("y").head(), &xh);
        let t = tempdir();
        let mut c = cfg_for(Mode::Wal, &fresh(&t), &k);
        c.prefix = "y".to_string();
        c.prefetch = false;
        let db = open_read_only_with_store(c, store.clone());
        match db {
            Ok(db) => {
                let conn = db.connect().unwrap();
                let tables = conn
                    .query("SELECT count(*) FROM sqlite_master", &[])
                    .unwrap();
                assert_eq!(tables[0][0].as_int(), Some(0), "an empty database");
            }
            Err(e) => panic!("the open of y with x's head: {e}"),
        }
        assert_eq!(
            head_at(&store, "y").database_id,
            head_at(&store, "x").database_id,
            "the empty database is x's"
        );
    }
}

#[test]
fn a_format_4_head_with_its_database_id_edited_or_removed_fails_its_code() {
    // A seal test of formats 1 to 4 (`bucket::legacy`).
    let _legacy = crate::format::LegacySeal::on();
    for mode in MODES {
        let store = mem();
        let k = tempdir();
        let _w = writer(mode, &store, &k);
        let key = keys().head();
        let good = body(&store, &key);
        let id = head(&*store)
            .database_id
            .expect("a format 4 head has an id");
        let edited = if id.starts_with('0') {
            id.replacen('0', "1", 1)
        } else {
            id.replacen(&id[..1], "0", 1)
        };
        for what in ["edited", "removed"] {
            let text = String::from_utf8(good.clone()).unwrap();
            let bad = if what == "edited" {
                text.replace(&id, &edited)
            } else {
                text.replace(&format!(",\"database_id\":\"{id}\""), "")
            };
            assert_ne!(bad, text, "{mode:?} {what}: the edit changed nothing");
            put(&store, &key, bad.as_bytes());
            let (r, records) = integrity_records(|| read_with(Reader::Restore, mode, &store, &k));
            assert_integrity(r, std::slice::from_ref(&key), &format!("{mode:?} {what}"));
            assert_eq!(records, 1, "{mode:?} {what}");
        }
        put(&store, &key, &good);
        assert_eq!(read_with(Reader::ReadOnly, mode, &store, &k).unwrap(), 45);
    }
}

#[test]
fn after_a_good_open_a_format_3_head_over_a_format_4_database_is_a_downgrade() {
    for mode in MODES {
        let store = mem();
        let k = tempdir();
        let w = writer(mode, &store, &k);
        let enc_keys = cfg_for(mode, w.path(), &k).keys().unwrap();
        let key = keys().head();
        let good = body(&store, &key);
        let h = head(&*store);
        let pass = || crate::gc::gc_once(&*store, &enc_keys, Duration::ZERO, None);
        pass().unwrap_or_else(|e| panic!("{mode:?}: the untouched head: {e}"));
        // A valid format 3 head (coded under the integrity key), put over the format 4 one.
        let v3 = Head {
            format: crate::format::FORMAT_V3,
            database_id: None,
            ..h.clone()
        };
        put(&store, &key, &enc_keys.encode_head(&v3));
        let (got, records) = integrity_records(pass);
        match got {
            Err(Error::Corrupt(m)) => {
                assert_eq!(m, format!("integrity: {key} format downgraded"), "{mode:?}")
            }
            r => panic!("{mode:?}: expected Corrupt, got {:?}", r.map(|_| ())),
        }
        assert_eq!(records, 1, "{mode:?}");
        put(&store, &key, &good);
        pass().unwrap_or_else(|e| panic!("{mode:?}: the head put back: {e}"));
    }
}

#[test]
fn a_format_3_database_opens_writes_seals_and_restores_as_before() {
    for mode in MODES {
        let store = mem();
        let k = tempdir();
        let _dir = {
            let _legacy = crate::format::LegacyFormat3::on();
            writer(mode, &store, &k)
        };
        let h = head(&*store);
        assert_eq!((h.format, h.database_id.clone()), (3, None), "{mode:?}");
        for r in [Reader::ReadOnly, Reader::Restore, Reader::Verify] {
            let rows =
                read_with(r, mode, &store, &k).unwrap_or_else(|e| panic!("{mode:?} {r:?}: {e}"));
            if !matches!(r, Reader::Restore) {
                assert_eq!(rows, 45, "{mode:?} {r:?}");
            }
        }
        // A writer reopens it writes and seals.
        let dir2 = tempdir();
        let db = crate::takeover_with_store(cfg_for(mode, &fresh(&dir2), &k), store.clone());
        let db = db.map(|_| ());
        assert!(
            db.is_ok(),
            "{mode:?}: takeover of a format 3 database: {db:?}"
        );
        let w = open_with_store(cfg_for(mode, &fresh(&dir2), &k), store.clone()).unwrap();
        let c = w.connect().unwrap();
        for id in 46..=50 {
            put_row(mode, &c, id);
        }
        match mode {
            Mode::Wal => c.checkpoint(CheckpointMode::Truncate).unwrap(),
            Mode::Mvcc => truncate_checkpoint(&c).unwrap(),
        }
        c.close().unwrap();
        w.close().unwrap();
        let h = head(&*store);
        assert_eq!(
            (h.format, h.database_id),
            (3, None),
            "{mode:?}: still format 3"
        );
        assert_eq!(
            read_with(Reader::ReadOnly, mode, &store, &k).unwrap(),
            50,
            "{mode:?}"
        );
    }
}

/// Task 6: what a failed read leaves in telemetry: one ERROR record, nothing but the object's
/// key, kind, generation and seq in it, and `buckwal.outcome = integrity` on the reading span.
mod telemetry_signals {
    use super::*;
    use crate::telemetry::testing::Capture;
    use tracing::info_span;

    /// `f` inside a `caller` span that declares `buckwal.outcome`, under a capture.
    fn in_caller<T>(f: impl FnOnce() -> T) -> (T, crate::telemetry::testing::Spans) {
        let cap = Capture::new();
        let out = tracing::subscriber::with_default(cap.subscriber(), || {
            let caller = info_span!("caller", buckwal.outcome = tracing::field::Empty);
            caller.in_scope(f)
        });
        (out, cap.spans())
    }

    fn records(
        spans: &crate::telemetry::testing::Spans,
    ) -> Vec<&crate::telemetry::testing::EventRec> {
        spans
            .events
            .iter()
            .filter(|e| e.field("message") == Some("integrity check failed"))
            .collect()
    }

    #[test]
    fn a_tampered_page_yields_one_record_and_an_integrity_outcome() {
        // A seal test of formats 1 to 4 (`bucket::legacy`).
        let _legacy = crate::format::LegacySeal::on();
        for mode in MODES {
            let store = mem();
            let k = tempdir();
            let _w = writer(mode, &store, &k);
            let (_, m) = current_manifest(&store);
            let mut top: Vec<u32> = m.pages.keys().copied().collect();
            top.sort_unstable();
            let (p, q) = (top[top.len() - 1], top[top.len() - 2]);
            let (a, b) = (keys().page(m.pages[&p], p), keys().page(m.pages[&q], q));
            let (ba, bb) = (body(&store, &a), body(&store, &b));
            put(&store, &a, &bb);
            put(&store, &b, &ba);
            let (got, spans) = in_caller(|| read_with(Reader::ReadOnly, mode, &store, &k));
            assert_integrity(got, &[a.clone(), b.clone()], &format!("{mode:?}"));
            let recs = records(&spans);
            assert_eq!(recs.len(), 1, "{mode:?}: {recs:?}");
            let r = recs[0];
            assert_eq!(r.level, tracing::Level::ERROR);
            assert_eq!(r.target, "buckwal");
            assert_eq!(r.field("kind"), Some("page"));
            assert!([&a, &b].iter().any(|k| r.field("key") == Some(k.as_str())));
            let mut names: Vec<&str> = r.fields.keys().map(String::as_str).collect();
            names.sort_unstable();
            assert_eq!(
                names,
                ["generation", "key", "kind", "message", "seq"],
                "{r:?}"
            );
            for v in r.fields.values() {
                assert!(v.len() < 200, "{v}");
            }
            let c = spans.last_named("caller");
            assert_eq!(
                c.field("buckwal.outcome"),
                Some("integrity"),
                "{mode:?}: {c:?}"
            );
        }
    }

    #[test]
    fn restore_and_gc_spans_end_with_outcome_integrity() {
        // A seal test of formats 1 to 4 (`bucket::legacy`).
        let _legacy = crate::format::LegacySeal::on();
        for mode in MODES {
            let store = mem();
            let k = tempdir();
            let w = writer(mode, &store, &k);
            // Not the head: a manifest edited in place (the readers below all read it).
            let g = head(&*store).manifest.expect("sealed");
            let mk = keys().manifest(g);
            let mut forged = body(&store, &mk);
            let n = forged.len();
            forged[n - 1] ^= 1;
            put(&store, &mk, &forged);
            // GC reads a manifest once it is old enough.
            let min_age = cfg_for(mode, w.path(), &k)
                .engine(crate::io::IoMode::Writer)
                .gc_min_age();
            store.advance_clock(min_age + Duration::from_secs(1));
            // A takeover restores first, so its restore span is the one that fails.
            for (reader, span) in [
                (Reader::Restore, "buckwal.restore"),
                (Reader::Takeover, "buckwal.restore"),
                (Reader::Gc, "buckwal.gc"),
            ] {
                let (got, spans) = in_caller(|| read_with(reader, mode, &store, &k));
                assert!(
                    matches!(&got, Err(Error::Corrupt(m)) if m.starts_with("integrity: ")),
                    "{mode:?} {reader:?}: {got:?}"
                );
                assert_eq!(records(&spans).len(), 1, "{mode:?} {reader:?}");
                let s = spans.last_named(span);
                assert_eq!(
                    s.field("buckwal.outcome"),
                    Some("integrity"),
                    "{mode:?} {reader:?}: {s:?}"
                );
            }
        }
    }

    #[test]
    fn a_wrong_key_yields_no_record_and_no_integrity_outcome() {
        for mode in MODES {
            let store = mem();
            let k = tempdir();
            let _w = writer(mode, &store, &k);
            let other = tempdir();
            for r in ALL_READERS {
                let (got, spans) =
                    in_caller(|| read_with_key(r, mode, &store, &other, &"22".repeat(32)));
                assert_encryption(got, WRONG_KEY, &format!("{mode:?} {r:?}"));
                assert!(records(&spans).is_empty(), "{mode:?} {r:?}");
                assert!(
                    spans
                        .spans
                        .iter()
                        .all(|s| s.field("buckwal.outcome") != Some("integrity")),
                    "{mode:?} {r:?}"
                );
            }
        }
    }
}

/// A prefetch that meets a tampered page ends its `buckwal.prefetch` span with outcome
/// `integrity`, which is a failure; the page is not made local.
#[test]
fn a_prefetch_over_a_tampered_page_ends_integrity() {
    // A seal test of formats 1 to 4 (`bucket::legacy`).
    let _legacy = crate::format::LegacySeal::on();
    use crate::telemetry::testing::Capture;
    let store = mem();
    let k = tempdir();
    let _w = writer(Mode::Wal, &store, &k);
    let (_, m) = current_manifest(&store);
    let mut top: Vec<u32> = m.pages.keys().copied().collect();
    top.sort_unstable();
    let (p, q) = (top[top.len() - 1], top[top.len() - 2]);
    let (a, b) = (keys().page(m.pages[&p], p), keys().page(m.pages[&q], q));
    let (ba, bb) = (body(&store, &a), body(&store, &b));
    put(&store, &a, &bb);
    put(&store, &b, &ba);
    let t = tempdir();
    let mut c = cfg_for(Mode::Wal, &fresh(&t), &k);
    c.prefetch = true;
    let cap = Capture::new();
    let spans = tracing::subscriber::with_default(cap.subscriber(), || {
        let _db = open_read_only_with_store(c, store.clone()).unwrap();
        let until = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let spans = cap.spans();
            if spans
                .named("buckwal.prefetch")
                .iter()
                .any(|s| s.closed.is_some())
            {
                break spans;
            }
            assert!(std::time::Instant::now() < until, "no prefetch span closed");
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    });
    let s = spans.last_named("buckwal.prefetch");
    assert_eq!(s.field("buckwal.outcome"), Some("integrity"), "{s:?}");
}
