//! `buckwal vacuum` (spec Choice A (a)): the in-place `VACUUM` on the writer's own database,
//! its log record replayed by a restore, the pack after it, the guard that refuses the copy
//! form of the statement on an encrypted database, and the free-page share of a pack run.
//!
//! This file never holds the two words of the copy form next to each other: the test
//! `source_never_contains_vacuum_into` searches every file under `crates/` for them. The SQL
//! of that form is made from its parts here.
use super::*;
use crate::files::DB_FILE;
use crate::telemetry::testing::Capture;
use crate::vacuum::VACUUM_INTO_REFUSED;
use crate::{VacuumReport, Value};

/// The kinds of database a vacuum must work on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Plain,
    Encrypted,
    Mvcc,
}

const KINDS: [Kind; 3] = [Kind::Plain, Kind::Encrypted, Kind::Mvcc];

/// The writer's config of `kind` for `dir`; an encrypted one has its key in `keydir`.
fn kind_config(kind: Kind, dir: &Path, keydir: &TempDir) -> Config {
    let mut c = match kind {
        Kind::Plain | Kind::Mvcc => config(dir),
        Kind::Encrypted => enc_config(dir, keydir, &mixed_key_hex()),
    };
    if kind == Kind::Mvcc {
        c.journal_mode = JournalMode::Mvcc;
    }
    // No packer thread: each pack run here is the test's own.
    c.pack = false;
    c
}

/// A text of about 3.5 KB with the plaintext marker: each row fills most of a 4 KiB page.
fn big(id: i64) -> String {
    format!("{MARKER}-{id:06}{}", "x".repeat(3500))
}

fn insert_big(c: &Connection, ids: std::ops::RangeInclusive<i64>) {
    for id in ids {
        c.execute(
            "INSERT INTO t(id, v) VALUES (?, ?)",
            &[Value::from_i64(id), Value::build_text(big(id))],
        )
        .unwrap();
    }
}

/// Every row of `t`: the id, the text cut to its first 30 bytes, and its length.
fn rows(c: &Connection) -> Vec<(i64, String, i64)> {
    c.query(
        "SELECT id, substr(v, 1, 30), length(v) FROM t ORDER BY id",
        &[],
    )
    .unwrap()
    .iter()
    .map(|r| (r[0].as_int().unwrap(), text(&r[1]), r[2].as_int().unwrap()))
    .collect()
}

/// A writer of `cfg`: rows 1..=400, packed when `pack` is true, then rows 201..=400 deleted
/// and a TRUNCATE checkpoint, closed. About half the pages of `app.db` are free after it.
fn half_deleted(store: &Arc<MemStore>, cfg: &Config, pack: bool) {
    let db = open_with_store(cfg.clone(), store.clone()).unwrap();
    let c = db.connect().unwrap();
    create(&c);
    insert_big(&c, 1..=400);
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    if pack {
        pack_now(store.clone(), cfg);
    }
    c.execute("DELETE FROM t WHERE id > 200", &[]).unwrap();
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    c.close().unwrap();
    db.close().unwrap();
}

/// One pack run against `store`, from a working directory of its own; it must pack.
fn pack_now(store: Arc<MemStore>, cfg: &Config) -> crate::PackReport {
    let t = tempdir();
    let mut cfg = cfg.clone();
    cfg.dir = t.path().join("packer");
    let r = crate::pack::pack_with_store(&cfg, store).unwrap();
    assert_eq!(r.outcome, crate::PackOutcome::Packed, "{r:?}");
    r
}

fn file_len(dir: &Path) -> u64 {
    std::fs::metadata(dir.join(DB_FILE)).unwrap().len()
}

fn vacuum(store: &Arc<MemStore>, cfg: &Config) -> VacuumReport {
    crate::vacuum_with_store(cfg, store.clone()).unwrap_or_else(|e| panic!("vacuum: {e}"))
}

/// The rows of a restore of `store` (with the key of `cfg`), after an integrity check of the
/// restored copy.
fn restored_rows(store: &Arc<MemStore>, cfg: &Config) -> Vec<(i64, String, i64)> {
    let t = tempdir();
    let dir = fresh(&t);
    let mut rc = cfg.clone();
    rc.dir = dir.clone();
    restore_with_store(&rc, store.clone(), &dir).unwrap_or_else(|e| panic!("restore: {e}"));
    let db = open_read_only_with_store(rc, store.clone()).unwrap();
    let c = db.connect().unwrap();
    integrity_ok(&c);
    let r = rows(&c);
    c.close().unwrap();
    db.close().unwrap();
    r
}

fn expected(ids: impl IntoIterator<Item = i64>) -> Vec<(i64, String, i64)> {
    ids.into_iter()
        .map(|id| {
            let v = big(id);
            (id, v[..30].to_string(), v.len() as i64)
        })
        .collect()
}

#[test]
fn vacuum_shrinks_file_and_integrity_check_passes() {
    for kind in KINDS {
        let store = mem();
        let (a, k) = (tempdir(), tempdir());
        let cfg = kind_config(kind, a.path(), &k);
        half_deleted(&store, &cfg, false);
        let before = file_len(a.path());
        let r = vacuum(&store, &cfg);
        assert_eq!(r.file_bytes_before, before, "{kind:?} {r:?}");
        assert!(r.free_pages_before * 3 > r.pages_before, "{kind:?} {r:?}");
        assert!(r.free_page_share_before() > 0.3, "{kind:?} {r:?}");
        assert_eq!(r.free_pages_after, 0, "{kind:?} {r:?}");
        assert!(r.pages_after * 10 < r.pages_before * 7, "{kind:?} {r:?}");
        assert!(r.file_bytes_after * 10 < before * 7, "{kind:?} {r:?}");
        assert_eq!(file_len(a.path()), r.file_bytes_after, "{kind:?}");
        if kind == Kind::Encrypted {
            let head = std::fs::read(a.path().join(DB_FILE)).unwrap();
            assert!(!head.starts_with(b"SQLite format 3"), "plaintext header");
        }
        // The writer opens its vacuumed file again, whole.
        let db = open_with_store(cfg.clone(), store.clone()).unwrap();
        let c = db.connect().unwrap();
        integrity_ok(&c);
        assert_eq!(rows(&c), expected(1..=200), "{kind:?}");
        c.close().unwrap();
        db.close().unwrap();
    }
}

/// The vacuum refuses a directory that the writer does not own: an open there would restore
/// the database and take it over.
#[test]
fn vacuum_refuses_an_unclaimed_directory() {
    let store = mem();
    let (a, k) = (tempdir(), tempdir());
    half_deleted(&store, &kind_config(Kind::Plain, a.path(), &k), false);
    let b = tempdir();
    let etag = head_etag(&store);
    let r = crate::vacuum_with_store(&kind_config(Kind::Plain, &fresh(&b), &k), store.clone());
    match r {
        Err(Error::Config(m)) => assert!(m.contains("owns"), "{m}"),
        r => panic!("expected Config, got {r:?}"),
    }
    assert_eq!(head_etag(&store), etag, "the refused vacuum wrote the head");
    assert!(!fresh(&b).join(DB_FILE).exists());
}

/// The vacuum takes the directory's lock as any writer open does: it fails while another open
/// (for example `serve`) has the directory, and the file stays as it was.
#[test]
fn vacuum_refuses_a_directory_another_open_holds() {
    let store = mem();
    let (a, k) = (tempdir(), tempdir());
    let cfg = kind_config(Kind::Plain, a.path(), &k);
    half_deleted(&store, &cfg, false);
    let before = file_len(a.path());
    let db = open_with_store(cfg.clone(), store.clone()).unwrap();
    match crate::vacuum_with_store(&cfg, store.clone()) {
        Err(Error::AlreadyOpen(_)) => {}
        r => panic!("expected AlreadyOpen, got {r:?}"),
    }
    assert_eq!(file_len(a.path()), before);
    db.close().unwrap();
    assert!(vacuum(&store, &cfg).file_bytes_after < before);
}

#[test]
fn vacuum_on_encrypted_database_keeps_pack_encrypted() {
    for kind in [Kind::Encrypted, Kind::Plain] {
        let store = mem();
        let (a, k) = (tempdir(), tempdir());
        let cfg = kind_config(kind, a.path(), &k);
        half_deleted(&store, &cfg, true);
        let r = vacuum(&store, &cfg);
        let p = pack_now(store.clone(), &cfg);
        assert!(p.pack_size <= r.file_bytes_after, "{p:?} {r:?}");
        assert!(p.free_page_share < 0.01, "{p:?}");
        let found = needles_found(&store);
        if kind == Kind::Encrypted {
            assert_eq!(found, BTreeSet::new(), "plaintext in the encrypted bucket");
        } else {
            // The control: the same search finds the rows of a plain pack.
            assert!(found.contains("marker"), "{found:?}");
        }
        // The pack after the vacuum restores, opens with the key and passes its check.
        assert_eq!(restored_rows(&store, &cfg), expected(1..=200), "{kind:?}");
    }
}

#[test]
fn vacuum_log_record_and_later_commits_replay_correctly() {
    for kind in KINDS {
        let store = mem();
        let (a, k) = (tempdir(), tempdir());
        let cfg = kind_config(kind, a.path(), &k);
        // A pack from before the vacuum, so the restore applies the vacuum's log record over a
        // file of the old page numbers.
        half_deleted(&store, &cfg, true);
        vacuum(&store, &cfg);
        let db = open_with_store(cfg.clone(), store.clone()).unwrap();
        let c = db.connect().unwrap();
        insert_big(&c, 401..=450);
        c.execute("DELETE FROM t WHERE id IN (7, 410)", &[])
            .unwrap();
        c.execute(
            "UPDATE t SET v = ? WHERE id = 9",
            &[Value::build_text(big(9000))],
        )
        .unwrap();
        let want = rows(&c);
        assert_eq!(want.len(), 200 + 50 - 2, "{kind:?}");
        assert_eq!(
            restored_rows(&store, &cfg),
            want,
            "{kind:?}: log over the old pack"
        );
        // A pack of the same chain gives the same rows.
        pack_now(store.clone(), &cfg);
        assert_eq!(restored_rows(&store, &cfg), want, "{kind:?}: the new pack");
        c.close().unwrap();
        db.close().unwrap();
    }
}

/// Every file under `crates/` (no build output), as (path, bytes).
fn crate_files() -> Vec<(PathBuf, Vec<u8>)> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let mut out = Vec::new();
    let mut dirs = vec![root];
    while let Some(d) = dirs.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let e = e.unwrap();
            let name = e.file_name();
            if name == "target" || name.to_string_lossy().starts_with('.') {
                continue;
            }
            let path = e.path();
            if e.file_type().unwrap().is_dir() {
                dirs.push(path);
            } else {
                let body = std::fs::read(&path).unwrap();
                out.push((path, body));
            }
        }
    }
    out
}

/// A byte of an unquoted SQL name (lower case).
fn name_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'$'
}

/// The end of the white space and the comments (`/* ... */`, `-- ...`) at `at`.
fn skip_gap(s: &[u8], mut at: usize) -> usize {
    loop {
        if at < s.len() && s[at].is_ascii_whitespace() {
            at += 1;
        } else if s[at..].starts_with(b"/*") {
            at = match s[at + 2..].windows(2).position(|w| w == b"*/") {
                Some(i) => at + 2 + i + 2,
                None => s.len(),
            };
        } else if s[at..].starts_with(b"--") {
            at = match s[at..].iter().position(|&b| b == b'\n') {
                Some(i) => at + i + 1,
                None => s.len(),
            };
        } else {
            return at;
        }
    }
}

/// The end of one SQL name at `at` (a schema name): unquoted, or in `"..."`, `` `...` `` or
/// `[...]`. `None` when there is no name.
fn skip_name(s: &[u8], at: usize) -> Option<usize> {
    let close = match s.get(at)? {
        b'"' => b'"',
        b'`' => b'`',
        b'[' => b']',
        &b if name_byte(b) => {
            let n = s[at..].iter().take_while(|&&b| name_byte(b)).count();
            return Some(at + n);
        }
        _ => return None,
    };
    let n = s[at + 1..].iter().position(|&b| b == close)?;
    Some(at + 1 + n + 1)
}

/// `body` holds the copy form of the vacuum statement, in any case: the first word, then
/// white space and comments (`/* ... */`, `-- ...`) in any mix, at most one schema name
/// (unquoted or quoted), and the second word. A comment alone between the two words is
/// enough. The first and the second word must each be a whole word.
fn holds_copy_form(body: &[u8]) -> bool {
    // The second word in two parts: a quoted name may stand between the two words, and the
    // text from one literal to the next looks like one.
    let first: &[u8] = b"vacuum";
    let second: &[u8] = concat!("in", "to").as_bytes();
    let s = body.to_ascii_lowercase();
    let whole = |at: usize, word: &[u8]| {
        s[at..].starts_with(word) && s.get(at + word.len()).is_none_or(|&b| !name_byte(b))
    };
    for i in 0..s.len() {
        if !whole(i, first) || (i > 0 && name_byte(s[i - 1])) {
            continue;
        }
        let mut at = i + first.len();
        let mut named = false;
        loop {
            at = skip_gap(&s, at);
            if whole(at, second) {
                return true;
            }
            match skip_name(&s, at) {
                Some(end) if !named => {
                    named = true;
                    at = end;
                }
                _ => break,
            }
        }
    }
    false
}

/// The matcher finds every spelling of the copy form that the engine accepts, and nothing
/// else. The spellings are made from their parts at run time, so this file holds none.
#[test]
fn copy_form_matcher_finds_every_spelling() {
    for (f, s) in [
        ("VACUUM", concat!("IN", "TO")),
        ("vacuum", concat!("in", "to")),
        ("VaCuUm", concat!("iN", "tO")),
    ] {
        for sql in [
            format!("{f} {s} 'x'"),
            format!("{f}\n\t\r\n {s} 'x'"),
            format!("{f}/*x*/{s} 'x'"),
            format!("{f} /* a\n b */ {s} 'x'"),
            format!("{f} -- c\n{s} 'x'"),
            format!("{f} main {s} 'x'"),
            format!("{f}\tMain\n{s} 'x'"),
            format!("{f} \"main\" {s} 'x'"),
            format!("{f}`aux`{s} 'x'"),
            format!("{f} [aux]{s} 'x'"),
            format!("{f} /* a */ main /* b */ -- c\n {s} 'x'"),
            format!("conn.execute(\"{f} main {s} '/tmp/c.db'\")"),
            format!("EXPLAIN {f} {s} 'x'"),
        ] {
            assert!(holds_copy_form(sql.as_bytes()), "{sql:?}");
        }
        for sql in [
            format!("{f}; SELECT 1 {s}"),
            format!("{f}ed {s}"),
            format!("{f} main aux {s}"),
            format!("{f} 'x' {s}"),
            format!("{f}.rs {s}"),
            format!("{f} {s}x"),
            format!("x{f} {s}"),
            f.to_string(),
        ] {
            assert!(!holds_copy_form(sql.as_bytes()), "{sql:?}");
        }
    }
}

/// No source under `crates/` runs or names the copy form: on an encrypted database it writes
/// a plaintext file (docs/packer-recon.md). The search finds each spelling that
/// [`holds_copy_form`] names: any case, white space and comments between the two words, and
/// a schema name. The guard (`guard.rs`) stays the protection at run time: it refuses each
/// spelling on an encrypted database.
#[test]
fn source_never_contains_vacuum_into() {
    let files = crate_files();
    assert!(
        files
            .iter()
            .any(|(p, _)| p.ends_with("buckwal/src/vacuum.rs")),
        "the walk found the crate sources"
    );
    let hits: Vec<_> = files
        .iter()
        .filter(|(_, body)| holds_copy_form(body))
        .map(|(p, _)| p.display().to_string())
        .collect();
    assert_eq!(hits, Vec::<String>::new());
}

/// The copy form of the vacuum statement to `path`, built from its parts: `first` and
/// `second` are the two words in the case to test, `ws` the white space between them.
fn copy_form(first: &str, ws: &str, second: &str, path: &Path) -> String {
    format!("{first}{ws}{second} '{}'", path.display())
}

/// Spellings of the copy form: cases, white space, a comment and a schema name.
fn copy_forms(path: &Path) -> Vec<String> {
    let mut v = Vec::new();
    for (first, second) in [
        ("VACUUM", concat!("IN", "TO")),
        ("vacuum", concat!("in", "to")),
        ("VaCuUm", concat!("In", "To")),
    ] {
        for ws in [" ", "\n\t  ", " /* c */ ", " main "] {
            v.push(copy_form(first, ws, second, path));
        }
    }
    v.push(format!(
        "  {}",
        copy_form("VACUUM", "\r\n", concat!("IN", "TO"), path)
    ));
    v.push(format!(
        "EXPLAIN {}",
        copy_form("VACUUM", " ", concat!("IN", "TO"), path)
    ));
    v
}

fn refused(r: Result<impl std::fmt::Debug>, sql: &str) {
    match r {
        Err(Error::Unsupported(what)) => assert_eq!(what, VACUUM_INTO_REFUSED, "{sql}"),
        r => panic!("{sql}: expected the refusal, got {r:?}"),
    }
}

/// Every SQL path of a connection refuses the copy form on an encrypted database, on a writer
/// that `buckwal vacuum` opens (the engine's vacuum on) and on a normal one. No file is made.
#[test]
fn vacuum_into_on_encrypted_database_is_refused() {
    let message = Error::Unsupported(VACUUM_INTO_REFUSED).to_string();
    assert!(message.contains("plaintext"), "{message}");
    assert!(message.contains("buckwal vacuum"), "{message}");
    let store = mem();
    let (a, k) = (tempdir(), tempdir());
    let cfg = kind_config(Kind::Encrypted, a.path(), &k);
    half_deleted(&store, &cfg, false);
    let out = tempdir();
    let target = out.path().join("copy.db");
    for vacuum_on in [true, false] {
        let db = if vacuum_on {
            crate::db::open_for_vacuum(cfg.clone(), store.clone()).unwrap()
        } else {
            open_with_store(cfg.clone(), store.clone()).unwrap()
        };
        let c = db.connect().unwrap();
        for sql in copy_forms(&target) {
            refused(c.execute(&sql, &[]), &sql);
            refused(c.query(&sql, &[]), &sql);
            refused(c.run(&sql, &[], &[]), &sql);
            refused(c.describe(&sql), &sql);
            refused(c.execute_batch(&sql), &sql);
            let batch = format!("SELECT 1; {sql}");
            refused(c.execute_batch(&batch), &batch);
            // The split path of a batch (a batching PRAGMA in it) checks the whole string too.
            let split = format!("PRAGMA buckwal_batching = off; {sql}");
            refused(c.execute_batch(&split), &split);
            assert!(!target.exists(), "{sql}: the copy was written");
        }
        // Other SQL still runs, and the in-place statement on the open with the vacuum on.
        assert_eq!(ids(&c).len(), 200);
        if vacuum_on {
            c.execute("VACUUM", &[]).unwrap();
            integrity_ok(&c);
            assert_eq!(ids(&c).len(), 200);
        }
        c.close().unwrap();
        db.close().unwrap();
    }
    assert!(std::fs::read_dir(out.path()).unwrap().next().is_none());
}

/// The guard leaves the copy form of a plain database to the engine.
#[test]
fn vacuum_into_on_plain_database_is_not_refused_by_the_guard() {
    let store = mem();
    let (a, k) = (tempdir(), tempdir());
    let cfg = kind_config(Kind::Plain, a.path(), &k);
    half_deleted(&store, &cfg, false);
    let out = tempdir();
    let target = out.path().join("copy.db");
    let db = crate::db::open_for_vacuum(cfg, store.clone()).unwrap();
    let c = db.connect().unwrap();
    let sql = copy_form("vacuum", "  ", concat!("in", "to"), &target);
    let r = c.execute(&sql, &[]);
    assert_ne!(r, Err(Error::Unsupported(VACUUM_INTO_REFUSED)), "{sql}");
    c.close().unwrap();
    db.close().unwrap();
}

/// The pack run's span carries the free-page share as `buckwal.free_page_share`: the warn
/// alert's data (spec Choice A (a)). It falls to about zero after a vacuum.
#[test]
fn pack_span_has_free_page_share() {
    let store = mem();
    let (a, k) = (tempdir(), tempdir());
    let cfg = kind_config(Kind::Plain, a.path(), &k);
    half_deleted(&store, &cfg, false);
    let share = |cap: &Capture| -> f64 {
        let spans = cap.spans();
        let s = spans.last_named("buckwal.pack");
        s.field("buckwal.free_page_share")
            .unwrap_or_else(|| panic!("no buckwal.free_page_share"))
            .parse()
            .unwrap()
    };
    let cap = Capture::new();
    let r = tracing::subscriber::with_default(cap.subscriber(), || pack_now(store.clone(), &cfg));
    let before = share(&cap);
    assert_eq!(before, r.free_page_share);
    assert!(before > 0.3, "{before}");
    vacuum(&store, &cfg);
    let cap = Capture::new();
    let r = tracing::subscriber::with_default(cap.subscriber(), || pack_now(store.clone(), &cfg));
    let after = share(&cap);
    assert_eq!(after, r.free_page_share);
    assert!(after < 0.01, "{after}");
}

/// `buckwal vacuum` sends one `buckwal.vacuum` span with the pages before and after, and the
/// free-page share before.
#[test]
fn vacuum_span_has_pages_and_free_page_share() {
    let store = mem();
    let (a, k) = (tempdir(), tempdir());
    let cfg = kind_config(Kind::Plain, a.path(), &k);
    half_deleted(&store, &cfg, false);
    let cap = Capture::new();
    let r = tracing::subscriber::with_default(cap.subscriber(), || vacuum(&store, &cfg));
    let spans = cap.spans();
    let s = spans.last_named("buckwal.vacuum");
    assert_eq!(s.field("buckwal.outcome"), Some("vacuumed"));
    let f = |name: &str| {
        s.field(name)
            .unwrap_or_else(|| panic!("no {name}"))
            .to_string()
    };
    assert_eq!(f("buckwal.vacuum.pages_before"), r.pages_before.to_string());
    assert_eq!(f("buckwal.vacuum.pages_after"), r.pages_after.to_string());
    assert_eq!(f("buckwal.vacuum.free_pages_after"), "0");
    assert_eq!(
        f("buckwal.vacuum.bytes_after"),
        r.file_bytes_after.to_string()
    );
    let share: f64 = f("buckwal.free_page_share").parse().unwrap();
    assert_eq!(share, r.free_page_share_before());
    assert!(f("buckwal.elapsed_ms").parse::<u64>().is_ok());
}
