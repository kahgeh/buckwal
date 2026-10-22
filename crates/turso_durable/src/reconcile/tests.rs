use super::*;
use crate::config::JournalMode;
use crate::engine::tests::{
    cfg, cfg_deadline, commit_row, crash, create_table, head, ids, integrity, keys, log_seqs,
    restored, run, seal_now, writer, writer_with,
};
use crate::engine::{open_engine, Engine};
use crate::files::DIRTY_FILE;
use crate::format::LogFile;
use crate::format::{encode_record, Head, LogRecord};
use crate::io::db_storage::seal_page_set;
use crate::io::stash;
use crate::io::wal_file::wal_frame_pages;
use crate::restore::{generation_image, restore_into};
use crate::store::{FaultStore, MemStore, ObjectStore};
use std::collections::BTreeSet;
use std::fs;
use std::sync::Arc;
use std::time::Duration;
use turso_core::{CheckpointMode, Connection, Value};

fn attach(store: Arc<dyn ObjectStore>, dir: &Path, cfg: &EngineConfig) -> (Bucket, LocalState) {
    let state = LocalState::read(dir).unwrap().expect("local state");
    let b = Bucket::attach(
        store,
        keys(),
        cfg.retry.clone(),
        cfg.upload_concurrency,
        &state.writer_id,
        state.epoch,
    )
    .unwrap();
    (b, state)
}

/// Attach to the bucket as the directory's writer and reconcile the directory.
fn reconciled(
    store: Arc<dyn ObjectStore>,
    dir: &Path,
    cfg: &EngineConfig,
) -> (Reconciled, Bucket, LocalState) {
    let (mut b, mut s) = attach(store, dir, cfg);
    let r = reconcile(dir, &mut b, &mut s, cfg).unwrap_or_else(|e| panic!("reconcile: {e}"));
    (r, b, s)
}

fn open(dir: &Path, bucket: Bucket, state: &LocalState) -> (Engine, Arc<Connection>) {
    let e = open_engine(dir, bucket, state, &cfg(), None).unwrap();
    let c = e.db.connect().unwrap();
    (e, c)
}

/// Reconcile, then open.
fn reopen(store: Arc<dyn ObjectStore>, dir: &Path) -> (Reconciled, Engine, Arc<Connection>) {
    let (r, b, s) = reconciled(store, dir, &cfg());
    let (e, c) = open(dir, b, &s);
    (r, e, c)
}

/// The bucket's image `B` of the head's generation.
fn bucket_image(store: &dyn ObjectStore) -> Vec<u8> {
    generation_image(store, &keys(), &head(store), &cfg())
        .unwrap()
        .0
}

fn local_wal(dir: &Path) -> Vec<u8> {
    fs::read(dir.join("app.db-wal")).unwrap()
}

fn rows(conn: &Arc<Connection>) -> Vec<Vec<Value>> {
    run(conn, "SELECT id, v FROM t ORDER BY id").unwrap()
}

fn checkpoint(conn: &Arc<Connection>, mode: CheckpointMode) {
    stash::clear();
    conn.checkpoint(mode)
        .unwrap_or_else(|e| panic!("checkpoint: {e} ({:?})", stash::take()));
}

fn passive(conn: &Arc<Connection>) {
    checkpoint(
        conn,
        CheckpointMode::Passive {
            upper_bound_inclusive: None,
        },
    );
}

fn commit_rows(conn: &Arc<Connection>, ids: std::ops::RangeInclusive<i64>) {
    for id in ids {
        commit_row(conn, id).unwrap_or_else(|e| panic!("commit {id}: {e}"));
    }
}

/// Seal a crashed directory's generation by hand, as `BucketDbStorage::seal` does up to its
/// head write: every page in the dirty file or in a frame of the local `-wal`, read from the
/// local `app.db`. The local state and files are left as they are. Returns the new generation.
fn seal_by_hand(store: Arc<dyn ObjectStore>, dir: &Path) -> u64 {
    let wal = local_wal(dir);
    let ps = u32::from_be_bytes(wal[8..12].try_into().unwrap());
    let db = fs::read(dir.join("app.db")).unwrap();
    let page_count = (db.len() / ps as usize) as u32;
    let persisted = fs::read(dir.join(DIRTY_FILE)).unwrap_or_default();
    assert!(
        !persisted.is_empty(),
        "the checkpoint persisted no dirty page"
    );
    let set = seal_page_set(
        &BTreeSet::new(),
        &persisted,
        &wal_frame_pages(&wal),
        page_count,
    );
    let pages = set
        .iter()
        .map(|&p| {
            let off = (p as usize - 1) * ps as usize;
            (p, db[off..off + ps as usize].to_vec())
        })
        .collect();
    let (mut b, _) = attach(store, dir, &cfg());
    b.seal_generation(pages, ps, page_count).unwrap()
}

#[test]
fn level_restart_continues() {
    let mem = Arc::new(MemStore::new());
    let d = tempfile::tempdir().unwrap();
    let (e, conn) = writer(mem.clone(), d.path());
    create_table(&conn);
    commit_rows(&conn, 1..=20);
    let crashed = crash(e, conn, d.path());
    let before = log_seqs(&*mem, 1).len() as u64;

    let (r, e2, c2) = reopen(mem.clone(), crashed.path());
    assert_eq!(r, Reconciled::Level);
    assert_eq!(ids(&c2), (1..=20).collect::<Vec<_>>());
    commit_row(&c2, 21).unwrap();
    assert_eq!(
        log_seqs(&*mem, 1),
        (1..=before + 1).collect::<Vec<_>>(),
        "the restarted writer continues at the next seq"
    );
    let _crashed2 = crash(e2, c2, crashed.path());

    let (_r, _e3, c3) = restored(mem.clone());
    assert_eq!(integrity(&c3), "ok");
    assert_eq!(ids(&c3), (1..=21).collect::<Vec<_>>());
}

#[test]
fn ahead_restart_uploads_unacked_tail() {
    let mem = Arc::new(MemStore::new());
    let fault = Arc::new(FaultStore::new(mem.clone()));
    let d = tempfile::tempdir().unwrap();
    let (e, conn) = writer_with(
        fault.clone(),
        d.path(),
        &cfg_deadline(Duration::from_millis(200)),
    );
    create_table(&conn);
    commit_rows(&conn, 1..=5);
    fault.fail_next_puts(u64::MAX / 2);
    assert!(commit_row(&conn, 6).is_err(), "commit 6 must fail");
    fault.fail_next_puts(0);
    let crashed = crash(e, conn, d.path());
    let before = log_seqs(&*mem, 1).len() as u64;
    assert!(local_wal(crashed.path()).len() > bucket_image(&*mem).len());

    let (r, e2, c2) = reopen(mem.clone(), crashed.path());
    assert_eq!(
        r,
        Reconciled::Ahead {
            uploaded: LogPos {
                generation: 1,
                seq: before + 1
            }
        }
    );
    assert_eq!(
        bucket_image(&*mem),
        local_wal(crashed.path()),
        "the bucket now holds the local -wal"
    );
    // The failed commit's frames are valid on disk: the engine recovers them.
    assert_eq!(ids(&c2), (1..=6).collect::<Vec<_>>());
    let local = rows(&c2);
    let _crashed2 = crash(e2, c2, crashed.path());

    let (_r, _e3, c3) = restored(mem.clone());
    assert_eq!(integrity(&c3), "ok");
    assert_eq!(rows(&c3), local, "the restore equals the local database");
}

#[test]
fn behind_restart_takes_bucket_image() {
    let mem = Arc::new(MemStore::new());
    let d = tempfile::tempdir().unwrap();
    let (e, conn) = writer(mem.clone(), d.path());
    create_table(&conn);
    commit_rows(&conn, 1..=200);
    seal_now(&conn);
    commit_rows(&conn, 201..=230);
    let crashed = crash(e, conn, d.path());
    let wal = crashed.path().join("app.db-wal");
    let len = fs::metadata(&wal).unwrap().len();
    fs::OpenOptions::new()
        .write(true)
        .open(&wal)
        .unwrap()
        .set_len(len / 2)
        .unwrap();

    let (r, b, s) = reconciled(mem.clone(), crashed.path(), &cfg());
    assert_eq!(r, Reconciled::Behind);
    assert_eq!(local_wal(crashed.path()), bucket_image(&*mem));
    assert!(!s.complete);
    assert!(!LocalState::read(crashed.path()).unwrap().unwrap().complete);
    assert_eq!(s.generation, 2);

    let (_e2, c2) = open(crashed.path(), b, &s);
    assert_eq!(integrity(&c2), "ok");
    assert_eq!(ids(&c2), (1..=230).collect::<Vec<_>>());
}

#[test]
fn restart_after_head_moved_discards_covered_wal() {
    let mem = Arc::new(MemStore::new());
    let d = tempfile::tempdir().unwrap();
    let (e, conn) = writer(mem.clone(), d.path());
    create_table(&conn);
    commit_rows(&conn, 1..=300);
    passive(&conn);
    let crashed = crash(e, conn, d.path());
    assert_eq!(seal_by_hand(mem.clone(), crashed.path()), 2);
    assert_eq!(
        LocalState::read(crashed.path())
            .unwrap()
            .unwrap()
            .generation,
        1
    );

    let (r, b, s) = reconciled(mem.clone(), crashed.path(), &cfg());
    assert_eq!(r, Reconciled::NewGeneration);
    assert_eq!(s.generation, 2);
    assert_eq!(
        LocalState::read(crashed.path())
            .unwrap()
            .unwrap()
            .generation,
        2
    );
    assert_eq!(local_wal(crashed.path()), bucket_image(&*mem));
    assert!(local_wal(crashed.path()).is_empty());
    assert!(log_seqs(&*mem, 2).is_empty(), "uploaded into log/2");
    let dirty = crashed.path().join(DIRTY_FILE);
    assert_eq!(fs::metadata(dirty).map(|m| m.len()).unwrap_or(0), 0);

    let (e2, c2) = open(crashed.path(), b, &s);
    assert_eq!(integrity(&c2), "ok");
    assert_eq!(ids(&c2), (1..=300).collect::<Vec<_>>());
    commit_row(&c2, 301).unwrap();
    let _crashed2 = crash(e2, c2, crashed.path());

    let (_r, _e3, c3) = restored(mem.clone());
    assert_eq!(integrity(&c3), "ok");
    assert_eq!(ids(&c3), (1..=301).collect::<Vec<_>>());
}

/// The seal records G+1 locally before the restart write reaches the `-wal`: a crash in
/// between leaves state == head == G+1 next to a `-wal` that still holds G.
#[test]
fn sealed_state_before_restart_write_discards_old_wal() {
    let mem = Arc::new(MemStore::new());
    let d = tempfile::tempdir().unwrap();
    let (e, conn) = writer(mem.clone(), d.path());
    create_table(&conn);
    commit_rows(&conn, 1..=300);
    passive(&conn);
    let crashed = crash(e, conn, d.path());
    let g = seal_by_hand(mem.clone(), crashed.path());
    let mut state = LocalState::read(crashed.path()).unwrap().unwrap();
    state.generation = g;
    state.write_atomic(crashed.path()).unwrap();
    assert!(!local_wal(crashed.path()).is_empty());

    let (r, b, s) = reconciled(mem.clone(), crashed.path(), &cfg());
    assert_eq!(r, Reconciled::NewGeneration);
    assert_eq!(s.generation, g);
    assert!(local_wal(crashed.path()).is_empty());
    assert!(
        log_seqs(&*mem, g).is_empty(),
        "uploaded G's frames into G+1"
    );
    let dirty = crashed.path().join(DIRTY_FILE);
    assert_eq!(fs::metadata(dirty).map(|m| m.len()).unwrap_or(0), 0);

    let (e2, c2) = open(crashed.path(), b, &s);
    assert_eq!(ids(&c2), (1..=300).collect::<Vec<_>>());
    commit_row(&c2, 301).unwrap();
    let _crashed2 = crash(e2, c2, crashed.path());

    let (_r, _e3, c3) = restored(mem.clone());
    assert_eq!(integrity(&c3), "ok");
    assert_eq!(ids(&c3), (1..=301).collect::<Vec<_>>());
}

#[test]
fn older_generation_wal_is_detected_by_its_header() {
    let header = |epoch: u8| {
        let mut h = vec![0u8; 32];
        h[12..24].fill(epoch);
        h
    };
    let mut g1 = header(1);
    g1.extend([7u8; 100]);
    let mut g2 = header(2);
    g2.extend([9u8; 50]);
    assert!(holds_older_generation(&g1, &g2), "other salts");
    assert!(holds_older_generation(&g1, &[]), "nothing yet in G+1");
    assert!(!holds_older_generation(&g1, &g1[..60]), "same salts");
    assert!(!holds_older_generation(&[], &g2), "no local -wal");
    assert!(!holds_older_generation(&g1[..10], &g2), "no local header");
}

#[test]
fn foreign_writer_id_is_fenced() {
    let mem = Arc::new(MemStore::new());
    let d = tempfile::tempdir().unwrap();
    let (e, conn) = writer(mem.clone(), d.path());
    create_table(&conn);
    commit_rows(&conn, 1..=3);
    let crashed = crash(e, conn, d.path());
    let wal = local_wal(crashed.path());
    let h = head(&*mem);
    let other = "f".repeat(32);
    let foreign = Head {
        writer_id: other.clone(),
        ..h
    };
    mem.put(&keys().head(), &foreign.encode()).unwrap();

    let c = cfg();
    let mut b = Bucket::attach(mem.clone(), keys(), c.retry.clone(), 1, &other, 0).unwrap();
    let mut s = LocalState::read(crashed.path()).unwrap().unwrap();
    let r = reconcile(crashed.path(), &mut b, &mut s, &c);
    assert!(matches!(r, Err(Error::Fenced(_))), "{r:?}");
    assert_eq!(
        local_wal(crashed.path()),
        wal,
        "a fenced reconcile changes nothing"
    );
}

#[test]
fn foreign_record_in_log_is_fenced() {
    let mem = Arc::new(MemStore::new());
    let d = tempfile::tempdir().unwrap();
    let (e, conn) = writer(mem.clone(), d.path());
    create_table(&conn);
    commit_rows(&conn, 1..=3);
    let crashed = crash(e, conn, d.path());
    let next = log_seqs(&*mem, 1).len() as u64 + 1;
    let rec = encode_record(&LogRecord::Extents {
        file: LogFile::Wal,
        writer_id: "f".repeat(32),
        syncs: vec![vec![]],
    });
    mem.put(&keys().log(1, next), &rec).unwrap();

    let (mut b, mut s) = attach(mem.clone(), crashed.path(), &cfg());
    let r = reconcile(crashed.path(), &mut b, &mut s, &cfg());
    assert!(matches!(r, Err(Error::Fenced(_))), "{r:?}");
}

#[test]
fn local_generation_ahead_is_corrupt() {
    let mem = Arc::new(MemStore::new());
    let d = tempfile::tempdir().unwrap();
    let (e, conn) = writer(mem.clone(), d.path());
    create_table(&conn);
    commit_rows(&conn, 1..=3);
    let crashed = crash(e, conn, d.path());
    let (mut b, mut s) = attach(mem.clone(), crashed.path(), &cfg());
    s.generation = head(&*mem).generation + 1;
    let r = reconcile(crashed.path(), &mut b, &mut s, &cfg());
    assert!(matches!(r, Err(Error::Corrupt(_))), "{r:?}");
}

/// D6/D7-carried invariant: pages a PASSIVE checkpoint backfilled before a crash reach the
/// next seal after a restart (the engine re-backfills every valid frame: nbackfills starts
/// at 0, sqlite3_ondisk.rs:1567, 2018). `dir` starts with a writer already open on it.
fn passive_checkpoint_crash_then_seal_keeps_pages(
    mem: Arc<MemStore>,
    dir: &Path,
    e: Engine,
    conn: Arc<Connection>,
    first: i64,
) {
    commit_rows(&conn, first..=first + 299);
    passive(&conn);
    let crashed = crash(e, conn, dir);

    let (r, e2, c2) = reopen(mem.clone(), crashed.path());
    assert_eq!(r, Reconciled::Level);
    let gen = e2.uploader.generation();
    commit_rows(&c2, first + 300..=first + 309);
    seal_now(&c2);
    assert_eq!(e2.uploader.generation(), gen + 1, "no boundary sealed");
    let _crashed2 = crash(e2, c2, crashed.path());

    let r = tempfile::tempdir().unwrap();
    restore_into(mem.clone(), &keys(), r.path(), &cfg()).unwrap();
    let (b, st) = attach(mem.clone(), r.path(), &cfg());
    let (_e3, c3) = open(r.path(), b, &st);
    assert_eq!(integrity(&c3), "ok");
    assert_eq!(ids(&c3), (1..=first + 309).collect::<Vec<_>>());
}

#[test]
fn seal_after_passive_checkpoint_crash_keeps_pages() {
    let mem = Arc::new(MemStore::new());
    let d = tempfile::tempdir().unwrap();
    let (e, conn) = writer(mem.clone(), d.path());
    create_table(&conn);
    passive_checkpoint_crash_then_seal_keeps_pages(mem, d.path(), e, conn, 1);
}

#[test]
fn seal_after_passive_checkpoint_crash_keeps_pages_on_restored_dir() {
    let mem = Arc::new(MemStore::new());
    let d = tempfile::tempdir().unwrap();
    let (e, conn) = writer(mem.clone(), d.path());
    create_table(&conn);
    commit_rows(&conn, 1..=50);
    seal_now(&conn);
    let _crashed = crash(e, conn, d.path());

    let (r, e2, c2) = restored(mem.clone());
    assert!(!e2.storage.is_complete(), "a restore starts incomplete");
    passive_checkpoint_crash_then_seal_keeps_pages(mem, r.path(), e2, c2, 51);
}

/// Pages the dirty file names hold a newer local version than the bucket's: after a restart
/// they are never fetched over it.
#[test]
fn dirty_pages_are_local_at_open() {
    let mem = Arc::new(MemStore::new());
    let d = tempfile::tempdir().unwrap();
    let (e, conn) = writer(mem.clone(), d.path());
    create_table(&conn);
    commit_rows(&conn, 1..=200);
    seal_now(&conn);
    let _crashed = crash(e, conn, d.path());

    let (r, e2, c2) = restored(mem.clone());
    commit_rows(&c2, 201..=260);
    passive(&c2);
    let crashed = crash(e2, c2, r.path());
    let persisted = fs::read(crashed.path().join(DIRTY_FILE)).unwrap();
    let sealed = crate::engine::tests::manifest(&*mem).1;
    let dirty: Vec<u32> = persisted
        .chunks_exact(4)
        .map(|c| u32::from_le_bytes(c.try_into().unwrap()))
        .filter(|p| sealed.pages.contains_key(p))
        .collect();
    assert!(!dirty.is_empty(), "no sealed page was dirtied");

    let (rec, e3, _c3) = reopen(mem.clone(), crashed.path());
    assert_eq!(rec, Reconciled::Level);
    assert!(!e3.storage.is_complete());
    for &p in &dirty {
        e3.storage.ensure_local(p).unwrap();
    }
    assert_eq!(
        e3.storage.lazy_gets(),
        0,
        "fetched a dirty page over its local copy"
    );
}

/// Behind the head by more than one sealed generation, or by one the head's manifest does
/// not name: the local files cannot be brought level, and the error says the directory is
/// stale and to restore again.
#[test]
fn generation_jump_is_corrupt() {
    let mem = Arc::new(MemStore::new());
    let d = tempfile::tempdir().unwrap();
    let (e, conn) = writer(mem.clone(), d.path());
    create_table(&conn);
    commit_rows(&conn, 1..=3);
    let crashed = crash(e, conn, d.path());

    // Head at 1 with no manifest, local state at 0.
    let (mut b, mut s) = attach(mem.clone(), crashed.path(), &cfg());
    s.generation = 0;
    let r = reconcile(crashed.path(), &mut b, &mut s, &cfg());
    assert!(
        matches!(&r, Err(Error::Corrupt(m))
            if m.contains("generation") && m.contains("stale") && m.contains("restore again")),
        "{r:?}"
    );

    // Head at 3 after two seals, local state at 1.
    let (e2, c2) = writer(mem.clone(), crashed.path());
    commit_rows(&c2, 4..=6);
    seal_now(&c2);
    commit_rows(&c2, 7..=9);
    seal_now(&c2);
    let crashed2 = crash(e2, c2, crashed.path());
    assert_eq!(head(&*mem).generation, 3);
    let (mut b, mut s) = attach(mem.clone(), crashed2.path(), &cfg());
    s.generation = 1;
    let wal = local_wal(crashed2.path());
    let r = reconcile(crashed2.path(), &mut b, &mut s, &cfg());
    assert!(
        matches!(&r, Err(Error::Corrupt(m))
            if m.contains("generation") && m.contains("stale") && m.contains("restore again")),
        "{r:?}"
    );
    assert_eq!(
        local_wal(crashed2.path()),
        wal,
        "a refused reconcile changes nothing"
    );
}

/// Never sealed, restored, then the restored disk lost its database file and half its
/// `-wal`: the bucket's image comes back and the empty database file is seeded from it.
#[test]
fn behind_on_restored_dir_seeds_empty_database_file() {
    let mem = Arc::new(MemStore::new());
    let d = tempfile::tempdir().unwrap();
    let (e, conn) = writer(mem.clone(), d.path());
    create_table(&conn);
    commit_rows(&conn, 1..=40);
    let _crashed = crash(e, conn, d.path());
    assert_eq!(head(&*mem).manifest, None);

    let r = tempfile::tempdir().unwrap();
    restore_into(mem.clone(), &keys(), r.path(), &cfg()).unwrap();
    let db = r.path().join(DB_FILE);
    assert!(fs::metadata(&db).unwrap().len() > 0, "restore seeds app.db");
    fs::OpenOptions::new()
        .write(true)
        .open(&db)
        .unwrap()
        .set_len(0)
        .unwrap();
    let wal = r.path().join(WAL_FILE);
    let len = fs::metadata(&wal).unwrap().len();
    fs::OpenOptions::new()
        .write(true)
        .open(&wal)
        .unwrap()
        .set_len(len / 2)
        .unwrap();

    let (rec, b, s) = reconciled(mem.clone(), r.path(), &cfg());
    assert_eq!(rec, Reconciled::Behind);
    assert!(
        fs::metadata(&db).unwrap().len() > 0,
        "app.db was not seeded"
    );
    assert_eq!(local_wal(r.path()), bucket_image(&*mem));
    let (_e2, c2) = open(r.path(), b, &s);
    assert_eq!(integrity(&c2), "ok");
    assert_eq!(ids(&c2), (1..=40).collect::<Vec<_>>());
}

/// A read-only open never uploads: unacknowledged local commits are refused, not shipped.
#[test]
fn read_only_reconcile_refuses_to_upload_ahead() {
    let mem = Arc::new(MemStore::new());
    let fault = Arc::new(FaultStore::new(mem.clone()));
    let d = tempfile::tempdir().unwrap();
    let (e, conn) = writer_with(
        fault.clone(),
        d.path(),
        &cfg_deadline(Duration::from_millis(200)),
    );
    create_table(&conn);
    commit_rows(&conn, 1..=5);
    fault.fail_next_puts(u64::MAX / 2);
    assert!(commit_row(&conn, 6).is_err(), "commit 6 must fail");
    fault.fail_next_puts(0);
    let crashed = crash(e, conn, d.path());
    let puts = mem.stats().puts;

    let ro = EngineConfig {
        read_only: true,
        ..cfg()
    };
    let (mut b, mut s) = attach(mem.clone(), crashed.path(), &ro);
    let r = reconcile(crashed.path(), &mut b, &mut s, &ro);
    assert_eq!(r, Err(Error::Unsupported("writes on a read-only open")));
    assert_eq!(mem.stats().puts, puts, "a read-only reconcile uploaded");
}

/// After a claim, `log/G` holds the old writer's records, then the claimer's: accepted. An
/// old writer's record after one of the claimer's means the two interleaved: `Fenced`.
#[test]
fn claimed_generation_accepts_old_writer_prefix_only() {
    let mem = Arc::new(MemStore::new());
    let d = tempfile::tempdir().unwrap();
    let (e, conn) = writer(mem.clone(), d.path());
    create_table(&conn);
    commit_rows(&conn, 1..=3);
    let _crashed = crash(e, conn, d.path());
    let old = head(&*mem);
    let claimer = "c".repeat(32);
    let claimed = Head {
        writer_id: claimer.clone(),
        ..old.clone()
    };
    mem.put(&keys().head(), &claimed.encode()).unwrap();
    let image = generation_image(&*mem, &keys(), &claimed, &cfg()).unwrap();

    let next = log_seqs(&*mem, old.generation).len() as u64 + 1;
    let empty = |w: &str| {
        encode_record(&LogRecord::Extents {
            file: LogFile::Wal,
            writer_id: w.to_string(),
            syncs: vec![vec![]],
        })
    };
    mem.put(&keys().log(old.generation, next), &empty(&claimer))
        .unwrap();
    let with_claimer = generation_image(&*mem, &keys(), &claimed, &cfg()).unwrap();
    assert_eq!(with_claimer.0, image.0);
    assert_eq!(with_claimer.1, image.1 + 1);

    mem.put(
        &keys().log(old.generation, next + 1),
        &empty(&old.writer_id),
    )
    .unwrap();
    let r = generation_image(&*mem, &keys(), &claimed, &cfg());
    assert!(matches!(r, Err(Error::Fenced(_))), "{r:?}");

    // Two claims in one generation: old, then a middle writer, then the head's: accepted. The
    // old writer resuming after the middle one is not.
    let middle = "m".repeat(32);
    let gen = old.generation;
    mem.put(&keys().log(gen, next), &empty(&middle)).unwrap();
    mem.put(&keys().log(gen, next + 1), &empty(&claimer))
        .unwrap();
    let three = generation_image(&*mem, &keys(), &claimed, &cfg()).unwrap();
    assert_eq!(three.1, image.1 + 2);
    mem.put(&keys().log(gen, next + 1), &empty(&old.writer_id))
        .unwrap();
    mem.put(&keys().log(gen, next + 2), &empty(&claimer))
        .unwrap();
    let r = generation_image(&*mem, &keys(), &claimed, &cfg());
    assert!(
        matches!(&r, Err(Error::Fenced(m)) if m.contains("both sides")),
        "{r:?}"
    );
}

/// Hand-built log images for the fence reading rule.
mod fence_image {
    use super::*;
    use crate::format::Op;

    enum R {
        Ext(&'static str, u64),
        Fence(&'static str, u64),
    }

    /// Image and count of `log/1` holding `recs` (an extents record writes its number at offset
    /// 0 of its own byte), with the head naming `head_writer`.
    fn image(recs: &[R], head_writer: &str) -> crate::Result<(Vec<u8>, u64)> {
        let mem = MemStore::new();
        for (i, r) in recs.iter().enumerate() {
            let rec = match r {
                R::Ext(w, n) => LogRecord::Extents {
                    file: LogFile::Wal,
                    writer_id: w.to_string(),
                    syncs: vec![vec![Op::Write {
                        off: *n - 1,
                        bytes: vec![*n as u8],
                    }]],
                },
                R::Fence(w, e) => LogRecord::Fence {
                    writer_id: w.to_string(),
                    epoch: *e,
                },
            };
            mem.put(&keys().log(1, i as u64 + 1), &encode_record(&rec))
                .unwrap();
        }
        let head = Head {
            journal_mode: JournalMode::Wal,
            format: 1,
            generation: 1,
            manifest: None,
            writer_id: head_writer.to_string(),
            epoch: 0,
        };
        generation_image(&mem, &keys(), &head, &cfg())
    }

    #[test]
    fn image_accepts_old_records_then_fence_then_fencer_records() {
        let r = image(
            &[
                R::Ext("A", 1),
                R::Ext("A", 2),
                R::Fence("B", 1),
                R::Ext("B", 3),
            ],
            "B",
        )
        .unwrap();
        assert_eq!(r, (vec![1, 2, 3], 4));
    }

    #[test]
    fn image_accepts_a_fence_with_nothing_after() {
        let r = image(&[R::Ext("A", 1), R::Ext("A", 2), R::Fence("B", 1)], "B").unwrap();
        assert_eq!(r, (vec![1, 2], 3));
    }

    #[test]
    fn image_accepts_rising_fences() {
        let r = image(
            &[
                R::Ext("A", 1),
                R::Fence("B", 1),
                R::Ext("B", 2),
                R::Fence("C", 2),
                R::Ext("C", 3),
            ],
            "C",
        )
        .unwrap();
        assert_eq!(r, (vec![1, 2, 3], 5));
        let r = image(&[R::Ext("A", 1), R::Fence("B", 1), R::Fence("B", 1)], "B").unwrap();
        assert_eq!(r, (vec![1], 3));
    }

    #[test]
    fn image_refuses_another_writer_after_a_fence() {
        let r = image(&[R::Ext("A", 1), R::Fence("B", 1), R::Ext("A", 2)], "B");
        assert!(
            matches!(&r, Err(Error::Fenced(m)) if m.contains("seq 3")),
            "{r:?}"
        );
    }

    #[test]
    fn image_refuses_a_fence_epoch_that_does_not_rise() {
        for second in [R::Fence("C", 2), R::Fence("C", 1)] {
            let r = image(&[R::Fence("B", 2), second], "C");
            assert!(
                matches!(&r, Err(Error::Fenced(m)) if m.contains("seq 2")),
                "{r:?}"
            );
        }
    }
}
