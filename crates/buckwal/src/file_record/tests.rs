use super::*;
use crate::format::LocalState;

fn base_state() -> LocalState {
    LocalState {
        format: 5,
        writer_id: "w".into(),
        generation: 1,
        complete: true,
        claimed: true,
        head_etag: String::new(),
        epoch: 0,
        claim_id: String::new(),
        seal_not_before_ms: 0,
        seal_quiet_ms: 0,
        next_generation_floor: 0,
        files: None,
    }
}

fn dir_with_files() -> tempfile::TempDir {
    let d = tempfile::tempdir().unwrap();
    fs::write(d.path().join(DB_FILE), vec![1u8; 8192]).unwrap();
    fs::write(d.path().join(WAL_FILE), vec![2u8; 64]).unwrap();
    d
}

fn closed(dir: &Path, mvcc: bool) -> LocalState {
    let files = DbFiles::new(dir, mvcc, base_state());
    files.write_clean_close().unwrap();
    LocalState::read(dir).unwrap().unwrap()
}

fn detail_of(r: Result<()>) -> String {
    match r {
        Err(Error::Corrupt(m)) => m,
        r => panic!("not Corrupt: {r:?}"),
    }
}

#[test]
fn check_passes_when_nothing_changed() {
    let d = dir_with_files();
    let s = closed(d.path(), false);
    check_at_open(d.path(), &s).unwrap();
}

#[test]
fn no_record_checks_nothing() {
    let d = dir_with_files();
    check_at_open(d.path(), &base_state()).unwrap();
}

#[test]
fn each_changed_detail_is_refused_with_the_exact_message() {
    let want = "app.db was changed outside buckwal since buckwal last used it; restore into a new directory";
    // size
    let d = dir_with_files();
    let s = closed(d.path(), false);
    fs::write(d.path().join(DB_FILE), vec![1u8; 4096]).unwrap();
    assert_eq!(detail_of(check_at_open(d.path(), &s)), want);
    // time: same bytes, new modification time
    let d = dir_with_files();
    let s = closed(d.path(), false);
    let f = File::options()
        .write(true)
        .open(d.path().join(DB_FILE))
        .unwrap();
    f.set_modified(std::time::SystemTime::now() + std::time::Duration::from_secs(5))
        .unwrap();
    assert_eq!(detail_of(check_at_open(d.path(), &s)), want);
    // page1: same size, same time, other bytes
    let d = dir_with_files();
    let s = closed(d.path(), false);
    let mtime = fs::metadata(d.path().join(DB_FILE))
        .unwrap()
        .modified()
        .unwrap();
    fs::write(d.path().join(DB_FILE), vec![9u8; 8192]).unwrap();
    File::options()
        .write(true)
        .open(d.path().join(DB_FILE))
        .unwrap()
        .set_modified(mtime)
        .unwrap();
    assert_eq!(detail_of(check_at_open(d.path(), &s)), want);
    // wal header
    let d = dir_with_files();
    let s = closed(d.path(), false);
    fs::write(d.path().join(WAL_FILE), vec![3u8; 64]).unwrap();
    assert_eq!(detail_of(check_at_open(d.path(), &s)), want);
    // wal size
    let d = dir_with_files();
    let s = closed(d.path(), false);
    fs::remove_file(d.path().join(WAL_FILE)).unwrap();
    assert_eq!(detail_of(check_at_open(d.path(), &s)), want);
    // MVCC log header
    let d = dir_with_files();
    fs::write(d.path().join(LOG_FILE), vec![4u8; 40]).unwrap();
    let s = closed(d.path(), true);
    fs::write(d.path().join(LOG_FILE), vec![5u8; 40]).unwrap();
    assert_eq!(detail_of(check_at_open(d.path(), &s)), want);
}

#[test]
fn crash_without_write_open_checks_only_app_db() {
    let d = dir_with_files();
    let mut s = closed(d.path(), false);
    s.files.as_mut().unwrap().clean_close = false;
    // The -wal differs: not checked after a crash.
    fs::write(d.path().join(WAL_FILE), vec![7u8; 100]).unwrap();
    check_at_open(d.path(), &s).unwrap();
    // app.db differs: refused.
    fs::write(d.path().join(DB_FILE), vec![8u8; 8192]).unwrap();
    detail_of(check_at_open(d.path(), &s));
}

#[test]
fn crash_with_write_open_checks_nothing() {
    let d = dir_with_files();
    let mut s = closed(d.path(), false);
    let rec = s.files.as_mut().unwrap();
    rec.clean_close = false;
    rec.db_write_open = true;
    fs::write(d.path().join(DB_FILE), vec![8u8; 100]).unwrap();
    check_at_open(d.path(), &s).unwrap();
}

#[test]
fn begin_write_fsyncs_state_before_returning() {
    let d = dir_with_files();
    let s = closed(d.path(), false);
    let files = DbFiles::new(d.path(), false, s);
    files.begin_write().unwrap();
    let on_disk = LocalState::read(d.path()).unwrap().unwrap().files.unwrap();
    assert!(on_disk.db_write_open);
    assert!(!on_disk.clean_close);
}

#[test]
fn begin_write_without_a_record_starts_one_with_write_open_on() {
    let d = dir_with_files();
    let files = DbFiles::new(d.path(), false, base_state());
    files.begin_write().unwrap();
    // Nothing is on disk yet: an open would check nothing anyway.
    assert!(LocalState::read(d.path()).unwrap().is_none());
    // A state write in between persists the flag, and an open still checks nothing.
    files.write_state(&base_state()).unwrap();
    let on_disk = LocalState::read(d.path()).unwrap().unwrap();
    assert!(on_disk.files.as_ref().unwrap().db_write_open);
    check_at_open(d.path(), &on_disk).unwrap();
}

#[test]
fn end_write_records_new_size_and_page1() {
    let d = dir_with_files();
    let s = closed(d.path(), false);
    let files = DbFiles::new(d.path(), false, s);
    files.begin_write().unwrap();
    fs::write(d.path().join(DB_FILE), vec![6u8; 12288]).unwrap();
    let token = files.sync_token();
    files.end_write(token).unwrap();
    let state = LocalState::read(d.path()).unwrap().unwrap();
    let rec = state.files.as_ref().unwrap();
    assert!(!rec.db_write_open);
    assert!(!rec.clean_close);
    assert_eq!(rec.db_size, 12288);
    // A crash now: the app.db part matches, so the open is not refused.
    check_at_open(d.path(), &state).unwrap();
}

#[test]
fn end_write_keeps_the_flag_on_when_a_write_came_after_the_fsync_started() {
    let d = dir_with_files();
    let s = closed(d.path(), false);
    let files = DbFiles::new(d.path(), false, s);
    files.begin_write().unwrap();
    let token = files.sync_token();
    files.begin_write().unwrap();
    files.end_write(token).unwrap();
    let state = LocalState::read(d.path()).unwrap().unwrap();
    assert!(state.files.unwrap().db_write_open);
}

#[test]
fn write_state_keeps_the_file_record() {
    let d = dir_with_files();
    let s = closed(d.path(), false);
    let files = DbFiles::new(d.path(), false, s.clone());
    files.begin_write().unwrap();
    let mut other = s;
    other.generation = 9;
    other.files = None;
    files.write_state(&other).unwrap();
    let on_disk = LocalState::read(d.path()).unwrap().unwrap();
    assert_eq!(on_disk.generation, 9);
    assert!(on_disk.files.unwrap().db_write_open);
}

#[test]
fn mark_in_use_turns_clean_close_off_and_writes_it() {
    let d = dir_with_files();
    let mut s = closed(d.path(), false);
    mark_in_use(d.path(), &mut s).unwrap();
    assert!(!s.files.as_ref().unwrap().clean_close);
    let rec = LocalState::read(d.path()).unwrap().unwrap().files.unwrap();
    assert!(!rec.clean_close);
    assert!(!rec.db_write_open);
    // No record: nothing is written.
    let d = dir_with_files();
    let mut s = base_state();
    mark_in_use(d.path(), &mut s).unwrap();
    assert!(LocalState::read(d.path()).unwrap().is_none());
}
