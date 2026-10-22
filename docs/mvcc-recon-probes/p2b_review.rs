//! P2: a `.db-log` sync whose completion is held. The commit's step must not return Done,
//! and a second connection must not see the row, until the completion is released.
use mvcc_probe::rec_io::RecIo;
use mvcc_probe::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};
use turso_core::{Connection, IO};

/// Run `sql` on a fresh connection on its own thread; wait up to `timeout`. Returns the
/// result (or "blocked") and the receiver, so a blocked reader's eventual result can be
/// printed later. The reader counts its StepResult kinds.
fn read(db: &Db, sql: &str, timeout: Duration) -> (String, mpsc::Receiver<String>) {
    let conn = db.connect();
    let io = db.io.clone();
    let sql = sql.to_string();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let t0 = Instant::now();
        let mut stmt = match conn.prepare(&sql) {
            Ok(s) => s,
            Err(e) => {
                let _ = tx.send(format!("prepare Err({e})"));
                return;
            }
        };
        let (mut n_io, mut n_yield, mut val) = (0u64, 0u64, String::from("<no row>"));
        let r = loop {
            match stmt.step() {
                Ok(turso_core::StepResult::Done) => break "Done".to_string(),
                Ok(turso_core::StepResult::Row) => {
                    val = stmt.row().unwrap().get_values().next().unwrap().to_string();
                }
                Ok(turso_core::StepResult::IO) => {
                    n_io += 1;
                    io.step().unwrap();
                    std::thread::yield_now();
                }
                Ok(turso_core::StepResult::Yield) => {
                    n_yield += 1;
                    io.step().unwrap();
                    std::thread::yield_now();
                }
                Ok(other) => break format!("{other:?}"),
                Err(e) => break format!("Err({e})"),
            }
        };
        let _ = tx.send(format!(
            "{r} value={val} (IO steps {n_io}, Yield steps {n_yield}, {:?})",
            t0.elapsed()
        ));
    });
    match rx.recv_timeout(timeout) {
        Ok(s) => (s, rx),
        Err(_) => (format!("blocked>{}ms", timeout.as_millis()), rx),
    }
}

/// A non-aggregate reader (`SELECT id FROM t`) that reports every Row the moment `step`
/// returns it, and then Done, through a channel.
fn streaming_reader(db: &Db) -> mpsc::Receiver<String> {
    let conn = db.connect();
    let io = db.io.clone();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut stmt = conn.prepare("SELECT id FROM t ORDER BY id").unwrap();
        loop {
            match stmt.step() {
                Ok(turso_core::StepResult::Done) => {
                    let _ = tx.send("Done".into());
                    break;
                }
                Ok(turso_core::StepResult::Row) => {
                    let _ = tx.send(format!("Row id={}", stmt.row().unwrap().get_values().next().unwrap()));
                }
                Ok(turso_core::StepResult::IO) | Ok(turso_core::StepResult::Yield) => {
                    io.step().unwrap();
                    std::thread::yield_now();
                }
                Ok(other) => {
                    let _ = tx.send(format!("{other:?}"));
                    break;
                }
                Err(e) => {
                    let _ = tx.send(format!("Err({e})"));
                    break;
                }
            }
        }
    });
    rx
}


fn read_in_tx(db: &Db, begin: &str, sql: &str, timeout: Duration) -> String {
    let conn = db.connect();
    let io = db.io.clone();
    let (begin, sql) = (begin.to_string(), sql.to_string());
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let t0 = Instant::now();
        ok(&conn, &io, &begin);
        let mut stmt = conn.prepare(&sql).unwrap();
        let mut val = String::from("<no row>");
        let r = loop {
            match stmt.step() {
                Ok(turso_core::StepResult::Done) => break "Done".to_string(),
                Ok(turso_core::StepResult::Row) => { val = stmt.row().unwrap().get_values().next().unwrap().to_string(); }
                Ok(turso_core::StepResult::IO) | Ok(turso_core::StepResult::Yield) => { io.step().unwrap(); std::thread::yield_now(); }
                Ok(other) => break format!("{other:?}"),
                Err(e) => break format!("Err({e})"),
            }
        };
        let _ = tx.send(format!("{r} value={val} ({:?})", t0.elapsed()));
        std::thread::sleep(Duration::from_secs(4));
    });
    rx.recv_timeout(timeout).unwrap_or_else(|_| format!("blocked>{}ms", timeout.as_millis()))
}

fn visible(s: &str) -> String {
    if s.starts_with("blocked") {
        "false(reader blocked)".into()
    } else if s.starts_with("Done value=1") {
        "true".into()
    } else if s.starts_with("Done value=0") {
        "false".into()
    } else {
        format!("?({s})")
    }
}

fn main() {
    watchdog(60, "p2b");
    let dir = fresh_dir("p2b");
    let rec = RecIo::new(platform());
    let io: Arc<dyn IO> = Arc::new(rec.clone());
    let db = Arc::new(open(io.clone(), &dir, false).unwrap());
    let setup = db.connect();
    ok(&setup, &io, "PRAGMA journal_mode = 'mvcc'");
    ok(&setup, &io, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");
    ok(&setup, &io, "INSERT INTO t VALUES (1, 'before')");
    ok(&setup, &io, "CREATE TABLE u(x)");

    rec.fault.delay_log_sync.store(true, SeqCst);
    rec.mark("delay_log_sync on; thread A: INSERT id=2");
    let done = Arc::new(AtomicBool::new(false));
    let steps = Arc::new(AtomicUsize::new(0));
    let a_conn: Arc<Connection> = db.connect();
    let (d2, s2, io2) = (done.clone(), steps.clone(), io.clone());
    let a = std::thread::spawn(move || {
        let mut stmt = a_conn.prepare("INSERT INTO t VALUES (2, 'held')").unwrap();
        let r = loop {
            s2.fetch_add(1, SeqCst);
            match stmt.step() {
                Ok(turso_core::StepResult::Done) => break Ok(()),
                Ok(turso_core::StepResult::IO) | Ok(turso_core::StepResult::Yield) => {
                    io2.step().unwrap();
                    std::thread::yield_now();
                }
                Ok(other) => break Err(format!("{other:?}")),
                Err(e) => break Err(e.to_string()),
            }
        };
        d2.store(true, SeqCst);
        r
    });

    // Thread B (this thread).
    std::thread::sleep(Duration::from_millis(200));
    let held = rec.fault.held.lock().unwrap().len();
    let before_done = done.load(SeqCst);
    let srx = streaming_reader(&db);
    let (r2, rx2) = read(&db, "SELECT count(*) FROM t WHERE id = 2", Duration::from_secs(2));
    let (r1, rx1) = read(&db, "SELECT count(*) FROM t WHERE id = 1", Duration::from_secs(2));
    let (ru, rxu) = read(&db, "SELECT count(*) FROM u", Duration::from_secs(2));
    let rb = read_in_tx(&db, "BEGIN", "SELECT count(*) FROM t WHERE id = 2", Duration::from_secs(1));
    let rc = read_in_tx(&db, "BEGIN CONCURRENT", "SELECT count(*) FROM t WHERE id = 2", Duration::from_secs(1));
    println!("REVIEW reader inside BEGIN: SELECT id=2 before release -> {rb}");
    println!("REVIEW reader inside BEGIN CONCURRENT: SELECT id=2 before release -> {rc}");
    let steps_before = steps.load(SeqCst);
    println!("held completions: {held}, A's step() calls so far: {steps_before}");
    println!("reader id=2 (the held row): {r2}");
    println!("reader id=1 (committed earlier): {r1}");
    println!("reader table u (untouched): {ru}");
    let seen: Vec<String> = srx.try_iter().collect();
    println!("streaming reader `SELECT id FROM t ORDER BY id` (started before the 2 s waits): rows_seen_before_release={:?} select_done_before_release={}",
        seen.iter().filter(|e| e.starts_with("Row")).collect::<Vec<_>>(), seen.iter().any(|e| e == "Done"));
    println!("before-release: done={before_done} visible={}", visible(&r2));

    rec.fault.delay_log_sync.store(false, SeqCst);
    rec.mark("release");
    rec.fault.release();
    let t0 = Instant::now();
    while !done.load(SeqCst) && t0.elapsed() < Duration::from_secs(5) {
        std::thread::sleep(Duration::from_millis(1));
    }
    let after_done = done.load(SeqCst);
    let ra = if after_done { Some(a.join().unwrap()) } else { None };
    let late = |first: &str, rx: mpsc::Receiver<String>| {
        if !first.starts_with("blocked") {
            return "(had returned before release)".to_string();
        }
        rx.recv_timeout(Duration::from_secs(3)).unwrap_or_else(|_| "still blocked".into())
    };
    println!("blocked readers after release: id=2 -> {}, id=1 -> {}, u -> {}", late(&r2, rx2), late(&r1, rx1), late(&ru, rxu));
    std::thread::sleep(Duration::from_millis(50));
    println!("streaming reader after release: {:?}", srx.try_iter().collect::<Vec<_>>());
    let (ra2, _) = read(&db, "SELECT count(*) FROM t WHERE id = 2", Duration::from_secs(3));
    println!("new reader id=2 after release: {ra2}");
    println!("after-release: done={after_done} visible={}", visible(&ra2));
    println!("thread A result: {ra:?} (released after {:?})", t0.elapsed());
    println!("ops:");
    let ops = rec.ops();
    let from = ops.iter().position(|o| matches!(o, mvcc_probe::rec_io::Op::Mark(m) if m.starts_with("delay_log_sync"))).unwrap();
    print_ops(&ops[from..]);
}
