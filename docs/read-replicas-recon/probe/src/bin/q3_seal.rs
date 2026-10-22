//! Q3: across a seal's WAL restart. The follower's plan: apply the rest of generation G, run a local
//! TRUNCATE checkpoint, then apply generation G+1's frames from seq 1. Probed end to end, with a
//! real `kill -9` of the applying process between (and inside) the steps, then a restart that
//! resumes. Two resume policies are compared: `seq` (position = the replica's own WAL: `wal_state`
//! checkpoint_seq_no and max_frame, no side file) and `pos` (a tiny position file written before
//! and after each local checkpoint, plus the WAL's max_frame). Three generations are used so that a
//! wrong resume can step back over a checkpointed newer generation.
//!
//! Parent mode (no args) builds the inputs from a real writer, then for each kill stage runs a child
//! that applies and kills itself, then a second child that resumes and checks.
//! Child mode: `q3_seal child <dir> <stage|resume>`.
use probe::*;
use std::path::{Path, PathBuf};
use std::process::Command;
use buckwal::{Batching, CheckpointMode, Value};

const FS: usize = FRAME_HDR + 4096;

fn read_frames(p: &Path) -> Vec<Vec<u8>> {
    std::fs::read(p).unwrap().chunks(FS).map(|c| c.to_vec()).collect()
}

fn kill_self() -> ! {
    Command::new("kill").args(["-9", &std::process::id().to_string()]).status().unwrap();
    std::thread::sleep(std::time::Duration::from_secs(5));
    unreachable!()
}

fn is_commit(f: &[u8]) -> bool {
    u32::from_be_bytes(f[4..8].try_into().unwrap()) != 0
}

/// Apply frames[from..] of one generation, one session per transaction. `stop_mid_tx` kills the
/// process after `n` frames of the first transaction that has more than `n` frames, uncommitted.
fn apply_gen(c: &std::sync::Arc<turso_core::Connection>, frames: &[Vec<u8>], from: usize, kill_after_frames: Option<usize>) {
    let mut i = from;
    while i < frames.len() {
        let mut j = i;
        while !is_commit(&frames[j]) {
            j += 1;
        }
        c.wal_insert_begin().unwrap();
        for k in i..=j {
            c.wal_insert_frame(k as u64 + 1, &frames[k]).unwrap();
            if let Some(n) = kill_after_frames {
                if k + 1 == n {
                    kill_self();
                }
            }
        }
        c.wal_insert_end(true).unwrap();
        i = j + 1;
    }
}

fn write_pos(dir: &Path, gi: usize, stage: &str) {
    let tmp = dir.join("pos.tmp");
    std::fs::write(&tmp, format!("{gi} {stage}")).unwrap();
    std::fs::File::open(&tmp).unwrap().sync_all().unwrap();
    std::fs::rename(&tmp, dir.join("pos")).unwrap();
}

fn read_pos(dir: &Path) -> (usize, String) {
    match std::fs::read_to_string(dir.join("pos")) {
        Ok(s) => {
            let (a, b) = s.split_once(' ').unwrap();
            (a.parse().unwrap(), b.to_string())
        }
        Err(_) => (0, "applying".into()),
    }
}

/// The whole follower run for generations [from_gi..], optionally dying at `die`.
/// `die` is checked at: "mid:<gi>" (inside the last transaction of generation gi, uncommitted),
/// "end:<gi>" (all of gi applied and committed), "ckpt:<gi>" (after the checkpoint that leaves gi,
/// before anything is applied to gi+1).
fn run(c: &std::sync::Arc<turso_core::Connection>, dir: &Path, gens: &[Vec<Vec<u8>>], mut gi: usize, mut have: usize, mut ckpt_pending: bool, die: &str) {
    loop {
        if ckpt_pending {
            // a checkpoint leaves generation gi-1
            if c.wal_state().unwrap().max_frame > 0 {
                let busy = ckpt_retry(c);
                if busy > 0 {
                    println!("  (checkpoint answered Busy {busy} times before it ran)");
                }
            }
            write_pos(dir, gi, "applying");
            ckpt_pending = false;
            if die == format!("ckpt:{}", gi - 1) {
                kill_self();
            }
        }
        let mid = die.strip_prefix("mid:").and_then(|g| g.parse::<usize>().ok()) == Some(gi);
        let n = gens[gi].len();
        apply_gen(c, &gens[gi], have, if mid { Some(n.saturating_sub(3).max(have + 1)) } else { None });
        if die == format!("end:{gi}") {
            kill_self();
        }
        if gi + 1 == gens.len() {
            break;
        }
        write_pos(dir, gi + 1, "ckpt_pending");
        if let Some(ms) = die.strip_prefix("inckpt:").and_then(|x| x.parse::<u64>().ok()) {
            let c2 = c.clone();
            std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_micros(ms));
                kill_self()
            });
            let _ = ckpt_truncate(&c2);
            // (no retry here: this thread is killed mid-checkpoint on purpose)
            std::thread::sleep(std::time::Duration::from_secs(5));
        }
        gi += 1;
        have = 0;
        ckpt_pending = true;
    }
}

fn child(dir: &Path, stage: &str) {
    let gens: Vec<Vec<Vec<u8>>> = (0..3).map(|i| read_frames(&dir.join(format!("g{i}.frames")))).collect();
    let (db, c) = open_core(&dir.join("replica").join("app.db"));
    let st = c.wal_state().unwrap();
    println!("[{stage}] start: wal_state={st:?} rows={}", count(&c));
    match stage {
        "report" => {
            let (gi, how) = read_pos(dir);
            println!("[{stage}] after the kill: pos file = generation index {gi} {how}, local WAL {st:?} (max_frame > 0 means the kill landed before the WAL was truncated, 0 means after)");
        }
        "resume_seq" | "resume_pos" => {
            let start_rows = count(&c);
            let (gi, have, pending) = if stage == "resume_seq" {
                // position = the WAL alone
                (st.checkpoint_seq_no as usize, st.max_frame as usize, false)
            } else {
                let (gi, how) = read_pos(dir);
                println!("[{stage}] pos file: generation index {gi}, {how}");
                if how == "ckpt_pending" {
                    (gi, 0, true)
                } else {
                    (gi, st.max_frame as usize, false)
                }
            };
            println!("[{stage}] resuming at generation index {gi}, frame {have}, ckpt_pending={pending}");
            // watch the visible row count across the resume for a step back
            let c2 = db.connect().unwrap();
            let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let (stop2, lo) = (stop.clone(), std::sync::Arc::new(std::sync::atomic::AtomicI64::new(start_rows)));
            let lo2 = lo.clone();
            let watcher = std::thread::spawn(move || {
                let mut hi = start_rows;
                let mut worst_drop = 0;
                while !stop2.load(std::sync::atomic::Ordering::Relaxed) {
                    let n = count(&c2);
                    if n > hi { hi = n }
                    if hi - n > worst_drop { worst_drop = hi - n }
                    std::thread::sleep(std::time::Duration::from_micros(200));
                }
                lo2.store(worst_drop, std::sync::atomic::Ordering::Relaxed);
            });
            run(&c, dir, &gens, gi, have, pending, "none");
            stop.store(true, std::sync::atomic::Ordering::Relaxed);
            watcher.join().unwrap();
            println!("[{stage}] done: wal_state={:?} rows={} integrity={} worst_visible_row_drop={}", c.wal_state().unwrap(), count(&c), integrity(&c), lo.load(std::sync::atomic::Ordering::Relaxed));
        }
        _ => {
            // the first run, killing itself at the stage, writing the pos file as it goes
            run(&c, dir, &gens, 0, 0, false, stage);
            println!("[{stage}] finished without dying");
        }
    }
    drop(db);
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("child") {
        child(Path::new(&args[2]), &args[3]);
        return;
    }
    // ---- parent: a real writer produces three generations (each ends in a multi-frame transaction)
    let b = baseline("q3", Batching::Off);
    let g0 = b.gen;
    let bulk = |lo: i64, hi: i64, size: i64| {
        b.w.transaction(|t| {
            for i in lo..hi {
                t.execute("INSERT INTO t(id, v) VALUES (?, zeroblob(?))", &[Value::from_i64(i), Value::from_i64(size)])?;
            }
            Ok(())
        })
        .unwrap()
    };
    for i in 11..14 {
        b.w.execute("INSERT INTO t(id, v) VALUES (?, zeroblob(100))", &[Value::from_i64(i)]).unwrap();
    }
    bulk(10000, 11500, 3000); // about 1,500 frames, so the local checkpoint takes a measurable time
    b.w.checkpoint(CheckpointMode::Truncate).unwrap(); // seal: WAL restarts with new salts
    let g1 = b.wdb.generation();
    b.w.execute("INSERT INTO t(id, v) VALUES (300, zeroblob(10))", &[]).unwrap();
    bulk(200, 220, 2000);
    b.w.checkpoint(CheckpointMode::Truncate).unwrap();
    let g2 = b.wdb.generation();
    b.w.execute("INSERT INTO t(id, v) VALUES (301, zeroblob(10))", &[]).unwrap();
    bulk(400, 410, 1500);
    let expect = match &b.w.query("SELECT count(*) FROM t", &[]).unwrap()[0][0] {
        Value::Numeric(turso_core::Numeric::Integer(i)) => *i,
        v => panic!("{v:?}"),
    };
    println!("writer: generations {g0} {g1} {g2}, expected rows {expect}");

    let frames_of = |gen: u64| {
        let mut rb = Rebuild::default();
        let mut out: Vec<u8> = Vec::new();
        for (_seq, ops) in gen_syncs(b.store.as_ref(), gen) {
            if let Some(ops) = ops {
                for f in rb.feed(&ops) {
                    out.extend_from_slice(&f.2);
                }
            }
        }
        out
    };
    let inputs: PathBuf = b.replica_db_path.parent().unwrap().parent().unwrap().to_path_buf();
    for (i, g) in [g0, g1, g2].iter().enumerate() {
        let bytes = frames_of(*g);
        println!("generation index {i} (log gen {g}): {} frames", bytes.len() / FS);
        std::fs::write(inputs.join(format!("g{i}.frames")), bytes).unwrap();
    }
    let exe = std::env::current_exe().unwrap();
    let cp = |from: &Path, to: &Path| {
        assert!(Command::new("cp").args(["-R", from.to_str().unwrap(), to.to_str().unwrap()]).status().unwrap().success());
    };
    let stages = [
        "mid:0", "end:0", "ckpt:0", "mid:1", "end:1", "ckpt:1", "mid:2",
        "inckpt:1000", "inckpt:3000", "inckpt:6000", "inckpt:10000", "inckpt:15000", "inckpt:25000", "inckpt:40000", "inckpt:70000",
        "none",
    ];
    let mut failures = [0, 0];
    for (n, stage) in stages.iter().enumerate() {
        let killed = inputs.join(format!("killed{n}"));
        std::fs::create_dir_all(killed.join("replica")).unwrap();
        std::fs::copy(&b.replica_db_path, killed.join("replica").join("app.db")).unwrap();
        for i in 0..3 {
            std::fs::copy(inputs.join(format!("g{i}.frames")), killed.join(format!("g{i}.frames"))).unwrap();
        }
        println!("=== kill stage {stage}");
        if *stage != "none" {
            let o = Command::new(&exe).args(["child", killed.to_str().unwrap(), stage]).output().unwrap();
            print!("{}", String::from_utf8_lossy(&o.stdout));
            println!("[{stage}] child exit: {:?}", o.status);
            if stage.starts_with("inckpt:") {
                // where did the kill land? ask a child to report the replica's WAL state
                let o = Command::new(&exe).args(["child", killed.to_str().unwrap(), "report"]).output().unwrap();
                print!("{}", String::from_utf8_lossy(&o.stdout));
            }
        }
        for (k, policy) in ["resume_seq", "resume_pos"].iter().enumerate() {
            let run_dir = inputs.join(format!("killed{n}-{policy}"));
            cp(&killed, &run_dir);
            let o = Command::new(&exe).args(["child", run_dir.to_str().unwrap(), policy]).output().unwrap();
            let out = String::from_utf8_lossy(&o.stdout).to_string();
            print!("{out}");
            let drop_seen = out.rsplit("worst_visible_row_drop=").next().unwrap_or("0").trim().parse::<i64>().unwrap_or(-1);
            if !o.status.success() {
                println!("{policy}: RESUME FAILED: {}", String::from_utf8_lossy(&o.stderr));
                failures[k] += 1;
            } else if !out.contains(&format!("rows={expect} ")) || !out.contains("\"ok\"") {
                println!("{policy}: MISMATCH (wanted final rows={expect} and integrity ok)");
                failures[k] += 1;
            } else if drop_seen != 0 {
                println!("{policy}: FINAL OK BUT READERS SAW ROWS STEP BACK by up to {drop_seen}");
                failures[k] += 1;
            } else {
                println!("{policy}: OK rows={expect} integrity ok, no step back");
            }
        }
    }
    println!("SUMMARY stages={} failures: seq-only={} pos-file={}", stages.len(), failures[0], failures[1]);
}
