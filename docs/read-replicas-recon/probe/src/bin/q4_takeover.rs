//! Q4: across a takeover. Writer A keeps committing while writer B takes over the bucket from
//! another directory. The log gains a fence record, then B's records. The follower applies the
//! whole log in order (skipping fences, checkpointing between generations) and every commit A
//! was acked must be on the replica, together with B's, and nothing A was refused.
use probe::*;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use turso_durable::{open_with_store, takeover_with_store, Batching, Value};

fn main() {
    for b in [
        Batching::Off,
        Batching::Window { max_wait: std::time::Duration::from_millis(5), max_commits: 64, max_bytes: 1 << 20 },
    ] {
        println!("=== batching {b:?}");
        run(b);
    }
}

fn run(batching: Batching) {
    let base = baseline("q4", batching);
    let store = base.store.clone();
    let root = base.replica_db_path.parent().unwrap().parent().unwrap().to_path_buf();
    let acked_a: Arc<Mutex<Vec<i64>>> = Arc::default();
    let failed_a: Arc<Mutex<Vec<(i64, String)>>> = Arc::default();
    let stop = Arc::new(AtomicBool::new(false));
    let a_conn = base.w;
    let (acked2, failed2, stop2) = (acked_a.clone(), failed_a.clone(), stop.clone());
    let a = std::thread::spawn(move || {
        let mut i = 1000;
        while !stop2.load(SeqCst) {
            match a_conn.execute("INSERT INTO t(id, v) VALUES (?, zeroblob(500))", &[Value::from_i64(i)]) {
                Ok(_) => acked2.lock().unwrap().push(i),
                Err(e) => {
                    failed2.lock().unwrap().push((i, format!("{e}")));
                    break;
                }
            }
            i += 1;
        }
    });
    std::thread::sleep(std::time::Duration::from_millis(300));
    let dir_b = root.join("writer-b");
    let rep = takeover_with_store(config(&dir_b, batching), store.clone()).unwrap();
    println!("takeover report: {rep:?}");
    let bdb = open_with_store(config(&dir_b, batching), store.clone()).unwrap();
    let bc = bdb.connect().unwrap();
    let mut b_ids = vec![];
    for i in 5000..5010 {
        bc.execute("INSERT INTO t(id, v) VALUES (?, zeroblob(300))", &[Value::from_i64(i)]).unwrap();
        b_ids.push(i);
    }
    bc.transaction(|t| {
        for i in 6000..6030 {
            t.execute("INSERT INTO t(id, v) VALUES (?, zeroblob(3000))", &[Value::from_i64(i)])?;
        }
        Ok(())
    })
    .unwrap();
    b_ids.extend(6000..6030);
    a.join().unwrap();
    stop.store(true, SeqCst);
    let acked: Vec<i64> = acked_a.lock().unwrap().clone();
    println!("A acked {} commits, refused: {:?}", acked.len(), failed_a.lock().unwrap());
    println!("B generation {} acked {} rows", bdb.generation(), b_ids.len());

    let log = read_log(store.as_ref());
    // the log, as the follower sees it: one line per generation, fences and seal starts called out
    for g in log.iter().map(|o| o.gen).filter(|g| *g >= base.gen).collect::<BTreeSet<_>>() {
        let objs: Vec<_> = log.iter().filter(|o| o.gen == g).collect();
        let mut notes = Vec::new();
        for o in &objs {
            match &o.rec {
                Rec::Fence { epoch } => notes.push(format!("seq {} FENCE epoch {epoch}", o.seq)),
                Rec::Extents { syncs } if syncs.is_empty() => notes.push(format!("seq {} empty (seal start)", o.seq)),
                _ => {}
            }
        }
        println!("log generation {g}: {} objects (seq 1..{}), {}", objs.len(), objs.last().unwrap().seq, notes.join(", "));
    }

    // the follower: all generations from the baseline's on, in order
    let (db, c) = open_core(&base.replica_db_path);
    let gens: BTreeSet<u64> = log.iter().map(|o| o.gen).filter(|g| *g >= base.gen).collect();
    let mut first = true;
    for g in &gens {
        if !first {
            let busy = ckpt_retry(&c);
            println!("local TRUNCATE checkpoint before generation {g} (Busy answers: {busy})");
        }
        first = false;
        let mut rb = Rebuild::default();
        let mut frame_no = 0u64;
        let mut fences_skipped = 0;
        let mut frames_total = 0;
        for (_seq, ops) in gen_syncs(store.as_ref(), *g) {
            let Some(ops) = ops else { fences_skipped += 1; continue };
            let frames = rb.feed(&ops);
            if frames.is_empty() {
                continue;
            }
            c.wal_insert_begin().unwrap();
            for f in &frames {
                frame_no += 1;
                c.wal_insert_frame(frame_no, &f.2).unwrap();
            }
            c.wal_insert_end(true).unwrap();
            frames_total += frames.len();
        }
        println!("applied generation {g}: {frames_total} frames, {fences_skipped} fence record(s) skipped");
    }
    let replica: BTreeSet<i64> = ids(&c).into_iter().collect();
    let expect: BTreeSet<i64> = (1..=10).chain(acked.iter().copied()).chain(b_ids.iter().copied()).collect();
    let missing: Vec<_> = expect.difference(&replica).collect();
    let extra: Vec<_> = replica.difference(&expect).collect();
    println!(
        "replica rows {} expected (baseline + A acked + B acked) {} missing={missing:?} extra(not acked, in log before fence)={extra:?} integrity={}",
        replica.len(), expect.len(), integrity(&c)
    );
    println!("{}", if missing.is_empty() { "RESULT every acked commit is on the replica" } else { "RESULT ACKED COMMITS MISSING" });
    drop(db);
}
