//! Q6: a reader holds a long read transaction while the follower applies frames and checkpoints.
//! Measures: do frame inserts block or fail, does the reader keep its snapshot, what do PASSIVE and
//! TRUNCATE checkpoints answer while the reader is open, and how long they take to answer. Then the
//! seal case: the follower cannot truncate its WAL, so it keeps numbering frames on (the local WAL
//! need not restart where the writer's did).
use probe::*;
use std::time::Instant;
use turso_core::CheckpointMode as M;
use turso_durable::{Batching, CheckpointMode, Value};

fn frames_of(store: &dyn turso_durable::store::ObjectStore, gen: u64) -> Vec<Vec<u8>> {
    let mut rb = Rebuild::default();
    let mut out = vec![];
    for (_s, ops) in gen_syncs(store, gen) {
        if let Some(ops) = ops {
            out.extend(rb.feed(&ops).into_iter().map(|f| f.2));
        }
    }
    out
}

fn is_commit(f: &[u8]) -> bool {
    u32::from_be_bytes(f[4..8].try_into().unwrap()) != 0
}

/// Apply whole transactions, local frame numbers continuing from the local WAL's max_frame.
fn apply(c: &std::sync::Arc<turso_core::Connection>, frames: &[Vec<u8>]) -> (usize, std::time::Duration) {
    let t0 = Instant::now();
    let mut i = 0;
    let mut txs = 0;
    while i < frames.len() {
        let mut j = i;
        while !is_commit(&frames[j]) {
            j += 1;
        }
        c.wal_insert_begin().unwrap();
        let mut no = c.wal_state().unwrap().max_frame;
        for f in &frames[i..=j] {
            no += 1;
            c.wal_insert_frame(no, f).unwrap();
        }
        c.wal_insert_end(true).unwrap();
        txs += 1;
        i = j + 1;
    }
    (txs, t0.elapsed())
}

fn main() {
    let b = baseline("q6", Batching::Off);
    let g0 = b.gen;
    for i in 11..14 {
        b.w.execute("INSERT INTO t(id, v) VALUES (?, zeroblob(100))", &[Value::from_i64(i)]).unwrap();
    }
    b.w.transaction(|t| {
        for i in 100..140 {
            t.execute("INSERT INTO t(id, v) VALUES (?, zeroblob(3000))", &[Value::from_i64(i)])?;
        }
        Ok(())
    })
    .unwrap();
    b.w.checkpoint(CheckpointMode::Truncate).unwrap();
    let g1 = b.wdb.generation();
    b.w.transaction(|t| {
        for i in 200..220 {
            t.execute("INSERT INTO t(id, v) VALUES (?, zeroblob(2000))", &[Value::from_i64(i)])?;
        }
        Ok(())
    })
    .unwrap();
    let want = match &b.w.query("SELECT count(*) FROM t", &[]).unwrap()[0][0] {
        Value::Numeric(turso_core::Numeric::Integer(i)) => *i,
        v => panic!("{v:?}"),
    };
    let f0 = frames_of(b.store.as_ref(), g0);
    let f1 = frames_of(b.store.as_ref(), g1);
    println!("writer generations {g0} {g1}, rows at the end {want}, frames {} + {}", f0.len(), f1.len());

    let (db, apply_c) = open_core(&b.replica_db_path);
    let reader = db.connect().unwrap();
    let fresh = db.connect().unwrap();
    reader.execute("BEGIN").unwrap();
    println!("reader BEGIN, rows seen {}", count(&reader));

    let (txs, took) = apply(&apply_c, &f0);
    println!("follower applied generation {g0}: {txs} transactions in {took:?}, no Busy, while the reader holds its snapshot");
    println!("  reader rows (same tx) {} | new connection rows {}", count(&reader), count(&fresh));

    let t = Instant::now();
    let p = apply_c.checkpoint(M::Passive { upper_bound_inclusive: None });
    println!("PASSIVE checkpoint while reader open: {:?} in {:?}", p.as_ref().map(|r| (r.wal_max_frame, r.wal_total_backfilled)), t.elapsed());
    println!("  reader rows (same tx) {} | integrity on new connection {}", count(&reader), integrity(&fresh));

    let t = Instant::now();
    let tr = ckpt_truncate(&apply_c);
    println!("TRUNCATE checkpoint while reader open: {:?} answered in {:?} (no wait)", tr.as_ref().map(|r| r.wal_max_frame).map_err(|e| format!("{e:?}")), t.elapsed());
    let t = Instant::now();
    let mut tries = 0;
    while ckpt_truncate(&apply_c).is_err() && t.elapsed() < std::time::Duration::from_secs(2) {
        tries += 1;
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    println!("TRUNCATE retried {tries} times over {:?}, still Busy while the reader is open", t.elapsed());

    // the seal: the follower cannot restart its WAL, so it keeps numbering frames on
    let before = apply_c.wal_state().unwrap();
    let (txs, took) = apply(&apply_c, &f1);
    let after = apply_c.wal_state().unwrap();
    println!("generation {g1} applied without a WAL restart: {txs} transactions in {took:?}, local max_frame {} -> {}", before.max_frame, after.max_frame);
    println!("  reader rows (same tx) {} | new connection rows {} (want {want})", count(&reader), count(&fresh));

    reader.execute("COMMIT").unwrap();
    let t = Instant::now();
    let tr = ckpt_retry(&apply_c);
    println!("reader COMMIT, then TRUNCATE: ok after {tr} Busy answers in {:?}; wal_state {:?}", t.elapsed(), apply_c.wal_state().unwrap());
    println!("final rows {} (want {want}) integrity {}", count(&fresh), integrity(&fresh));
    // the reader that started before sees the new data on its next transaction
    println!("old reader connection, new transaction: rows {}", count(&reader));

    // ---- second scenario: a reader that begins AFTER some frames (a non-zero read mark)
    println!("--- reader with a non-zero read mark");
    // a second baseline: the first replica copy has been changed by the first scenario
    let b2 = baseline("q6c", Batching::Off);
    for i in 11..14 {
        b2.w.execute("INSERT INTO t(id, v) VALUES (?, zeroblob(100))", &[Value::from_i64(i)]).unwrap();
    }
    b2.w.transaction(|t| {
        for i in 100..140 {
            t.execute("INSERT INTO t(id, v) VALUES (?, zeroblob(3000))", &[Value::from_i64(i)])?;
        }
        Ok(())
    })
    .unwrap();
    let fr = frames_of(b2.store.as_ref(), b2.gen);
    let (db2, ap2) = open_core(&b2.replica_db_path);
    let rd2 = db2.connect().unwrap();
    // apply the first three single-row commits, then start the reader on that snapshot
    let first_txs: usize = {
        let mut n = 0;
        let mut txs = 0;
        for (i, f) in fr.iter().enumerate() {
            if is_commit(f) {
                txs += 1;
                if txs == 3 {
                    n = i + 1;
                    break;
                }
            }
        }
        n
    };
    apply(&ap2, &fr[..first_txs]);
    rd2.execute("BEGIN").unwrap();
    println!("reader BEGIN after {first_txs} frames, rows seen {}", count(&rd2));
    apply(&ap2, &fr[first_txs..]);
    let t = Instant::now();
    let p = ap2.checkpoint(M::Passive { upper_bound_inclusive: None });
    println!("PASSIVE with that reader open: {:?} in {:?}", p.as_ref().map(|r| (r.wal_max_frame, r.wal_total_backfilled, r.wal_checkpoint_backfilled)).map_err(|e| format!("{e:?}")), t.elapsed());
    println!("  reader rows (same tx) {} | integrity on a new connection {}", count(&rd2), integrity(&db2.connect().unwrap()));
    let t = Instant::now();
    let tr = ckpt_truncate(&ap2);
    println!("TRUNCATE with that reader open: {:?} in {:?}", tr.as_ref().map(|r| r.wal_max_frame).map_err(|e| format!("{e:?}")), t.elapsed());
    println!("  reader rows (same tx) {} | wal_state {:?}", count(&rd2), ap2.wal_state().unwrap());
    rd2.execute("COMMIT").unwrap();
    let busy = ckpt_retry(&ap2);
    println!("after the reader's COMMIT: TRUNCATE ok after {busy} Busy answers, wal_state {:?}, rows {}", ap2.wal_state().unwrap(), count(&db2.connect().unwrap()));
}
