//! Q1 follow-up 2: apply the rolled-back-transaction log (q1b's scenario) through the frame API with the
//! committed-prefix rule: after each sync, only frames up to the last commit frame are applied,
//! the uncommitted tail is held back, and a later write that rewinds below the tail discards it.
//! Also counts what a naive "apply every whole frame" would have done.
use probe::*;
use turso_durable::{Batching, Value};

fn main() {
    let b = baseline("q1c", Batching::Off);
    let r: Result<(), turso_durable::Error> = b.w.transaction(|t| {
        for i in 1000..7000 {
            t.execute("INSERT INTO t(id, v) VALUES (?, zeroblob(3500))", &[Value::from_i64(i)])?;
        }
        Err(turso_durable::Error::Config("rollback on purpose".into()))
    });
    println!("big transaction: {r:?}");
    for i in 11..15 {
        b.w.execute("INSERT INTO t(id, v) VALUES (?, zeroblob(100))", &[Value::from_i64(i)]).unwrap();
    }
    b.w.transaction(|t| {
        for i in 100..110 {
            t.execute("INSERT INTO t(id, v) VALUES (?, zeroblob(3000))", &[Value::from_i64(i)])?;
        }
        Ok(())
    })
    .unwrap();
    println!("writer rows {:?}", b.w.query("SELECT count(*) FROM t", &[]).unwrap());

    let (db, c) = open_core(&b.replica_db_path);
    let reader = db.connect().unwrap();
    let mut img: Vec<u8> = Vec::new();
    let mut applied = 0usize; // frames of the image already applied (all committed)
    let (mut syncs, mut held_back_max) = (0, 0usize);
    for (_seq, ops) in gen_syncs(b.store.as_ref(), b.gen) {
        let Some(ops) = ops else { continue };
        syncs += 1;
        for op in &ops {
            apply_ops(&mut img, std::slice::from_ref(op));
        }
        let frames = whole_frames(&img);
        let committed = frames.iter().rposition(|f| f.1 != 0).map(|i| i + 1).unwrap_or(0);
        assert!(committed >= applied, "a write rewound below an applied (committed) frame");
        let tail = frames.len() - committed;
        held_back_max = held_back_max.max(tail);
        println!("  sync {syncs}: image has {} whole frames, committed prefix {committed}, uncommitted tail {tail}, applied so far {applied}", frames.len());
        // apply [applied..committed) one transaction at a time
        let mut i = applied;
        while i < committed {
            let mut j = i;
            while frames[j].1 == 0 {
                j += 1;
            }
            c.wal_insert_begin().unwrap();
            for (k, f) in frames[i..=j].iter().enumerate() {
                c.wal_insert_frame((i + k) as u64 + 1, f.2).unwrap();
            }
            c.wal_insert_end(true).unwrap();
            i = j + 1;
        }
        applied = committed;
    }
    println!("syncs {syncs}, applied {applied} frames, largest uncommitted tail held back (never applied) {held_back_max} frames");
    println!("replica rows {} integrity {} local wal_state {:?}", count(&reader), integrity(&reader), c.wal_state().unwrap());
}
