//! Q2: do rebuilt frames apply through wal_insert_begin / wal_insert_frame / wal_insert_end into a
//! copy of the writer's database, do readers on other connections in the same process see a commit
//! only after its last frame, and what does the API do with a repeated frame and with a gap?
use probe::*;
use buckwal::{Batching, Value};

fn main() {
    let b = baseline("q2", Batching::Off);
    println!("baseline: writer generation {} rows 10", b.gen);
    // writer: one single-row commit, one 40-row transaction (many frames), one more single-row commit
    b.w.execute("INSERT INTO t(id, v) VALUES (11, zeroblob(100))", &[]).unwrap();
    b.w.transaction(|t| {
        for i in 100..140 {
            t.execute("INSERT INTO t(id, v) VALUES (?, zeroblob(3000))", &[Value::from_i64(i)])?;
        }
        Ok(())
    })
    .unwrap();
    b.w.execute("INSERT INTO t(id, v) VALUES (12, zeroblob(100))", &[]).unwrap();
    let writer_rows = b.w.query("SELECT count(*) FROM t", &[]).unwrap();
    println!("writer rows: {writer_rows:?}");

    let (_db, apply) = open_core(&b.replica_db_path);
    let reader = _db.connect().unwrap();
    println!("replica before: rows={} integrity={}", count(&reader), integrity(&reader));

    let syncs = gen_syncs(b.store.as_ref(), b.gen);
    let mut rb = Rebuild::default();
    let mut frame_no = 0u64; // local WAL frame numbers start at 1
    let mut first = true;
    for (seq, ops) in &syncs {
        let Some(ops) = ops else { continue };
        let frames = rb.feed(ops);
        if frames.is_empty() {
            println!("log {}/{seq}: no frames (header or truncate only)", b.gen);
            continue;
        }
        apply.wal_insert_begin().unwrap();
        for (i, (page, db_size, bytes)) in frames.iter().enumerate() {
            frame_no += 1;
            let info = apply.wal_insert_frame(frame_no, bytes).unwrap();
            let last = i + 1 == frames.len();
            // a reader on another connection, mid-transaction
            let seen = count(&reader);
            println!(
                "log {}/{seq}: frame_no={frame_no} page={page} db_size_in_hdr={db_size} info={info:?} last_in_sync={last} reader_rows={seen}",
                b.gen
            );
        }
        apply.wal_insert_end(true).unwrap();
        println!("log {}/{seq}: wal_insert_end(true) done. reader_rows={}", b.gen, count(&reader));
        first = false;
    }
    let _ = first;
    println!("replica after: rows={} integrity={}", count(&reader), integrity(&reader));

    // repeat of an applied frame: same bytes, then changed bytes
    let last_frames: Vec<_> = whole_frames(&rb.img).iter().map(|f| (f.0, f.1, f.2.to_vec())).collect();
    let n = last_frames.len() as u64;
    apply.wal_insert_begin().unwrap();
    let r_same = apply.wal_insert_frame(n, &last_frames[(n - 1) as usize].2);
    println!("repeat last frame {n} same bytes: {r_same:?}");
    let mut changed = last_frames[(n - 1) as usize].2.clone();
    let l = changed.len();
    changed[l - 10] ^= 0xff;
    let r_diff = apply.wal_insert_frame(n, &changed);
    println!("repeat last frame {n} changed bytes: {r_diff:?}");
    let r_old = apply.wal_insert_frame(1, &last_frames[0].2);
    println!("repeat frame 1 (applied long ago) same bytes: {r_old:?}");
    // gap
    let r_gap = apply.wal_insert_frame(n + 2, &last_frames[(n - 1) as usize].2);
    println!("gap: frame {} when max is {n}: {r_gap:?}", n + 2);
    apply.wal_insert_end(false).unwrap();
    println!("after rollback: state={:?} rows={}", apply.wal_state().unwrap(), count(&reader));

    // uncommitted frames then end(false) must vanish
    apply.wal_insert_begin().unwrap();
    let mut extra = last_frames[(n - 1) as usize].2.clone();
    extra[4..8].copy_from_slice(&0u32.to_be_bytes()); // clear db_size: not a commit
    let r_nc = apply.wal_insert_frame(n + 1, &extra);
    println!("non-commit frame {}: {r_nc:?} state={:?}", n + 1, apply.wal_state().unwrap());
    apply.wal_insert_end(false).unwrap();
    println!("after end(false): state={:?} rows={} integrity={}", apply.wal_state().unwrap(), count(&reader), integrity(&reader));
}
