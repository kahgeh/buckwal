//! Q1: can the WAL byte ranges in the writer's log records always be rebuilt into whole frames,
//! and which frames carry a commit? Drives a real writer on a MemStore, then replays the log
//! sync by sync into a WAL image and inspects the image after every sync.
use probe::*;
use turso_durable::store::ObjectStore;
use turso_durable::{open_with_store, Batching, CheckpointMode, Value};

fn main() {
    for b in [
        Batching::Off,
        Batching::Window { max_wait: std::time::Duration::from_millis(5), max_commits: 64, max_bytes: 1 << 20 },
    ] {
        println!("=== batching {b:?}");
        run(b);
    }
}

fn run(b: Batching) {
    let store = mem();
    let dir = fresh_dir("q1").join("db");
    let db = open_with_store(config(&dir, b), store.clone()).unwrap();
    let c = db.connect().unwrap();
    c.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)").unwrap();
    for i in 0..5 {
        c.execute("INSERT INTO t(id, v) VALUES (?, zeroblob(?))", &[Value::from_i64(i), Value::from_i64(200 + i * 5000)]).unwrap();
    }
    // a multi-statement transaction touching many pages
    c.transaction(|t| {
        for i in 100..140 {
            t.execute("INSERT INTO t(id, v) VALUES (?, zeroblob(3000))", &[Value::from_i64(i)])?;
        }
        Ok(())
    })
    .unwrap();
    // a checkpoint, then more writes (WAL restart, header rewrite, truncate)
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    for i in 200..205 {
        c.execute("INSERT INTO t(id, v) VALUES (?, zeroblob(500))", &[Value::from_i64(i)]).unwrap();
    }
    c.checkpoint(CheckpointMode::Truncate).unwrap();
    c.execute("INSERT INTO t(id, v) VALUES (300, zeroblob(10))", &[]).unwrap();
    println!("writer generation now {}", db.generation());
    drop(c);
    db.close().unwrap();

    let log = read_log(store.as_ref());
    let mut img: Vec<u8> = Vec::new();
    let mut gen = u64::MAX;
    let (mut syncs_n, mut straddle, mut partial_after_sync, mut non_commit_end) = (0, 0, 0, 0);
    for o in &log {
        if o.gen != gen {
            gen = o.gen;
            img.clear();
            println!("-- generation {gen}");
        }
        match &o.rec {
            Rec::Fence { epoch } => println!("log {}/{}: fence epoch {epoch}", o.gen, o.seq),
            Rec::Extents { syncs } => {
                if syncs.is_empty() {
                    println!("log {}/{}: empty (seal start)", o.gen, o.seq);
                }
                for ops in syncs {
                    syncs_n += 1;
                    let before = img.len();
                    // does any single write start or end mid-frame (relative to the frame grid)?
                    let ps = wal_page_size(&img);
                    let mut desc = Vec::new();
                    for op in ops {
                        match op {
                            Op::Write { off, bytes } => {
                                if let Some(ps) = ps {
                                    let fs = (FRAME_HDR + ps) as u64;
                                    if *off >= WAL_HDR as u64 && ((*off - WAL_HDR as u64) % fs != 0 || bytes.len() as u64 % fs != 0) {
                                        straddle += 1;
                                    }
                                }
                                desc.push(format!("W@{off}+{}", bytes.len()));
                            }
                            Op::Truncate { len } => desc.push(format!("T{len}")),
                        }
                        apply_ops(&mut img, std::slice::from_ref(op));
                    }
                    let ps = wal_page_size(&img);
                    let whole = match ps {
                        Some(ps) => (img.len() >= WAL_HDR) && (img.len() - WAL_HDR) % (FRAME_HDR + ps) == 0,
                        None => img.is_empty(),
                    };
                    if !whole {
                        partial_after_sync += 1;
                    }
                    let frames = whole_frames(&img);
                    let last_commit = frames.last().map(|f| f.1 != 0);
                    if matches!(last_commit, Some(false)) {
                        non_commit_end += 1;
                    }
                    println!(
                        "log {}/{}: sync ops [{}] img {}->{} whole_frames={} frames={} last_frame_commit={:?}",
                        o.gen, o.seq, desc.join(" "), before, img.len(), whole, frames.len(), last_commit
                    );
                }
            }
        }
    }
    println!(
        "SUMMARY syncs={syncs_n} writes_off_frame_grid={straddle} syncs_ending_mid_frame={partial_after_sync} syncs_ending_on_noncommit_frame={non_commit_end} log_objects={} store_gets={}",
        log.len(),
        store.stats().gets
    );
}
