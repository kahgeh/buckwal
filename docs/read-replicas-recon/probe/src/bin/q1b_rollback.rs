//! Q1 follow-up: a large transaction that spills pages to the WAL and then rolls back. Does the log
//! hold non-commit frames, and does a later write land below the end of the rebuilt image (a rewrite
//! of frames already shipped)?
use probe::*;
use turso_durable::{open_with_store, Batching, Value};

fn main() {
    let store = mem();
    let dir = fresh_dir("q1b").join("db");
    let db = open_with_store(config(&dir, Batching::Off), store.clone()).unwrap();
    let c = db.connect().unwrap();
    c.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, v BLOB)").unwrap();
    c.execute("INSERT INTO t(id, v) VALUES (1, zeroblob(100))", &[]).unwrap();
    // ~20 MB of dirty pages in one transaction, then roll it back
    let r: Result<(), turso_durable::Error> = c.transaction(|t| {
        for i in 1000..7000 {
            t.execute("INSERT INTO t(id, v) VALUES (?, zeroblob(3500))", &[Value::from_i64(i)])?;
        }
        Err(turso_durable::Error::Config("rollback on purpose".into()))
    });
    println!("big transaction: {r:?}");
    for i in 2..5 {
        c.execute("INSERT INTO t(id, v) VALUES (?, zeroblob(100))", &[Value::from_i64(i)]).unwrap();
    }
    println!("rows after: {:?}", c.query("SELECT count(*) FROM t", &[]).unwrap());
    drop(c);
    db.close().unwrap();

    let log = read_log(store.as_ref());
    let mut img: Vec<u8> = Vec::new();
    let mut gen = u64::MAX;
    let (mut syncs, mut below_end, mut noncommit_end, mut max_img) = (0, 0, 0, 0);
    let (mut below_commit, mut committed_end) = (0usize, WAL_HDR);
    for o in &log {
        if o.gen != gen {
            gen = o.gen;
            img.clear();
        }
        let Rec::Extents { syncs: ss } = &o.rec else { continue };
        for ops in ss {
            syncs += 1;
            for op in ops {
                if let Op::Write { off, .. } = op {
                    if *off >= WAL_HDR as u64 && (*off as usize) < img.len() {
                        below_end += 1;
                    }
                    if *off >= WAL_HDR as u64 && (*off as usize) < committed_end {
                        below_commit += 1;
                    }
                }
                apply_ops(&mut img, std::slice::from_ref(op));
            }
            let frames = whole_frames(&img);
            if let Some(last) = frames.last() {
                if last.1 == 0 {
                    noncommit_end += 1;
                }
            }
            // the committed prefix: up to and including the last frame with db_size != 0
            let n_committed = frames.iter().rposition(|f| f.1 != 0).map(|i| i + 1).unwrap_or(0);
            let ps = wal_page_size(&img).unwrap_or(4096);
            committed_end = WAL_HDR + n_committed * (FRAME_HDR + ps);
            println!("  sync: image {} bytes, {} frames, committed prefix {} frames, uncommitted tail {} frames", img.len(), frames.len(), n_committed, frames.len() - n_committed);
            max_img = max_img.max(img.len());
        }
    }
    println!("log objects {} syncs {syncs} largest WAL image {max_img} bytes", log.len());
    println!("writes landing below the end of the image so far (rewrites of shipped frames): {below_end}");
    println!("syncs ending on a non-commit frame: {noncommit_end}");
    println!("writes landing below the last commit frame (rewrites of committed frames): {below_commit}");
}
