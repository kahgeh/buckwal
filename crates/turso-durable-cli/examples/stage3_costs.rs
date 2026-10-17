//! Stage-3 cost measurements (`results/writer-lease.txt`): the bucket requests of a takeover,
//! of a writer open, and of the lease's head check after an idle pause. Settings come from the
//! `DURABLE_*` variables, as for the CLI. Each command prints `key=value` lines, then every
//! request it made as `request <op> <key>` (a `stat` is a HEAD, counted as a GET).
//!
//! ```text
//! cargo run --release --locked -p turso-durable-cli --example stage3_costs -- takeover DIR
//! cargo run --release --locked -p turso-durable-cli --example stage3_costs -- open DIR
//! cargo run --release --locked -p turso-durable-cli --example stage3_costs -- lease DIR PAUSE_MS
//! ```
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use turso_durable::store::fault::FaultStore;
use turso_durable::store::s3::{S3Config, S3Store};
use turso_durable::store::{ObjectStore, StatsSnap};
use turso_durable::{Config, Database, Value};

type R<T> = Result<T, String>;

fn err<E: std::fmt::Display>(e: E) -> String {
    e.to_string()
}

/// The S3 store `config` names, behind a `FaultStore` that records every request.
fn store(config: &Config) -> R<Arc<FaultStore>> {
    let s3 = S3Store::new(S3Config {
        endpoint: config.endpoint.clone(),
        region: config.region.clone(),
        bucket: config.bucket.clone(),
        access_key: config.access_key.clone(),
        secret_key: config.secret_key.clone(),
        path_style: config.path_style,
        request_timeout: config.request_timeout,
    })
    .map_err(err)?;
    Ok(Arc::new(FaultStore::new(Arc::new(s3))))
}

fn stats(tag: &str, s: &StatsSnap) {
    println!("{tag}puts={}", s.puts);
    println!("{tag}gets={}", s.gets);
    println!("{tag}lists={}", s.lists);
    println!("{tag}deletes={}", s.deletes);
}

fn requests(events: &[String]) {
    for e in events {
        println!("request {e}");
    }
}

/// Take the database over into the empty `dir` (`takeover_with_store`).
fn takeover(dir: PathBuf) -> R<()> {
    let config = Config::from_env(dir).map_err(err)?;
    let fault = store(&config)?;
    let r = turso_durable::takeover_with_store(config, fault.clone()).map_err(err)?;
    println!("generation={}", r.generation);
    println!("epoch={}", r.epoch);
    println!("fence_seq={}", r.fence_seq);
    println!("quiet_ms={}", r.quiet.as_millis());
    println!("ms={}", r.elapsed.as_millis());
    println!("restore_log_objects={}", r.restore.log_objects);
    println!("restore_gets={}", r.restore.gets);
    println!("restore_lists={}", r.restore.lists);
    println!("restore_ms={}", r.restore.elapsed.as_millis());
    stats("total_", &fault.stats());
    requests(&fault.events());
    Ok(())
}

/// Open `dir` as the writer and report the requests of the open alone, then close it.
fn open(dir: PathBuf) -> R<()> {
    let config = Config::from_env(dir).map_err(err)?;
    let fault = store(&config)?;
    let t = Instant::now();
    let db = turso_durable::open_with_store(config, fault.clone()).map_err(err)?;
    println!("open_ms={}", t.elapsed().as_millis());
    println!("generation={}", db.generation());
    stats("open_", &db.bucket_stats());
    requests(&fault.events());
    let n = fault.events().len();
    let before = db.bucket_stats();
    db.close().map_err(err)?;
    stats("close_", &fault.stats().since(&before));
    for e in &fault.events()[n..] {
        println!("close_request {e}");
    }
    Ok(())
}

/// Two commits back to back, then one after `pause`: the bucket requests of each.
fn lease(dir: PathBuf, pause: Duration) -> R<()> {
    let config = Config::from_env(dir).map_err(err)?;
    println!("lease_ms={}", config.lease.as_millis());
    println!("pause_ms={}", pause.as_millis());
    let fault = store(&config)?;
    let db = turso_durable::open_with_store(config, fault.clone()).map_err(err)?;
    let conn = db.connect().map_err(err)?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS lease_cost(id INTEGER PRIMARY KEY, v BLOB)",
        &[],
    )
    .map_err(err)?;
    let commit = |id: i64| -> R<()> {
        conn.execute(
            "INSERT OR REPLACE INTO lease_cost(id, v) VALUES (?, ?)",
            &[Value::from_i64(id), Value::from_text("x")],
        )
        .map(|_| ())
        .map_err(err)
    };
    commit(1)?;
    let measure = |tag: &str, db: &Database, run: &dyn Fn() -> R<()>| -> R<()> {
        let (before, n) = (db.bucket_stats(), fault.events().len());
        run()?;
        stats(tag, &db.bucket_stats().since(&before));
        for e in &fault.events()[n..] {
            println!("{tag}request {e}");
        }
        Ok(())
    };
    measure("no_pause_", &db, &|| commit(2))?;
    std::thread::sleep(pause);
    measure("after_pause_", &db, &|| commit(3))?;
    conn.close().map_err(err)?;
    db.close().map_err(err)
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let r = match args.iter().map(String::as_str).collect::<Vec<_>>()[..] {
        ["takeover", dir] => takeover(dir.into()),
        ["open", dir] => open(dir.into()),
        ["lease", dir, ms] => ms
            .parse()
            .map_err(err)
            .and_then(|ms| lease(dir.into(), Duration::from_millis(ms))),
        _ => Err("usage: stage3_costs takeover DIR | open DIR | lease DIR PAUSE_MS".into()),
    };
    if let Err(e) = r {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}
