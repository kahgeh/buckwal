//! The timed packer test (spec "Tests": a timed test on RustFS through a delay proxy). It
//! measures the four stage 2 goals of the packer spec:
//!
//! 1. 50 ms added per request, 1 GB, commits that change 10% of the pages: no commit waits
//!    longer than about four small requests (its own log upload, the seal-start record, the
//!    head move and the WAL header record). The size-limit checkpoint pause is measured on
//!    its own (the spec: the README states its value).
//!    Two short phases at 50 ms add the commit that restarts the WAL itself (a PASSIVE
//!    checkpoint, then one commit). Its goal is its request count plus at most
//!    `RESTART_MARGIN` (1.5) small requests (spec rev 33). One small request is the median
//!    small PUT of the same run. With batching Off, the count is 4 (its own log record and
//!    three small requests), so the goal is at most 5.5. As a background commit
//!    (`background 5ms 64 1MiB 1000`, a full queue), the count is `BACKGROUND_UPLOADS` + 3
//!    = 21 (the records queued before it, then three small requests), so the goal is at
//!    most 22.5.
//! 2. 20 ms added: a restore of a 1 GB pack with no log after it is fully local within 10 s,
//!    and its open returns before the blocks are all local (Choice C).
//! 3. 20 ms added: a pack run on 1 GB, 10% changed, finishes within 30 s.
//! 4. 20 ms added: two packers at once leave one newest pack and no extra objects after GC.
//!
//! It is not part of `./scripts/test.sh` runs: the script skips every test whose name has
//! `timed_goal_` (and the `sys` and `rest` groups never name this binary). Run it alone on the
//! solo container, plain HTTP only:
//!
//! ```text
//! export BUCKWAL_RUSTFS_NAME=buckwal-rustfs-w BUCKWAL_RUSTFS_VOLUME=buckwal-objects-w \
//!   BUCKWAL_RUSTFS_PORT=9298
//! scripts/rustfs.sh reset && eval "$(scripts/rustfs.sh env)"
//! cargo test --locked --release -p buckwal-cli --test timed -- --ignored --nocapture
//! scripts/rustfs.sh down
//! ```
//!
//! `BUCKWAL_TIMED_MB` sets the database size (default 1024), `BUCKWAL_TIMED_CYCLES` the
//! number of 10% change cycles at 50 ms (default 2), and `BUCKWAL_TIMED_RESULTS` a file that
//! gets the result lines. The test prints every number, then fails when a goal is missed.
//!
//! The delay proxy is a TCP proxy in this file: it holds the first bytes of each new request
//! on a connection (the first bytes after a response, or the first on a new connection) for
//! the delay, then forwards. So each request waits the delay once, and a large body or
//! response streams at full speed after it. A new connection also waits the delay once for
//! its TCP connect.
mod common;

use buckwal::store::s3::{S3Config, S3Store};
use buckwal::store::ObjectStore;
use buckwal::{CheckpointMode, Config, Event, PackOutcome, PackReport, Value};
use common::*;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

/// Bytes of each row's blob: one row fills about one 4 KiB page.
const ROW_BYTES: u64 = 4000;
/// Rows per MiB of database (one row per page).
const ROWS_PER_MIB: u64 = 256;
/// Rows per commit while the database fills (32 MiB of WAL each).
const FILL_BATCH: u64 = 8192;
/// Small commits before each 10% change.
const SMALL_BEFORE: usize = 20;
/// Small commits after the switch and the maintenance run that follow a 10% change.
const SMALL_AFTER: usize = 30;
/// At most this many small commits wait for that switch.
const SMALL_CAP: usize = 1500;
/// The goal values of the spec.
const GOAL_REQUESTS: f64 = 4.0;
/// "About" four: the measured commit passes at up to half a request more.
const ABOUT: f64 = 0.5;
/// The request count of the commit that restarts the WAL with batching Off: its own log
/// record, the seal-start record, the head move and the WAL header record.
const OFF_RESTART_COUNT: f64 = 4.0;
/// The margin of a commit that restarts the WAL, in small requests, above its request count
/// (spec rev 33). It covers the fsync, the WAL header write and the checkpoint.
const RESTART_MARGIN: f64 = 1.5;
/// Tries of the in-commit switch phases.
const IN_COMMIT_TRIES: usize = 3;
/// The background setting of the spec's timed test, its `max_behind`, and the record uploads
/// the spec counts for it (about 57 commits per record).
const BACKGROUND: &str = "background 5ms 64 1MiB 1000";
const MAX_BEHIND: u32 = 1000;
const BACKGROUND_UPLOADS: u32 = 18;
/// A background try counts only with at least this many commits behind (90% of
/// `MAX_BEHIND`): a full queue.
const FULL_QUEUE: u64 = MAX_BEHIND as u64 * 9 / 10;
const GOAL_RESTORE: Duration = Duration::from_secs(10);
const GOAL_PACK: Duration = Duration::from_secs(30);

/// A TCP proxy in front of RustFS that holds each new request for `delay_ms` (module doc).
struct DelayProxy {
    port: u16,
    delay_ms: Arc<AtomicU64>,
    requests: Arc<AtomicU64>,
}

impl DelayProxy {
    fn spawn(upstream: String) -> DelayProxy {
        let l = TcpListener::bind("127.0.0.1:0").expect("bind the proxy");
        let port = l.local_addr().unwrap().port();
        let delay_ms = Arc::new(AtomicU64::new(0));
        let requests = Arc::new(AtomicU64::new(0));
        let (d, r) = (delay_ms.clone(), requests.clone());
        std::thread::spawn(move || {
            for c in l.incoming() {
                let Ok(client) = c else { continue };
                let (d, r, upstream) = (d.clone(), r.clone(), upstream.clone());
                // A new connection waits the delay once for its TCP connect, as on a real
                // link, on its own thread so the accept loop goes on.
                std::thread::spawn(move || {
                    let ms = d.load(SeqCst);
                    if ms > 0 {
                        std::thread::sleep(Duration::from_millis(ms));
                    }
                    // A failed connect drops the client: its request fails, as at an outage.
                    let Ok(server) = TcpStream::connect(&upstream) else {
                        return;
                    };
                    pipe(client, server, d, r);
                });
            }
        });
        DelayProxy {
            port,
            delay_ms,
            requests,
        }
    }

    fn endpoint(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    fn set_delay(&self, ms: u64) {
        self.delay_ms.store(ms, SeqCst);
    }
}

/// Copy both ways between `client` and `server`, on two threads. A read from the client
/// after a response (or the first read of the connection) waits the delay first.
fn pipe(client: TcpStream, server: TcpStream, delay_ms: Arc<AtomicU64>, requests: Arc<AtomicU64>) {
    let _ = client.set_nodelay(true);
    let _ = server.set_nodelay(true);
    let (Ok(client2), Ok(server2)) = (client.try_clone(), server.try_clone()) else {
        return;
    };
    let answered = Arc::new(AtomicBool::new(true));
    let a = answered.clone();
    std::thread::spawn(move || {
        let (mut from, mut to) = (client, server);
        let mut buf = vec![0u8; 256 * 1024];
        loop {
            let n = match from.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => n,
            };
            if a.swap(false, SeqCst) {
                requests.fetch_add(1, SeqCst);
                let ms = delay_ms.load(SeqCst);
                if ms > 0 {
                    std::thread::sleep(Duration::from_millis(ms));
                }
            }
            if to.write_all(&buf[..n]).is_err() {
                break;
            }
        }
        let _ = to.shutdown(Shutdown::Write);
    });
    std::thread::spawn(move || {
        let (mut from, mut to) = (server2, client2);
        let mut buf = vec![0u8; 256 * 1024];
        loop {
            let n = match from.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => n,
            };
            // Before the write: once the client has the response, its next request must find
            // the flag set, or that request would skip its delay.
            answered.store(true, SeqCst);
            if to.write_all(&buf[..n]).is_err() {
                break;
            }
        }
        let _ = to.shutdown(Shutdown::Write);
    });
}

/// The events the test looks at, with the time each arrived.
type Events = Arc<Mutex<Vec<(Instant, Event)>>>;

fn record(cfg: &mut Config, events: &Events) {
    let ev = events.clone();
    cfg.on_event = Some(Arc::new(move |e: &Event| {
        if matches!(
            e,
            Event::GenerationSwitched { .. }
                | Event::LogUploaded { .. }
                | Event::MaintenanceStarted { .. }
                | Event::MaintenanceFinished { .. }
                | Event::MaintenanceSkipped { .. }
                | Event::Packed { .. }
        ) {
            ev.lock().unwrap().push((Instant::now(), e.clone()));
        }
    }));
}

/// A small, fixed-sequence random number generator (no crate).
struct Lcg(u64);

impl Lcg {
    fn next(&mut self, below: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.0 >> 33) % below
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kind {
    /// One row changed.
    Small,
    /// 10% of the rows changed in one commit.
    Tenth,
    /// An explicit TRUNCATE checkpoint (a WAL restart and a generation switch).
    Checkpoint,
}

struct Commit {
    kind: Kind,
    start: Instant,
    wait: Duration,
}

/// The test's database settings: `dir` under the test's prefix, through `endpoint`, with the
/// packer thread off unless `pack`.
fn cfg_at(ctx: &Ctx, dir: &Path, endpoint: &str, pack: bool) -> Config {
    let mut c = ctx.config(dir);
    c.endpoint = endpoint.to_string();
    c.pack = pack;
    c
}

fn open_writer(cfg: Config) -> (buckwal::Database, buckwal::Connection) {
    let db = buckwal::open(cfg).expect("open writer");
    let conn = db.connect().expect("connect");
    (db, conn)
}

fn close_writer(db: buckwal::Database, conn: buckwal::Connection) {
    conn.close().expect("close connection");
    db.close().expect("close writer");
}

fn change_tenth(conn: &buckwal::Connection, k: u64) -> Duration {
    let t = Instant::now();
    conn.execute(
        &format!("UPDATE t SET b = randomblob({ROW_BYTES}) WHERE id % 10 = {k}"),
        &[],
    )
    .expect("10% change");
    t.elapsed()
}

fn small(conn: &buckwal::Connection, rng: &mut Lcg, rows: u64) -> Commit {
    let id = rng.next(rows) as i64 + 1;
    let start = Instant::now();
    conn.execute(
        &format!("UPDATE t SET b = randomblob({ROW_BYTES}) WHERE id = ?"),
        &[Value::from_i64(id)],
    )
    .expect("small commit");
    Commit {
        kind: Kind::Small,
        start,
        wait: start.elapsed(),
    }
}

fn int(conn: &buckwal::Connection, sql: &str) -> i64 {
    conn.query(sql, &[]).expect(sql)[0][0]
        .as_int()
        .unwrap_or_else(|| panic!("{sql}: no integer"))
}

fn pack_at(ctx: &Ctx, dir: &Path, endpoint: &str) -> PackReport {
    std::fs::create_dir_all(dir).unwrap();
    let cfg = cfg_at(ctx, dir, endpoint, false);
    buckwal::pack(&cfg).expect("pack run")
}

fn steps(r: &PackReport) -> String {
    r.step_times
        .iter()
        .map(|(s, d)| format!("{s}={}ms", d.as_millis()))
        .collect::<Vec<_>>()
        .join(",")
}

fn pack_desc(r: &PackReport) -> String {
    format!(
        "outcome={} pack_generation={} size={} bytes_down={} bytes_up={} source={} ms={} steps=[{}]",
        r.outcome.as_str(),
        r.pack_generation,
        r.pack_size,
        r.bytes_down,
        r.bytes_up,
        r.source.map_or("-", |s| s.as_str()),
        r.elapsed.as_millis(),
        steps(r)
    )
}

fn ms(d: Duration) -> String {
    format!("{:.0} ms", d.as_secs_f64() * 1000.0)
}

fn verdict(pass: bool) -> &'static str {
    if pass {
        "pass"
    } else {
        "miss"
    }
}

/// The verdict of a wait of `n` small requests against "about four": above 4.0 and up to
/// 4.0 + `ABOUT` is "about four", not a clean pass.
fn about_four(n: f64) -> &'static str {
    if n <= GOAL_REQUESTS {
        "pass"
    } else if n <= GOAL_REQUESTS + ABOUT {
        "about four"
    } else {
        "miss"
    }
}

/// The verdict of a commit that restarts the WAL: it waited `n` small requests, and its
/// request count is `count`. It passes at most `count` + `RESTART_MARGIN` small requests.
fn restart_wait(count: f64, n: f64) -> &'static str {
    verdict(n <= count + RESTART_MARGIN)
}

#[test]
fn restart_wait_bounds() {
    assert_eq!(restart_wait(OFF_RESTART_COUNT, 4.0), "pass");
    assert_eq!(restart_wait(OFF_RESTART_COUNT, 5.5), "pass");
    assert_eq!(restart_wait(OFF_RESTART_COUNT, 5.51), "miss");
    let bg = f64::from(BACKGROUND_UPLOADS + 3);
    assert_eq!(bg, 21.0);
    assert_eq!(restart_wait(bg, 22.5), "pass");
    assert_eq!(restart_wait(bg, 22.51), "miss");
}

fn env_u64(k: &str, default: u64) -> u64 {
    std::env::var(k)
        .ok()
        .map(|v| v.parse().unwrap_or_else(|_| panic!("{k} must be a number")))
        .unwrap_or(default)
}

/// The median time of 20 small PUTs (128 bytes) through `endpoint`.
fn small_put_median(ctx: &Ctx, endpoint: &str) -> Duration {
    let store = S3Store::new(S3Config {
        endpoint: endpoint.to_string(),
        region: ctx.var("BUCKWAL_REGION"),
        bucket: ctx.var("BUCKWAL_BUCKET"),
        access_key: ctx.var("BUCKWAL_ACCESS_KEY"),
        secret_key: ctx.var("BUCKWAL_SECRET_KEY"),
        path_style: ctx.var("BUCKWAL_PATH_STYLE") == "true",
        request_timeout: Duration::from_secs(30),
        ca_bundle: None,
    })
    .expect("S3 store");
    let mut times: Vec<Duration> = (0..20)
        .map(|i| {
            let t = Instant::now();
            store
                .put(&format!("{}/timed-probe/{i}", ctx.prefix()), &[7u8; 128])
                .expect("probe put");
            t.elapsed()
        })
        .collect();
    for i in 0..20 {
        let _ = store.delete(&format!("{}/timed-probe/{i}", ctx.prefix()));
    }
    times.sort();
    times[times.len() / 2]
}

/// The time intervals during which a maintenance run held calls back (its gate wait and its
/// checkpoint), from the `MaintenanceFinished` events. `elapsed` is the whole run and
/// includes `gate_wait`.
fn maintenance_intervals(events: &Events) -> Vec<(Instant, Instant)> {
    events
        .lock()
        .unwrap()
        .iter()
        .filter_map(|(t, e)| match e {
            Event::MaintenanceFinished { elapsed, .. } => Some((*t - *elapsed, *t)),
            _ => None,
        })
        .collect()
}

fn switch_times(events: &Events) -> Vec<(Instant, Duration)> {
    events
        .lock()
        .unwrap()
        .iter()
        .filter_map(|(t, e)| match e {
            Event::GenerationSwitched { elapsed, .. } => Some((*t, *elapsed)),
            _ => None,
        })
        .collect()
}

fn count_events(events: &Events, f: impl Fn(&Event) -> bool) -> usize {
    events.lock().unwrap().iter().filter(|(_, e)| f(e)).count()
}

fn blocks_file(dir: &Path) -> PathBuf {
    dir.join("app.db-blocks")
}

/// One commit that did a generation switch.
struct SwitchCommit {
    wait: Duration,
    /// Background commits behind (not yet in the bucket) just before it.
    behind: u64,
    /// Log records with commits that landed during its wait, before its switch.
    records_before: usize,
    /// The mean commits per record of those records.
    commits_per_record: Option<f64>,
    /// Background: the commits made to fill the queue before the checkpoint, and their time.
    fill_commits: usize,
    fill_time: Duration,
}

/// What `switch_in_commit` found.
struct SwitchTries {
    /// The commits that did the switch, in the tries that count.
    found: Vec<SwitchCommit>,
    /// Switches that came in the checkpoint call, not in the commit.
    in_checkpoint: usize,
    /// Background: tries that did not count, because the queue was below `FULL_QUEUE`
    /// commits behind just before the commit: (commits behind, the commit's wait).
    short: Vec<(u64, Duration)>,
}

/// Open the writer of `dir` (packer thread off) with `batching` (Off when `None`). Then try:
/// a few small commits (background: until `MAX_BEHIND` commits are behind, or 4000 commits,
/// or 10 s), a PASSIVE checkpoint, then one small commit. Off makes `IN_COMMIT_TRIES` tries.
/// Background counts a try only when at least `FULL_QUEUE` commits were behind just before
/// the commit, and tries until `IN_COMMIT_TRIES` tries count, at most `2 * IN_COMMIT_TRIES`
/// times.
fn switch_in_commit(
    ctx: &Ctx,
    dir: &Path,
    via: &str,
    rng: &mut Lcg,
    rows: u64,
    batching: Option<buckwal::Batching>,
) -> SwitchTries {
    let events: Events = Arc::default();
    let mut cfg = cfg_at(ctx, dir, via, false);
    if let Some(b) = batching {
        cfg.batching = b;
    }
    record(&mut cfg, &events);
    let (db, conn) = open_writer(cfg);
    let mut found = Vec::new();
    let mut in_checkpoint = 0;
    let mut short = Vec::new();
    let mut tries = 0;
    while found.len() < IN_COMMIT_TRIES
        && tries
            < if batching.is_some() {
                2 * IN_COMMIT_TRIES
            } else {
                IN_COMMIT_TRIES
            }
    {
        tries += 1;
        let fill = Instant::now();
        let mut fill_commits = 0;
        loop {
            small(&conn, rng, rows);
            fill_commits += 1;
            let enough = match batching {
                None => fill_commits >= 5,
                Some(_) => {
                    db.upload_stats().commits_behind >= u64::from(MAX_BEHIND)
                        || fill_commits >= 4000
                        || fill.elapsed() > Duration::from_secs(10)
                }
            };
            if enough {
                break;
            }
        }
        let fill_time = fill.elapsed();
        let t = Instant::now();
        conn.checkpoint(CheckpointMode::Passive).expect("passive");
        let t_end = Instant::now();
        in_checkpoint += switch_times(&events)
            .iter()
            .filter(|(s, _)| t <= *s && *s <= t_end)
            .count();
        let behind = db.upload_stats().commits_behind;
        let c = small(&conn, rng, rows);
        let end = c.start + c.wait;
        if batching.is_some() && behind < FULL_QUEUE {
            short.push((behind, c.wait));
            let _ = db.wait_durable(conn.last_commit().unwrap_or(0), Duration::from_secs(60));
            continue;
        }
        let switched = switch_times(&events)
            .into_iter()
            .find(|(s, _)| c.start <= *s && *s <= end);
        if let Some((at, _)) = switched {
            let before: Vec<usize> = events
                .lock()
                .unwrap()
                .iter()
                .filter_map(|(u, e)| match e {
                    Event::LogUploaded { commits, .. }
                        if !commits.is_empty() && c.start <= *u && *u <= at =>
                    {
                        Some(commits.len())
                    }
                    _ => None,
                })
                .collect();
            let commits_per_record = (!before.is_empty())
                .then(|| before.iter().sum::<usize>() as f64 / before.len() as f64);
            found.push(SwitchCommit {
                wait: c.wait,
                behind,
                records_before: before.len(),
                commits_per_record,
                fill_commits,
                fill_time,
            });
        }
        // Let the queue empty before the next try.
        let _ = db.wait_durable(conn.last_commit().unwrap_or(0), Duration::from_secs(60));
    }
    close_writer(db, conn);
    SwitchTries {
        found,
        in_checkpoint,
        short,
    }
}

#[test]
#[ignore = "timed: run alone on the solo RustFS container (file doc); not in ./scripts/test.sh"]
fn timed_goal_packer_stage2() {
    assert!(
        std::env::var("BUCKWAL_ENCRYPTION_KEY_FILE").is_err(),
        "the timed test runs plain HTTP without encryption"
    );
    let size_mb = env_u64("BUCKWAL_TIMED_MB", 1024);
    let cycles = env_u64("BUCKWAL_TIMED_CYCLES", 2);
    let rows = size_mb * ROWS_PER_MIB;
    let ctx = Ctx::new("timed_goal_packer_stage2");
    let direct = ctx.var("BUCKWAL_ENDPOINT");
    let upstream = direct
        .strip_prefix("http://")
        .expect("the timed test needs a plain HTTP endpoint")
        .trim_end_matches('/')
        .to_string();
    let proxy = DelayProxy::spawn(upstream);
    let via = proxy.endpoint();
    let mut out: Vec<String> = Vec::new();
    let mut goals: Vec<(String, String, String, &'static str)> = Vec::new();
    let mut note = |line: String| {
        eprintln!("timed: {line}");
        out.push(line);
    };
    note(format!(
        "size={size_mb} MiB rows={rows} row_bytes={ROW_BYTES} cycles={cycles} prefix={}",
        ctx.prefix()
    ));

    // ---- Phase A, no delay: fill the database, end the generation, make the first pack. ----
    proxy.set_delay(0);
    let dir_w = ctx.dir("writer");
    let pack_a = ctx.dir("pack-a");
    let t = Instant::now();
    {
        let (db, conn) = open_writer(cfg_at(&ctx, &dir_w, &via, false));
        conn.execute(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, b BLOB NOT NULL)",
            &[],
        )
        .expect("create");
        let mut next = 1u64;
        while next <= rows {
            let last = (next + FILL_BATCH - 1).min(rows);
            conn.execute("BEGIN", &[]).expect("begin");
            for id in next..=last {
                conn.execute(
                    &format!("INSERT INTO t(id, b) VALUES (?, randomblob({ROW_BYTES}))"),
                    &[Value::from_i64(id as i64)],
                )
                .expect("fill");
            }
            conn.execute("COMMIT", &[]).expect("commit");
            next = last + 1;
        }
        conn.checkpoint(CheckpointMode::Truncate).expect("truncate");
        close_writer(db, conn);
    }
    let file = std::fs::metadata(dir_w.join("app.db")).unwrap().len();
    note(format!(
        "fill: {} rows, app.db {file} bytes, {:.1} s (no delay)",
        rows,
        t.elapsed().as_secs_f64()
    ));
    let first = pack_at(&ctx, &pack_a, &via);
    assert_eq!(first.outcome, PackOutcome::Packed, "{}", pack_desc(&first));
    note(format!("first pack (no delay): {}", pack_desc(&first)));

    // ---- Goal 1: commits at 50 ms. ----
    proxy.set_delay(50);
    let r50 = small_put_median(&ctx, &via);
    note(format!(
        "small PUT median at 50 ms added: {} (one small request)",
        ms(r50)
    ));
    let events: Events = Arc::default();
    let mut commits: Vec<Commit> = Vec::new();
    let mut rng = Lcg(0x5eed);
    {
        let mut cfg = cfg_at(&ctx, &dir_w, &via, true);
        record(&mut cfg, &events);
        let t = Instant::now();
        let (db, conn) = open_writer(cfg);
        note(format!(
            "writer open at 50 ms (local copy, packer thread on): {}",
            ms(t.elapsed())
        ));
        for c in 0..cycles {
            for _ in 0..SMALL_BEFORE {
                commits.push(small(&conn, &mut rng, rows));
            }
            let start = Instant::now();
            let wait = change_tenth(&conn, c % 10);
            commits.push(Commit {
                kind: Kind::Tenth,
                start,
                wait,
            });
            let switches = switch_times(&events).len();
            let mut n = 0;
            let mut after = None;
            while n < SMALL_CAP {
                commits.push(small(&conn, &mut rng, rows));
                n += 1;
                let switched = switch_times(&events).len() > switches;
                let started =
                    count_events(&events, |e| matches!(e, Event::MaintenanceStarted { .. }));
                let finished = count_events(&events, |e| {
                    matches!(
                        e,
                        Event::MaintenanceFinished { .. } | Event::MaintenanceSkipped { .. }
                    )
                });
                if switched && started <= finished {
                    after = Some(n);
                    break;
                }
            }
            if after.is_none() {
                // No WAL restart came by itself: restart it with a TRUNCATE checkpoint.
                let start = Instant::now();
                conn.checkpoint(CheckpointMode::Truncate).expect("truncate");
                commits.push(Commit {
                    kind: Kind::Checkpoint,
                    start,
                    wait: start.elapsed(),
                });
            }
            for _ in 0..SMALL_AFTER {
                commits.push(small(&conn, &mut rng, rows));
            }
            note(format!(
                "cycle {c}: 10% change {} ; switch after {} small commits ({})",
                ms(wait),
                n,
                if after.is_some() {
                    "came by itself"
                } else {
                    "explicit TRUNCATE checkpoint"
                }
            ));
        }
        close_writer(db, conn);
    }
    let held = maintenance_intervals(&events);
    let switches = switch_times(&events);
    let overlaps = |c: &Commit, a: Instant, b: Instant| c.start <= b && a <= c.start + c.wait;
    let in_maintenance = |c: &Commit| held.iter().any(|(a, b)| overlaps(c, *a, *b));
    let did_switch = |c: &Commit| switches.iter().any(|(t, _)| overlaps(c, *t, *t));
    let longest = |f: &dyn Fn(&Commit) -> bool| {
        commits
            .iter()
            .filter(|c| f(c))
            .map(|c| c.wait)
            .max()
            .unwrap_or_default()
    };
    let small_all = longest(&|c| c.kind == Kind::Small);
    let small_free = longest(&|c| c.kind == Kind::Small && !in_maintenance(c));
    let small_held = longest(&|c| c.kind == Kind::Small && in_maintenance(c));
    let switch_commit = longest(&|c| c.kind == Kind::Small && did_switch(c) && !in_maintenance(c));
    let tenth = longest(&|c| c.kind == Kind::Tenth);
    let checkpoint = longest(&|c| c.kind == Kind::Checkpoint);
    let mut small_sorted: Vec<Duration> = commits
        .iter()
        .filter(|c| c.kind == Kind::Small)
        .map(|c| c.wait)
        .collect();
    small_sorted.sort();
    let median = small_sorted[small_sorted.len() / 2];
    let switch_wait = switches.iter().map(|(_, e)| *e).max().unwrap_or_default();
    let in_requests = |d: Duration| d.as_secs_f64() / r50.as_secs_f64();
    note(format!(
        "commits at 50 ms: {} small (median {}), {} 10% changes, {} explicit checkpoints; \
         {} generation switches (longest extra wait {}), {} maintenance runs, {} skipped",
        small_sorted.len(),
        ms(median),
        commits.iter().filter(|c| c.kind == Kind::Tenth).count(),
        commits
            .iter()
            .filter(|c| c.kind == Kind::Checkpoint)
            .count(),
        switches.len(),
        ms(switch_wait),
        held.len(),
        count_events(&events, |e| matches!(e, Event::MaintenanceSkipped { .. })),
    ));
    note(format!(
        "longest small commit outside a maintenance run: {} ({:.2} small requests)",
        ms(small_free),
        in_requests(small_free)
    ));
    note(format!(
        "longest small commit that switched the generation: {} ({:.2} small requests)",
        ms(switch_commit),
        in_requests(switch_commit)
    ));
    note(format!(
        "longest small commit held by the size-limit checkpoint: {} ; longest small commit of \
         all: {} ; longest 10% change commit (its own log upload of about {} MiB): {} ; \
         longest explicit TRUNCATE checkpoint: {}",
        ms(small_held),
        ms(small_all),
        size_mb / 10,
        ms(tenth),
        ms(checkpoint)
    ));
    let packed_b = count_events(&events, |e| matches!(e, Event::Packed { .. }));
    note(format!(
        "packer thread pack runs during the 50 ms commits: {packed_b}"
    ));
    let limit = GOAL_REQUESTS + ABOUT;
    goals.push((
        format!(
            "1. longest commit at 50 ms, {size_mb} MiB, 10% changed (size-limit checkpoint and \
             the 10% change commit itself apart)"
        ),
        format!("about 4 small requests (at most {limit:.1} x {})", ms(r50)),
        format!(
            "{} = {:.2} small requests",
            ms(small_free),
            in_requests(small_free)
        ),
        about_four(in_requests(small_free)),
    ));

    // ---- Goal 1b: a generation switch inside an app commit, batching Off, at 50 ms. ----
    // A PASSIVE checkpoint with no reader open backfills the whole WAL, so the next commit
    // restarts the WAL: that commit does the switch (seal-start record, head move, WAL
    // header record) and its own log record.
    let off = switch_in_commit(&ctx, &dir_w, &via, &mut rng, rows, None);
    let (waits, in_checkpoint) = (off.found, off.in_checkpoint);
    let r50b = small_put_median(&ctx, &via);
    note(format!(
        "in-commit switch, Off, 50 ms: {} tries, {} commits did the switch, {} switches came \
         in the checkpoint call; their waits: [{}] (one small PUT now {})",
        IN_COMMIT_TRIES,
        waits.len(),
        in_checkpoint,
        waits
            .iter()
            .map(|s| format!("{} = {:.2} small requests", ms(s.wait), in_requests(s.wait)))
            .collect::<Vec<_>>()
            .join(", "),
        ms(r50b)
    ));
    let worst = waits.iter().map(|s| s.wait).max();
    goals.push((
        "1b. commit that restarts the WAL at 50 ms, batching Off: own log record + three small \
         requests"
            .into(),
        format!(
            "count {OFF_RESTART_COUNT:.0} + at most {RESTART_MARGIN} = at most {:.1} small \
             requests (one = {})",
            OFF_RESTART_COUNT + RESTART_MARGIN,
            ms(r50)
        ),
        match worst {
            Some(w) => format!("{} = {:.2} small requests", ms(w), in_requests(w)),
            None => "not measured: no commit did the switch".into(),
        },
        worst.map_or("not measured", |w| {
            restart_wait(OFF_RESTART_COUNT, in_requests(w))
        }),
    ));

    // ---- The background case: `background 5ms 64 1MiB 1000`, at 50 ms. ----
    let batching: buckwal::Batching = BACKGROUND.parse().expect("background setting");
    let tries = switch_in_commit(&ctx, &dir_w, &via, &mut rng, rows, Some(batching));
    let bg = tries.found;
    note(format!(
        "background tries that did not count (fewer than {FULL_QUEUE} commits behind): {} {}",
        tries.short.len(),
        tries
            .short
            .iter()
            .map(|(b, w)| format!("[{b} behind, wait {}]", ms(*w)))
            .collect::<Vec<_>>()
            .join(" ")
    ));
    for (i, s) in bg.iter().enumerate() {
        note(format!(
            "background switch commit {i}: wait {} ; {} commits behind before it ; {} log \
             records landed during its wait before the switch ; {} commits per record \
             (mean of the records before it) ; filled the queue with {} commits in {}",
            ms(s.wait),
            s.behind,
            s.records_before,
            s.commits_per_record
                .map_or("-".to_string(), |c| format!("{c:.1}")),
            s.fill_commits,
            ms(s.fill_time)
        ));
    }
    // The spec's count: the records queued before it (at most max_behind commits; about 18
    // uploads at this setting), then three small requests. The goal adds `RESTART_MARGIN`.
    let bg_count = f64::from(BACKGROUND_UPLOADS + 3);
    let bound = r50.mul_f64(bg_count + RESTART_MARGIN);
    let worst_bg = bg.iter().max_by_key(|s| s.wait);
    goals.push((
        format!("1c. background commit that restarts the WAL at 50 ms, `{BACKGROUND}`"),
        format!(
            "records queued before it (at most {MAX_BEHIND} commits, {BACKGROUND_UPLOADS} \
             uploads), then three small requests: count {bg_count:.0} + at most \
             {RESTART_MARGIN} = at most {:.1} small requests ({})",
            bg_count + RESTART_MARGIN,
            ms(bound)
        ),
        match worst_bg {
            Some(s) => format!(
                "{} = {:.2} small requests; {} commits behind, {} records before the switch",
                ms(s.wait),
                in_requests(s.wait),
                s.behind,
                s.records_before
            ),
            None => format!(
                "not measured: no background commit with at least {FULL_QUEUE} commits behind \
                 did the switch"
            ),
        },
        worst_bg.map_or("not measured", |s| {
            let within = restart_wait(bg_count, in_requests(s.wait)) == "pass";
            verdict(bg.len() == IN_COMMIT_TRIES && within && s.behind <= u64::from(MAX_BEHIND))
        }),
    ));

    // ---- Goal 3: a pack run at 20 ms, 10% changed. ----
    // Prepare with no delay: pack what the commits left, then change 10% and end the
    // generation.
    proxy.set_delay(0);
    let prep = |k: u64, dir: &Path| {
        let (db, conn) = open_writer(cfg_at(&ctx, dir, &via, false));
        change_tenth(&conn, k);
        conn.checkpoint(CheckpointMode::Truncate).expect("truncate");
        close_writer(db, conn);
    };
    let absorb = pack_at(&ctx, &pack_a, &via);
    note(format!(
        "pack of the 50 ms commits (no delay): {}",
        pack_desc(&absorb)
    ));
    prep(5, &dir_w);
    proxy.set_delay(20);
    note(format!(
        "small PUT median at 20 ms added: {}",
        ms(small_put_median(&ctx, &via))
    ));
    let kept = pack_at(&ctx, &pack_a, &via);
    assert_eq!(kept.outcome, PackOutcome::Packed, "{}", pack_desc(&kept));
    note(format!(
        "pack run at 20 ms, kept copy: {}",
        pack_desc(&kept)
    ));
    proxy.set_delay(0);
    prep(6, &dir_w);
    proxy.set_delay(20);
    let fresh = pack_at(&ctx, &ctx.dir("pack-c"), &via);
    assert_eq!(fresh.outcome, PackOutcome::Packed, "{}", pack_desc(&fresh));
    note(format!(
        "pack run at 20 ms, new pack directory (downloads the pack): {}",
        pack_desc(&fresh)
    ));
    goals.push((
        format!(
            "3a. pack run at 20 ms, {size_mb} MiB, 10% changed, kept copy (the packer thread's \
             usual run)"
        ),
        format!("within {} s", GOAL_PACK.as_secs()),
        format!("{:.1} s", kept.elapsed.as_secs_f64()),
        verdict(kept.elapsed <= GOAL_PACK),
    ));
    goals.push((
        format!("3b. pack run at 20 ms, {size_mb} MiB, 10% changed, new pack directory"),
        format!("within {} s", GOAL_PACK.as_secs()),
        format!("{:.1} s", fresh.elapsed.as_secs_f64()),
        verdict(fresh.elapsed <= GOAL_PACK),
    ));

    // ---- Goal 2: restore at 20 ms, a pack and no log after it. ----
    let head = ctx.head();
    note(format!(
        "head generation {} ; newest pack generation {} (the log after the pack: generation {} \
         only)",
        head.generation, fresh.pack_generation, head.generation
    ));
    let dir_r = ctx.dir("restored");
    let start = Instant::now();
    let restored = buckwal::restore(&cfg_at(&ctx, &dir_r, &via, false), &dir_r).expect("restore");
    let t_restore = start.elapsed();
    let mut cfg = cfg_at(&ctx, &dir_r, &via, false);
    let ev_r: Events = Arc::default();
    record(&mut cfg, &ev_r);
    let (db, conn) = open_writer(cfg);
    let t_open = start.elapsed();
    let blocks_at_open = blocks_file(&dir_r).exists();
    let local = wait_until(
        "the restored copy fully local",
        Duration::from_secs(600),
        || (!blocks_file(&dir_r).exists()).then(Instant::now),
    );
    let t_local = local - start;
    let count = int(&conn, "SELECT count(*) FROM t");
    assert_eq!(count as u64, rows, "rows after the restore");
    note(format!(
        "restore at 20 ms: restore returned {} (generation={} log_objects={} gets={} bytes={} \
         bytes_fetched={}), writer open returned {} (blocks still remote at open: \
         {blocks_at_open}), fully local {} ; {count} rows",
        ms(t_restore),
        restored.generation,
        restored.log_objects,
        restored.gets,
        restored.bytes,
        restored.bytes_fetched,
        ms(t_open),
        ms(t_local)
    ));
    close_writer(db, conn);
    goals.push((
        format!("2. restore at 20 ms of a {size_mb} MiB pack with no log after it, fully local"),
        format!(
            "within {} s, open returns before all blocks are local",
            GOAL_RESTORE.as_secs()
        ),
        format!(
            "{:.1} s to fully local, open returned at {:.1} s with blocks remote: {blocks_at_open}",
            t_local.as_secs_f64(),
            t_open.as_secs_f64()
        ),
        verdict(t_local <= GOAL_RESTORE && blocks_at_open),
    ));

    // ---- Goal 4: two packers at once at 20 ms, then GC. ----
    proxy.set_delay(0);
    prep(7, &dir_r);
    proxy.set_delay(20);
    let pack_b = ctx.dir("pack-b");
    let barrier = Barrier::new(2);
    let run = |dir: &Path| {
        barrier.wait();
        pack_at(&ctx, dir, &via)
    };
    let (one, two) = std::thread::scope(|s| {
        let (run, pa, pb) = (&run, &pack_a, &pack_b);
        let a = s.spawn(move || run(pa));
        let b = s.spawn(move || run(pb));
        (a.join().unwrap(), b.join().unwrap())
    });
    note(format!("two packers, A: {}", pack_desc(&one)));
    note(format!("two packers, B: {}", pack_desc(&two)));
    let winners: Vec<&PackReport> = [&one, &two]
        .into_iter()
        .filter(|r| r.outcome == PackOutcome::Packed)
        .collect();
    let others_ok = [&one, &two].iter().all(|r| {
        matches!(
            r.outcome,
            PackOutcome::Packed | PackOutcome::Superseded | PackOutcome::NothingToDo
        )
    });
    // GC with a short trim grace and pack keep time, so the older packs go within a minute.
    let mut gc_cfg = cfg_at(&ctx, &ctx.dir("gc"), &via, false);
    gc_cfg.lease = Duration::from_secs(1);
    gc_cfg.retry_deadline = Duration::from_secs(1);
    gc_cfg.request_timeout = Duration::from_secs(2);
    gc_cfg.trim_grace = Duration::from_secs(5);
    gc_cfg.pack_keep = Some(Duration::from_secs(5));
    let newest = winners.first().and_then(|r| r.pack_key.clone());
    let pack_root = format!("{}/pack/", ctx.prefix());
    let mut gc_lines = Vec::new();
    let gc_start = Instant::now();
    let left = loop {
        let r = buckwal::gc(&gc_cfg).expect("gc");
        gc_lines.push(format!(
            "deleted={} packs_deleted={} orphan_packs_deleted={} uploads_aborted={} outcome={}",
            r.deleted,
            r.packs_deleted,
            r.orphan_packs_deleted,
            r.uploads_aborted,
            r.outcome.as_str()
        ));
        let packs = ctx.store().list(&pack_root).expect("list packs");
        let only_newest = newest.as_ref().is_some_and(|k| {
            let mut want = vec![k.clone(), format!("{k}.codes")];
            want.sort();
            let mut got = packs.clone();
            got.sort();
            got == want
        });
        if only_newest || gc_start.elapsed() > Duration::from_secs(120) {
            break packs;
        }
        std::thread::sleep(Duration::from_secs(3));
    };
    let all = ctx
        .store()
        .list(&format!("{}/", ctx.prefix()))
        .expect("list");
    let kinds = |p: &str| {
        all.iter()
            .filter(|k| k[ctx.prefix().len() + 1..].starts_with(p))
            .count()
    };
    let (n_log, n_pack, n_pages, n_manifest) = (
        kinds("log/"),
        kinds("pack/"),
        kinds("pages/"),
        kinds("manifest/"),
    );
    let n_other = all.len() - n_log - n_pack - n_pages - n_manifest;
    note(format!(
        "GC after the two packers: {} passes in {:.0} s, last: {} ; pack/ holds {:?}",
        gc_lines.len(),
        gc_start.elapsed().as_secs_f64(),
        gc_lines.last().unwrap(),
        left.iter()
            .map(|k| &k[ctx.prefix().len() + 1..])
            .collect::<Vec<_>>()
    ));
    note(format!(
        "bucket after GC: {} objects: log {n_log}, pack {n_pack}, pages {n_pages}, manifest \
         {n_manifest}, other {n_other} (head, pack-latest)",
        all.len()
    ));
    let mut want = newest
        .iter()
        .flat_map(|k| [k.clone(), format!("{k}.codes")])
        .collect::<Vec<_>>();
    want.sort();
    let mut got = left.clone();
    got.sort();
    let one_pack = winners.len() == 1 && others_ok && got == want && n_pages + n_manifest == 0;
    goals.push((
        "4. two packers at once at 20 ms, then GC".into(),
        "one newest pack and no extra objects after GC".into(),
        format!(
            "outcomes {} and {}; pack/ after GC holds {} objects ({})",
            one.outcome.as_str(),
            two.outcome.as_str(),
            left.len(),
            if got == want {
                "the newest pack and its codes"
            } else {
                "not only the newest pack"
            }
        ),
        verdict(one_pack),
    ));
    note(format!("proxy requests: {}", proxy.requests.load(SeqCst)));

    for (name, goal, measured, result) in &goals {
        note(format!("goal | {name} | {goal} | {measured} | {}", result));
    }
    if let Ok(path) = std::env::var("BUCKWAL_TIMED_RESULTS") {
        std::fs::write(&path, out.join("\n") + "\n").expect("write the results");
    }
    // A goal that was not measured (no commit did the switch) fails the test too.
    let missed: Vec<&String> = goals
        .iter()
        .filter(|g| g.3 == "miss" || g.3 == "not measured")
        .map(|g| &g.0)
        .collect();
    assert!(missed.is_empty(), "goals missed: {missed:?}");
}
