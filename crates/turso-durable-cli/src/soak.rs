//! `soak`: commit one 1 KB row at a time, printing `ack <id>` after each durable commit.
//! With `--connections N`, N threads each commit on their own connection; thread `i` commits
//! ids `start + i + k·N`.
use crate::args::{self, Checkpoint, Concurrent, SoakArgs};
use crate::{err, ms, say, say_all, stats_lines, store, CliResult};
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, SystemTime};
use turso_durable::{Batching, CheckpointMode, Config, Connection, Database, Event, Value};

/// `error <message>` on one line.
fn say_error(e: &dyn std::fmt::Display) {
    let msg = e.to_string().replace(['\n', '\r'], " ");
    say(&format!("error {msg}"));
}

/// A lock even after a thread panicked holding it.
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// `event log-uploaded` lists its commits as `1,2,3`, or `-` for none.
fn commit_list(commits: &[u64]) -> String {
    if commits.is_empty() {
        return "-".into();
    }
    let list: Vec<String> = commits.iter().map(u64::to_string).collect();
    list.join(",")
}

/// The `event maintenance-…` line for a maintenance event (`None` for any other event).
fn maintenance_line(e: &Event) -> Option<String> {
    Some(match e {
        Event::MaintenanceStarted {
            log_objects,
            wal_bytes,
            ..
        } => format!("event maintenance-started log_objects={log_objects} wal_bytes={wal_bytes}"),
        Event::MaintenanceFinished {
            gate_wait, elapsed, ..
        } => format!(
            "event maintenance-finished gate_wait_ms={} ms={}",
            ms(*gate_wait),
            ms(*elapsed)
        ),
        Event::MaintenanceSkipped { reason, .. } => format!(
            "event maintenance-skipped {}",
            reason.replace(['\n', '\r'], " ")
        ),
        _ => return None,
    })
}

/// The open database and one connection per thread.
struct Pool {
    db: Database,
    conns: Vec<Mutex<Connection>>,
}

/// What the threads share.
struct Soak<'a> {
    args: &'a SoakArgs,
    /// Commits take it shared; a checkpoint or reopen takes it alone, between commits.
    pool: RwLock<Option<Pool>>,
    /// Held by a checkpoint while it waits for the pool, so new commits queue behind it.
    turnstile: Mutex<()>,
    /// Acks so far, over every thread.
    acked: AtomicU64,
    /// The setting this soak chose (`--batching`, then the file), set after every open.
    /// Never held while waiting for the pool.
    batching: Mutex<Chosen>,
    seen: Mutex<Seen>,
}

/// What the batching file looked like when last read.
#[derive(Default)]
struct Seen {
    /// Its (mtime, length), `None` when it could not be read; `None` before the first read.
    stamp: Option<Option<(SystemTime, u64)>>,
    /// How many valid changes were read from it.
    changes: u64,
}

/// The setting in force, and the number of the file change it came from (0: `--batching`).
struct Chosen {
    change: u64,
    batching: Option<Batching>,
}

impl Soak<'_> {
    fn read(&self) -> RwLockReadGuard<'_, Option<Pool>> {
        let turnstile = lock(&self.turnstile);
        let pool = self.pool.read().unwrap_or_else(|e| e.into_inner());
        drop(turnstile);
        pool
    }

    fn write(&self) -> RwLockWriteGuard<'_, Option<Pool>> {
        let _turnstile = lock(&self.turnstile);
        self.pool.write().unwrap_or_else(|e| e.into_inner())
    }

    fn open_db(&self) -> CliResult<Database> {
        let mut cfg: Config = crate::writer_config(&self.args.dir)?;
        let events = self.args.batching.is_some() || self.args.batching_file.is_some();
        cfg.on_event = Some(Arc::new(move |e: &Event| match e {
            Event::SealStarted { .. } => say("event seal-started"),
            Event::SealFinished { .. } => say("event seal-finished"),
            Event::BatchWaiting { commits } if events => {
                say(&format!("event batch-waiting {commits}"))
            }
            Event::LogUploaded { commits, .. } if events => {
                say(&format!("event log-uploaded {}", commit_list(commits)))
            }
            e if events => {
                if let Some(line) = maintenance_line(e) {
                    say(&line);
                }
            }
            _ => {}
        }));
        let db = match self.args.page_put_delay_ms {
            Some(ms) => {
                let s = store(&cfg, Some(ms))?;
                turso_durable::open_with_store(cfg, s).map_err(err)?
            }
            None => turso_durable::open(cfg).map_err(err)?,
        };
        if let Some(b) = lock(&self.batching).batching {
            db.set_batching(b).map_err(err)?;
        }
        Ok(db)
    }

    /// Open the database and its connections, retrying until it works.
    fn open_all(&self) -> Pool {
        loop {
            let attempt = self.open_db().and_then(|db| {
                let conns = (0..self.args.connections)
                    .map(|_| db.connect().map(Mutex::new).map_err(err))
                    .collect::<CliResult<Vec<_>>>()?;
                lock(&conns[0])
                    .execute(
                        "CREATE TABLE IF NOT EXISTS soak(id INTEGER PRIMARY KEY, payload BLOB)",
                        &[],
                    )
                    .map_err(err)?;
                Ok(Pool { db, conns })
            });
            match attempt {
                Ok(x) => return x,
                Err(m) => {
                    say_error(&m);
                    std::thread::sleep(Duration::from_millis(500));
                }
            }
        }
    }

    /// When the batching file changed since it was last read, read it and set what it says
    /// (printing `batching <setting>`). Unreadable or bad content prints one `error: …` on
    /// stderr and keeps the setting. One thread handles each change; a change read before a
    /// newer one already set is dropped.
    fn check_batching_file(&self) {
        let Some(path) = &self.args.batching_file else {
            return;
        };
        let stamp = std::fs::metadata(path)
            .and_then(|m| Ok((m.modified()?, m.len())))
            .ok();
        let name = path.display().to_string();
        let (b, change) = {
            let mut seen = lock(&self.seen);
            if seen.stamp == Some(stamp) {
                return;
            }
            seen.stamp = Some(stamp);
            let parsed = std::fs::read_to_string(path)
                .map_err(|e| format!("{name}: {e}"))
                .and_then(|text| args::batching(&name, text.trim()));
            match parsed {
                Ok(b) => {
                    seen.changes += 1;
                    (b, seen.changes)
                }
                Err(m) => {
                    eprintln!("error: {m}");
                    return;
                }
            }
        };
        // With the pool held shared no reopen runs, so `batching` can be held across the set:
        // changes are set in the order they were read, and stored only once set.
        let pool = self.read();
        let mut chosen = lock(&self.batching);
        if chosen.change > change {
            return;
        }
        if let Err(e) = pool.as_ref().expect("open").db.set_batching(b) {
            eprintln!("error: {name}: {e}");
            return;
        }
        *chosen = Chosen {
            change,
            batching: Some(b),
        };
        say(&format!("batching {b}"));
    }

    /// The upload stats, taken and printed with no commit running, so every commit a block
    /// counts as uploaded is also counted as acked.
    fn print_stats(&self) {
        let pool = self.write();
        let stats = pool.as_ref().expect("open").db.upload_stats();
        say_all("stats ", &stats_lines(&stats));
    }

    /// Checkpoint on connection `i`, or close everything and reopen, with no commit running.
    fn checkpoint(&self, i: usize, cp: Checkpoint) {
        self.checkpoint_inner(i, cp);
        say(&format!("event checkpoint {}", name(cp)));
    }

    fn checkpoint_inner(&self, i: usize, cp: Checkpoint) {
        let mut pool = self.write();
        match mode(cp) {
            Some(m) => {
                if let Err(e) = lock(&pool.as_ref().expect("open").conns[i]).checkpoint(m) {
                    say_error(&e);
                }
            }
            None => {
                let old = pool.take().expect("open");
                for c in old.conns {
                    let c = c.into_inner().unwrap_or_else(|e| e.into_inner());
                    if let Err(e) = c.close() {
                        say_error(&e);
                    }
                }
                if let Err(e) = old.db.close() {
                    say_error(&e);
                }
                *pool = Some(self.open_all());
            }
        }
    }

    /// Thread `i`'s commits, retrying each until it is acked.
    fn worker(&self, i: usize) -> CliResult<()> {
        let n = self.args.connections as u64;
        let mut k = 0u64;
        loop {
            let index = i as u64 + k * n;
            if self.args.count.is_some_and(|c| index >= c) {
                return Ok(());
            }
            let id = self.args.start + index as i64;
            self.check_batching_file();
            let payload = vec![(id & 0xff) as u8; 1024];
            let blob = Value::from_slice(&payload).map_err(err)?;
            let r = {
                let pool = self.read();
                let conn = lock(&pool.as_ref().expect("open").conns[i]);
                write(&conn, begin_for(self.args.concurrent, i), id, blob)
            };
            if let Err(e) = r {
                say_error(&e);
                std::thread::sleep(Duration::from_millis(500));
                continue;
            }
            say(&format!("ack {id}"));
            k += 1;
            let acked = self.acked.fetch_add(1, SeqCst) + 1;
            if self
                .args
                .stats_every
                .is_some_and(|e| acked.is_multiple_of(e))
            {
                self.print_stats();
            }
            let Some(cp) = self.args.checkpoint else {
                continue;
            };
            if acked.is_multiple_of(self.args.every) {
                self.checkpoint(i, cp);
            }
        }
    }
}

/// The statement that opens thread `i`'s write transaction; `None` for an autocommit write.
fn begin_for(concurrent: Concurrent, i: usize) -> Option<&'static str> {
    match concurrent {
        Concurrent::Off => None,
        Concurrent::All => Some("BEGIN CONCURRENT"),
        Concurrent::Mixed if i.is_multiple_of(2) => Some("BEGIN"),
        Concurrent::Mixed => Some("BEGIN CONCURRENT"),
    }
}

/// Write row `id`: one autocommit statement, or inside `begin` ... `COMMIT`. `Ok` only once
/// the commit returned `Ok`; any failure rolls the transaction back (a rollback that cannot
/// run is moot) and is returned, to be retried by the caller.
fn write(
    conn: &Connection,
    begin: Option<&str>,
    id: i64,
    blob: Value,
) -> turso_durable::Result<u64> {
    const INSERT: &str = "INSERT OR REPLACE INTO soak(id, payload) VALUES (?, ?)";
    let params = [Value::from_i64(id), blob];
    let Some(begin) = begin else {
        return conn.execute(INSERT, &params);
    };
    conn.execute(begin, &[])?;
    let r = conn
        .execute(INSERT, &params)
        .and_then(|n| conn.execute("COMMIT", &[]).map(|_| n));
    if r.is_err() {
        let _ = conn.execute("ROLLBACK", &[]);
    }
    r
}

fn name(c: Checkpoint) -> &'static str {
    match c {
        Checkpoint::Passive => "passive",
        Checkpoint::Full => "full",
        Checkpoint::Restart => "restart",
        Checkpoint::Truncate => "truncate",
        Checkpoint::Reopen => "reopen",
    }
}

fn mode(c: Checkpoint) -> Option<CheckpointMode> {
    match c {
        Checkpoint::Passive => Some(CheckpointMode::Passive),
        Checkpoint::Full => Some(CheckpointMode::Full),
        Checkpoint::Restart => Some(CheckpointMode::Restart),
        Checkpoint::Truncate => Some(CheckpointMode::Truncate),
        Checkpoint::Reopen => None,
    }
}

pub fn run(args: &SoakArgs) -> CliResult<()> {
    let soak = Soak {
        args,
        pool: RwLock::new(None),
        turnstile: Mutex::new(()),
        acked: AtomicU64::new(0),
        batching: Mutex::new(Chosen {
            change: 0,
            batching: args.batching,
        }),
        seen: Mutex::new(Seen::default()),
    };
    *soak.write() = Some(soak.open_all());
    let shared = &soak;
    let results: Vec<CliResult<()>> = std::thread::scope(|s| {
        let threads: Vec<_> = (0..args.connections)
            .map(|i| s.spawn(move || shared.worker(i)))
            .collect();
        let joined = threads.into_iter().map(|t| t.join());
        joined
            .map(|r| r.unwrap_or_else(|_| Err("a soak thread panicked".into())))
            .collect()
    });
    results.into_iter().collect::<CliResult<()>>()?;
    let pool = soak.write().take().expect("open");
    for c in pool.conns {
        c.into_inner()
            .unwrap_or_else(|e| e.into_inner())
            .close()
            .map_err(err)?;
    }
    pool.db.close().map_err(err)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn begin_statement_per_thread() {
        assert_eq!(begin_for(Concurrent::Off, 3), None);
        assert_eq!(begin_for(Concurrent::All, 0), Some("BEGIN CONCURRENT"));
        assert_eq!(begin_for(Concurrent::All, 7), Some("BEGIN CONCURRENT"));
        assert_eq!(begin_for(Concurrent::Mixed, 0), Some("BEGIN"));
        assert_eq!(begin_for(Concurrent::Mixed, 1), Some("BEGIN CONCURRENT"));
        assert_eq!(begin_for(Concurrent::Mixed, 2), Some("BEGIN"));
    }

    #[test]
    fn maintenance_events_print_one_line_each() {
        let g = 3;
        let started = Event::MaintenanceStarted {
            generation: g,
            log_objects: 1000,
            wal_bytes: 4096,
        };
        let finished = Event::MaintenanceFinished {
            generation: g,
            gate_wait: Duration::from_micros(12_345),
            elapsed: Duration::from_micros(250_500),
        };
        let skipped = |reason: &str| Event::MaintenanceSkipped {
            generation: g,
            reason: reason.into(),
        };
        let lines: Vec<Option<String>> = [
            started,
            finished,
            skipped("gate_timeout"),
            skipped("error: two\nlines"),
            Event::BatchWaiting { commits: 1 },
        ]
        .iter()
        .map(maintenance_line)
        .collect();
        assert_eq!(
            lines,
            vec![
                Some("event maintenance-started log_objects=1000 wal_bytes=4096".into()),
                Some("event maintenance-finished gate_wait_ms=12.345 ms=250.500".into()),
                Some("event maintenance-skipped gate_timeout".into()),
                Some("event maintenance-skipped error: two lines".into()),
                None,
            ]
        );
    }
}
