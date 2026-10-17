# turso-durable

Make a tursodb database durable in an S3 bucket. A Rust library (`turso_durable`) and a CLI (`turso-durable`).

Your app keeps a normal local database directory. Every commit is uploaded to the bucket before it returns, so the commit survives the loss of the machine. A new machine can restore the database from the bucket alone.

This is stage 3: one writer process at a time, with manual takeover (a fence record and an epoch in the head), lease self-fencing and garbage collection on the bucket's clock. It keeps stage 2's runtime batching setting and OpenTelemetry export, and speaks HTTPS or plain HTTP. It is tested against RustFS on a laptop (not on AWS S3).

## How it works

- **Commits.** Each commit writes WAL frames. With batching `Off` (the default), the library uploads the bytes written since the last fsync as one log object (`log/<generation>/<seq>`) before the WAL's fsync. With a `Window`, the commit is queued for the uploader, fsynced locally, and the call then waits while the commit shares one log object with other commits (see Batching). Either way, the commit returns `Ok` only when a log object holding it is in the bucket.
- **Generations.** When the engine restarts its WAL after a checkpoint, the old WAL becomes a finished *generation*. The library then *seals* it. It first appends an empty *seal-start* log record and checks that `head` has not moved, so a writer that was taken over stops before it uploads anything. Then it uploads every page that changed in that generation (`pages/…`), then a manifest that maps each page to the generation holding its latest copy, then moves `head` to the next generation.
- **Restore.** Restore reads `head`, the newest manifest and the current generation's log. It rebuilds the WAL from the log and fetches pages from the bucket only when the engine first reads them.
- **Takeover.** Another machine takes the database over by restoring it and writing the head with a new writer id at the next epoch, then a fence record in the log (`takeover`, see Guarantees). The old writer, if it still runs, is fenced: its next append, seal or lease check fails `Fenced`.
- **The lease.** A writer trusts its last look at the head for `lease` (30 s). Past it, the next append, and each page or manifest PUT of a seal, reads the head first.
- **GC.** Log objects, manifests and pages that the head no longer needs are deleted once the bucket's own clock says they have been unreferenced for longer than the trim grace (10 min). A writer runs GC on a background thread at open and after each seal; `gc` runs it from anywhere.

## Use

The library is not on crates.io. Add it by path or git:

```toml
[dependencies]
turso_durable = { path = "…/crates/turso_durable" }
```

```rust
use std::time::Duration;
use turso_durable::telemetry::{self, TelemetryConfig};
use turso_durable::{Batching, CheckpointMode, Config, Value};

fn main() -> turso_durable::Result<()> {
    // Optional: export spans and logs over OTLP when DURABLE_OTEL=1 (see Telemetry).
    // Keep the guard: dropping it flushes the export.
    let _telemetry = telemetry::init(TelemetryConfig::from_env()?)?;

    // Endpoint, bucket and keys from DURABLE_* (see Settings), or build one with Config::new.
    let config = Config::from_env("/var/lib/myapp/db")?;
    let db = turso_durable::open(config)?;
    let conn = db.connect()?; // one Connection per thread

    conn.execute(
        "CREATE TABLE IF NOT EXISTS notes(id INTEGER PRIMARY KEY, body TEXT)",
        &[],
    )?;
    // Returns once the commit is in the bucket.
    conn.execute("INSERT INTO notes(body) VALUES (?)", &[Value::from_text("hello")])?;

    // Several statements, one commit.
    conn.transaction(|tx| {
        tx.execute("INSERT INTO notes(body) VALUES (?)", &[Value::from_text("a")])?;
        tx.execute("INSERT INTO notes(body) VALUES (?)", &[Value::from_text("b")])?;
        Ok(())
    })?;

    // Batching (see Batching). Off unless DURABLE_BATCHING or set_batching says otherwise.
    let window = Batching::Window {
        max_wait: Duration::from_millis(10),
        max_commits: 64,
        max_bytes: 1 << 20,
    };
    db.set_batching(window)?; // every connection without its own setting, from its next commit
    conn.set_batching(Some(Batching::Off))?; // this connection only; None = the database's

    // One transaction with its own setting, whatever the connection's is.
    conn.transaction_with(window, |tx| {
        tx.execute("INSERT INTO notes(body) VALUES (?)", &[Value::from_text("c")])?;
        Ok(())
    })?;
    // The same with the PRAGMA, which works only inside a transaction.
    conn.transaction(|tx| {
        tx.execute("PRAGMA durable_batching = 'window 50ms 64 1MiB'", &[])?;
        tx.execute("INSERT INTO notes(body) VALUES (?)", &[Value::from_text("d")])?;
        Ok(())
    })?;
    let stats = db.upload_stats();
    println!("{} commits acked, {} log PUTs", stats.commits_acked, stats.log_puts);

    for row in conn.query("SELECT id, body FROM notes", &[])? {
        println!("{:?} {:?}", row[0].as_int(), row[1].to_text());
    }

    // Optional: end the generation now (uploads its pages). Errors are reported.
    conn.checkpoint(CheckpointMode::Truncate)?;

    conn.close()?; // close every connection first,
    db.close() // then the database
}
```

The API:

| Item | What it does |
|---|---|
| `open(Config) -> Result<Database>` | Opens `config.dir` as the database's one writer (see the table below). On an empty or restored directory it takes the database over, as `takeover` does (see the warning below). |
| `is_claimed(dir) -> Result<bool>` | `true` when `dir` already holds a writer's own database, so `open` continues it. `false` for a missing or empty directory, one restored and never opened, or one whose takeover was interrupted, where `open` would take the database over. It reads only local files. |
| `takeover(Config) -> Result<TakeoverReport>` | Restores the database into `config.dir`, which must be missing or empty (a `durable.lock` alone counts as empty; `Config` otherwise, before any request), then takes it over there: the head names a new writer id at the next epoch, and a fence record follows in the log. A writer still running elsewhere is fenced for good. No engine opens; `open` of `config.dir` continues as the new writer. See Guarantees. |
| `TakeoverReport` | `generation` (the head's), `epoch` (the head's now, one above the old writer's), `fence_seq` (the fence record's seq in `log/<generation>`), `writer_id` (the new one), `restore` (the last restore's `RestoreReport`), `quiet` (the takeover quiet, `Duration::ZERO` for none) and `elapsed`. |
| `gc(&Config) -> Result<GcReport>` | One GC pass on the bucket alone (`config.dir` is ignored): deletes what an old enough head no longer reaches (see Guarantees). It sends only GETs, HEADs, LISTs and DELETEs, and is safe beside a running writer and restores. It does not retry a failed bucket call: it fails with `Bucket`. |
| `GcReport` | `generation` (the head's, 0 when the prefix holds no database), `trimmed_against` (`Option<u64>`, the manifest it trimmed against), `age` (`Option<Duration>`: that manifest's age by the bucket's clock, or, when none was old enough, the oldest one it asked about), `deleted`, `outcome` and `elapsed`. |
| `GcOutcome` | `Trimmed` (it trimmed against an old enough manifest, possibly deleting nothing), `TooYoung` (every manifest of the head is younger than the minimum age) or `Nothing` (no head, or no manifest yet). `as_str()` gives `trimmed`, `too_young` or `nothing`. |
| `Database::connect() -> Result<Connection>` | A new connection. Open one per thread. |
| `Database::generation()` | The generation the bucket's head is at, as this writer last saw it. |
| `Database::bucket_stats()` | PUTs, GETs, LISTs, DELETEs and bytes sent so far (`store::StatsSnap`). A HEAD (a seal's or GC's stat of an object) counts as a GET. |
| `Database::upload_stats() -> UploadStats` | How many commits each log PUT carried and how long acks waited, since open (fields below). |
| `Database::set_batching(Batching) -> Result<()>` | The setting of every connection that has none of its own, from its next commit. It starts as `Config::batching`. `Config` when the setting is invalid. |
| `Database::batching() -> Batching` | The database's setting. |
| `Database::close()` | Waits for pending uploads, waits for a GC pass in progress and stops the GC thread, releases the directory. It fails with `Config` while a connection is open. A close-time seal of a generation under a takeover quiet waits out the quiet first (see Guarantees). Dropping the last handle and connection also closes it; if a maintenance run is in progress then, the close finishes on the maintenance thread shortly after the drop returns, so call `close()` when you reopen the directory in the same process at once. |
| `Connection::execute(sql, params) -> Result<u64>` | Runs one statement and returns the number of changed rows. `sql` must hold exactly one statement. |
| `Connection::execute_batch(sql)` | Runs several statements, with no parameters. If the guard refuses any statement, none of them runs. |
| `Connection::query(sql, params) -> Result<Vec<Vec<Value>>>` | Runs one statement and collects its rows. |
| `Connection::transaction(\|tx\| …)` | `BEGIN`, your closure (`tx.execute`, `tx.query`), then `COMMIT`. It runs `ROLLBACK` if the closure or the commit fails. |
| `Connection::transaction_with(Batching, \|tx\| …)` | `transaction`, committing with this setting whatever the connection's and the database's are. The next statement or transaction is back to them. `Config` when the setting is invalid, before anything runs. |
| `Connection::set_batching(Option<Batching>) -> Result<()>` | This connection's own setting, from its next commit. `None` falls back to the database's. `Config` when the setting is invalid. |
| `Connection::batching() -> Option<Batching>` | This connection's own setting (`PRAGMA durable_batching` reads the effective one). |
| `Connection::checkpoint(CheckpointMode)` | `Passive`, `Full`, `Restart` or `Truncate`. `Truncate` ends the generation inside the call: its pages upload (a seal), and an upload failure is returned as an error. `Restart` only resets the WAL in memory, so the seal happens in the next commit, which also gets any upload error. |
| `Connection::close()` | Closes the connection. The writer's last connection runs a TRUNCATE checkpoint, which seals. Dropping a connection also closes it. |
| `restore(&Config, dir) -> Result<RestoreReport>` | Restores the bucket's current state into `dir`, which must hold no database. It writes nothing to the bucket. `config.dir` is ignored; `dir` is used. |
| `Value` | `turso_core::Value`, re-exported. |
| `Batching` | `Off` or `Window { max_wait: Duration, max_commits: u32, max_bytes: u64 }` (see Batching). It parses from and prints as the PRAGMA's text (`FromStr`, `Display`). `validate()` refuses a `max_commits` or `max_bytes` of 0, and a `max_wait` above 60 s. |
| `UploadStats` | The snapshot `Database::upload_stats` returns (fields below). |
| `telemetry::TelemetryConfig` | `enabled`, `endpoint`, `level` (`tracing::Level`), `test_run_id`. `Default` is disabled; `TelemetryConfig::from_env()` reads the `DURABLE_OTEL*` variables (see Settings). |
| `telemetry::init(TelemetryConfig) -> Result<TelemetryGuard>` | Installs the OTLP export as the process's global `tracing` subscriber (see Telemetry). Disabled, it installs nothing and returns an empty guard. `Config` when a global subscriber is already set, or the endpoint is not `http://`. |
| `telemetry::TelemetryGuard` | Keeps the export running. Dropping it, or `shutdown()`, flushes and stops the export, for at most 2 s. |

`UploadStats` fields (counters since open):

| Field | What it counts |
|---|---|
| `commits_acked` | Commits whose ack returned `Ok`. The acks of one call (`execute_batch` can make several commits) are counted only when every ack of that call succeeded. |
| `commits_uploaded` | Commits in log records confirmed in the bucket. |
| `log_puts` | Log records confirmed in the bucket that carry at least one commit. Fence, seal-start and reconcile catch-up records carry none and are not counted. |
| `log_put_bytes` | The encoded size of those records. |
| `commits_per_log_put` | `commits_uploaded / log_puts`, 0.0 when `log_puts` is 0. |
| `bytes_per_log_put` | `log_put_bytes / log_puts`, 0.0 when `log_puts` is 0. |
| `ack_wait_p50`, `ack_wait_p95`, `ack_wait_p99` | `Duration` quantiles of how long acks waited for the bucket. Each is the upper edge of its histogram sub-bucket, at most 6.25 % above the true value. |

The WAL-header record that a WAL restart ships (after a checkpoint, and at a close or `Truncate` checkpoint) counts as a commit in these counters. So a run of N commits can show a few more than N.

After an ambiguous log PUT (its reply was lost, so the record may have landed), `commits_uploaded`, `log_puts` and `log_put_bytes` undercount. That record's commits get an error ack at once. When the record is later settled at its log position, it is in the bucket, but these counters never count it.

What `open` does depends on the directory:

| Local directory | Bucket prefix | `open` |
|---|---|---|
| empty or missing | no `head` | Creates a new database with a new writer id. The head starts at generation 1. |
| empty or missing | has a `head` | Restores into the directory, then takes it over, as `takeover` does (see Guarantees). |
| restored by `restore`, never opened | — | Takes it over, then continues. |
| an interrupted takeover (its claim id is set) | — | Resumes the takeover under the same claim id, then continues. |
| this library's directory | — | Continues it. It first raises the head's epoch (one head PUT) and, when the generation's last log record is another writer's, appends a fence record. Then it reconciles the local WAL with the bucket (D9). |
| an `app.db` this library did not create | — | `Error::Config`. An existing SQLite or tursodb file cannot be adopted. |

**Warning: opening a restored or empty directory as the writer takes the database over.** `open` takes the database over under a new writer id, and the old writer, if it is still running anywhere, is fenced for good: its next commit fails with `Fenced`. Open such a directory only on purpose, for example to move the database to a new machine. To read the database without taking it over, use `verify` (CLI). Call `is_claimed(dir)` first to tell the cases apart. The takeover persists its claim id in the directory before it writes anything, so a takeover interrupted by a crash, or one that failed after its head write, resumes at the next `open` of that directory.

The files in the directory:

| File | What it is |
|---|---|
| `app.db`, `app.db-wal` | The database and its WAL. After a restore, `app.db` fills in lazily as pages are read. |
| `app.db-durable` | Local state as JSON: writer id, generation, whether every page is local, the claim, the epoch and the takeover quiet (see Bucket format). |
| `app.db-dirty` | Page numbers checkpointed since the last seal, fsynced before `app.db` is. |
| `durable.lock` | The exclusive lock held by the open process. |
| `app.db-restoring` (inside the directory) and `.<dir>.restore-*` (staging directories next to it) | Exist only while a restore runs. Open or restore cleans up after an interrupted restore. |

Errors are `turso_durable::Error`:

| Variant | When |
|---|---|
| `Fenced` | Another writer took the database over: the head names another writer id (or an epoch below this directory's), or a log position this writer needed holds another writer's record or fence. A fenced writer stays fenced. Also a takeover whose restored generation is no longer the head's: the old writer sealed since the restore (`… restore again`; `takeover` restores again by itself, up to 3 times in all). |
| `Timeout` | A bucket operation kept failing transiently for the whole retry deadline. An `Off` commit that fails this way is rolled back locally; a `Window` commit stays in the local database (see Batching). |
| `UploadFailed` | The bucket refused an upload permanently, or the uploader stopped. |
| `NotFound` | A key that must exist is missing: no database at this prefix (restore, read-only open), or a page the manifest names. |
| `Unsupported` | A refused feature: `MVCC`, `journal_mode other than WAL`, `synchronous below FULL`, `ATTACH`. |
| `Locked` | Another process has the directory open. |
| `AlreadyOpen` | This process already has the directory open. |
| `Corrupt` | A bucket object or local file failed its checks. |
| `Bucket` | A permanent S3 error outside an upload, such as 403 on a GET. |
| `Engine` | An error from turso_core, such as an SQL error. |
| `Io` | A local file error. |
| `Config` | Bad settings (an invalid batching setting or `PRAGMA durable_batching` misused included), a closed database, or connections still open at `close`. |

## Batching

The setting:

```rust
pub enum Batching {
    Off,
    Window { max_wait: Duration, max_commits: u32, max_bytes: u64 },
}
```

- `Off` (the default): each commit is its own upload, made inside the WAL sync.
- `Window`: commits waiting to upload share one PUT. It is sent when the first limit is reached: `max_wait` since the first commit in the batch, `max_commits` commits, or `max_bytes` bytes. Both counts must be at least 1, and `max_wait` at most 60 s (`MAX_BATCH_WAIT` in `mod defaults`); a longer one fails with `Config`.

**Where it is set.** All of these work at runtime, with no reopen:
- `Config::batching` (`DURABLE_BATCHING`): the database's setting at open.
- `Database::set_batching(b)`: the setting of every connection that has none of its own.
- `Connection::set_batching(Some(b))`: this connection's own setting. `None` falls back to the database's.
- For one transaction: `Connection::transaction_with(b, |tx| …)`, or `PRAGMA durable_batching` run inside the transaction.

**Which setting a commit uses:** the transaction's, else the connection's, else the database's. A change takes effect from the next commit. It never changes a commit already waiting.

**`PRAGMA durable_batching`.** The wrapper runs it itself; the engine never sees it.
- `PRAGMA durable_batching = off`, `PRAGMA durable_batching = 'window 50ms 64 1MiB'`, or the call form `PRAGMA durable_batching('…')`.
- Setting it works only inside a transaction (`BEGIN` … `COMMIT`), and lasts until that transaction ends. Outside a transaction it fails with `Config`; use `Connection::set_batching` there.
- Reading it (`PRAGMA durable_batching`, anywhere) returns one row: the effective setting as text, for example `off` or `window 50ms 64 1MiB`.
- `main.` only: `temp.durable_batching` or any other schema is refused with `Config`. `EXPLAIN` of it is refused too.
- In `execute` and `query` it must be the only statement, with no parameters. `execute_batch` runs it in place among other statements.
- The text, also used by `DURABLE_BATCHING` and the CLI's `--batching`: `off` or `window <max_wait> <max_commits> <max_bytes>`.
  - The keywords `off` and `window` are case-insensitive.
  - The unit suffixes are case-sensitive. `max_wait` is an integer with `us`, `ms` or `s`. `max_bytes` is an integer with no suffix, or `B`, `KiB`, `MiB` or `GiB`.

**What each setting guarantees.**
- Every ack waits for that commit's own data in the bucket, in every mode. The log is uploaded in order, so an acked commit also means every earlier commit is durable.
- **Off:** as stage 1. The commit holds the WAL write lock until its PUT returns, so each commit is its own upload when every connection is Off. If Window commits are still waiting when an Off commit syncs, its PUT carries them too, and the log stays in commit order.
- **Window:** the WAL sync completes after the local fsync, so the write lock is released and the next commit can start while this one's upload is pending. The call (`execute`, `COMMIT`, …) returns only once the uploader confirms a log object that contains the commit.
- **Window's trade-off:** other connections in the same process can read a batched commit up to one window before it is durable. If the machine is lost in that gap, the commit is lost, but nobody was told it succeeded. A reader that passes such data on to the outside world could pass on data that is then lost. Off has no such gap.
- **A single connection gains nothing from a window.** Its next commit cannot start until this one is acked, so each PUT still carries one commit, and every commit also waits out the window. Sharing needs several connections committing at once (see Costs).
- **How long an ack can wait.** A Window commit's window starts when the uploader takes the commit off its queue, and the commit's own upload is then retried for at most `retry_deadline` (30 s by default) after its window closes. During an outage, the commit can also queue behind an upload that is already retrying, for up to another `retry_deadline`. So an ack can wait up to about `max_wait` + 2 × `retry_deadline`. The same applies to an Off commit (and to a Window commit on the Off path): queued behind a retrying upload, it waits for that one first, then retries its own for up to `retry_deadline`.
- **Checkpoints:** before a seal uploads pages, the uploader uploads the waiting commits. If they cannot be uploaded, the seal fails with their error rather than seal over them. Closing the database also uploads what waits.

**A failed upload in Window mode.**
- Every waiting commit's ack returns an error (`Timeout` or `UploadFailed`; `Fenced` when another writer took over).
- Those commits are in the local database, and other connections can read them. They are not rolled back.
- The uploader keeps retrying them in order. Later commits queue behind them, as Off commits do while the bucket fails, so nothing is acked out of order. The exception is `Fenced`: a fenced writer never uploads again, so the waiting commits are dropped and never become durable from this writer.
- While the uploader is degraded, a Window commit takes the blocking Off path: it uploads inside its own WAL sync, behind the commits still owed. The uploader is degraded while the writer is fenced, a record is in doubt, a seal's head write is unresolved, or unsent Window commits are owed. So during a bucket outage, Window commits fail and roll back like Off ones, instead of piling up.
- An error after a Window `COMMIT` means "outcome unknown": the row is visible locally, and it may later become durable (the next upload that succeeds carries it). Retrying the same write can apply it twice. Make retried writes idempotent, for example with your own key and `INSERT OR REPLACE` or `ON CONFLICT DO NOTHING`, or read before you retry. A failed Off commit is rolled back locally, but its upload may still have landed (see Guarantees), so the same advice applies.

**Seeing it take effect.**
- `Database::upload_stats()`: `commits_per_log_put` is 1.0 when every commit is Off, and above 1 when commits share windows. The CLI's `stats` prints it.
- Each `durable.upload` span carries the number of commits and bytes that PUT held (see Telemetry).
- `soak --batching-file` changes the setting of a running process, and `--stats-every` prints the stats as it goes (see CLI).

### Maintenance checkpoint

**Why it exists.** A generation ends, and the restore it costs stops growing, only when the WAL restarts: the seal then uploads its pages, and a restore reads only the new generation's log. turso_core 0.8.3 restarts the WAL only when the next writer finds it fully backfilled with no reader on it (`storage/wal.rs:5289-5316`). Window releases the write lock before the upload, so under sustained commits from several connections the commits overlap and that moment never comes. Before this checkpoint was added, the final review's runs with 8 Window connections sealed 0 times in 6000–20000 commits at 1–10 ms windows, the WAL grew to 68–229 MB, and a restore needed about one GET per 2.8 commits. The engine does restart it at times, more often at longer windows (see the figures below). A writer open now seals such a generation itself. Measured on a laptop with 8 connections under `window 1ms 64 1MiB` (`results/batching-knob.txt`): in 6000 commits, a generation held 1888–1945 log objects without it and at most 1005 with it; in the system test, a soak killed after 3000 acks restored 936–973 log objects without it and 1 with it (threshold 200, killed right after a maintenance seal).

**What a run does.** Every writer open with maintenance on runs a background thread, `turso-durable-maintenance`; a read-only open never does. When the current generation is due, the thread:
1. closes the gate (below) and waits for the statements in flight to leave;
2. runs `checkpoint(TRUNCATE)` on its own engine connection, which seals the generation exactly as `Connection::checkpoint(CheckpointMode::Truncate)` does: it uploads the waiting Window commits first, then the pages, the manifest and the head;
3. opens the gate again.

**When a generation is due.** Either measure can trigger a run. 0 turns a measure off, and both 0 turn maintenance off.
- Its log holds at least `maintenance_log_objects` objects (default 1000, `DURABLE_MAINTENANCE_LOG_OBJECTS`). This bounds a restore to about 3 + 1000 GETs, plus the few objects that land while a run drains and seals.
- The local `-wal` has reached `maintenance_wal_bytes` (default 64 MiB, `DURABLE_MAINTENANCE_WAL_MIB`, in MiB).
- Every `Connection` call checks this when it ends (a few atomic loads), and wakes the thread when the generation is due.

**The gate.**
- Every public `Connection` call (`execute`, `query`, `execute_batch`, `transaction`, `transaction_with`, `checkpoint`, `close`) passes the gate around its engine work only. A Window commit's ack wait runs outside it, so an ack wait never holds maintenance up.
- An open explicit transaction counts as in flight until it commits or rolls back: `transaction()`, `transaction_with()`, or `BEGIN` through `execute`, `query` or `execute_batch`. Statements inside a transaction that is already open pass a closed gate. Only new transactions and autocommit statements wait.
- The drain waits at most `maintenance_gate_wait` (default 500 ms, `DURABLE_MAINTENANCE_GATE_WAIT_MS`). `Config` refuses 0 while maintenance is on. If anything is still in flight then (a transaction open longer than that, say), the gate reopens and the run is skipped as `gate_timeout`.
- **The hold bound.** A run holds new statements back for at most `maintenance_gate_wait` while it drains, plus the seal's duration (its page, manifest and head uploads). Only if the seal itself runs into a new outage can the seal take up to `retry_deadline` (30 s by default).

**When it does not run, and retries.**
- No run starts while the bucket is degraded or its last operation failed. The run is skipped as `bucket degraded` or `last bucket operation failed`, with no `MaintenanceStarted`, and the gate stays open. A failed operation is cleared by the next one that succeeds; on an idle database the uploader probes the bucket every 2 s (`MAX_BACKOFF`) with one head GET, which writes nothing, until it answers.
- No run starts during a takeover quiet either (see Guarantees), since its seal would wait out the quiet with the gate closed. The run is skipped as `takeover quiet`, the same way, and retried once the quiet has passed.
- After a skipped run the next waits at least 1 s (`MAINTENANCE_RETRY`). Gate timeouts and busy refusals in a row double that: 1, 2, 4, 8, 16, then 30 s (`MAINTENANCE_RETRY_MAX`). A seal resets it. After a failed run (`error: …`) the next waits 1 s, or runs sooner once the generation has grown by one more threshold. Both values are fixed, in `mod defaults`.
- `busy` means the engine refused the TRUNCATE: engine work outside the gate (a new connection being opened by `Database::connect`, or the prefetch) held the WAL.

**When a maintenance seal fails.** Maintenance never returns an error to a caller; it logs a warning and retries later. But a failed maintenance seal passes its error to the next commit, as a failed `checkpoint(Truncate)` has done since stage 1. The engine restarts the WAL in memory before the seal runs at `truncate(0)`, so the next commit must seal first, and while page PUTs still fail it gets the seal's error. An outage that also fails log PUTs fails commits either way; only an outage that fails page PUTs alone turns into commit failures. Nothing acked is lost.

**It applies in every mode.** Under Off the engine usually restarts the WAL by itself, so the trigger rarely fires (see the figures below).

**Seeing it.**
- Events (`#[doc(hidden)]`, like the others; see Guarantees): `MaintenanceStarted { generation, log_objects, wal_bytes }`; `MaintenanceFinished { generation, gate_wait, elapsed }`, which means the run sealed (`gate_wait` is the drain, `elapsed` the whole run with its seal); `MaintenanceSkipped { generation, reason }`, where `reason` is `bucket degraded`, `last bucket operation failed`, `takeover quiet`, `gate_timeout`, `busy`, `closed` (the database was closing), `panic` or `error: <message>`.
- The `durable.maintenance` span, one per run (see Telemetry), with outcome `sealed`, `bucket_failing`, `takeover_quiet`, `gate_timeout`, `busy`, `closed` or `error`. A failed or panicked run logs a WARN record; routine skips log at INFO.
- `soak` prints the events as lines and `bench` counts them (see CLI).

**What it costs.** Laptop figures (Apple M4 Max, RustFS in podman on the same Mac), from `results/batching-knob.txt`: 6000 commits of 1 KB on 8 connections. The table shows the two final rounds as "a / b"; the file also holds two earlier rounds of the same code, run before `bench` retried busy commits (their Off runs mostly stopped at a busy refusal), which the notes below include. "on" is the defaults, "off" sets both thresholds to 0, and "200" sets `maintenance_log_objects` to 200. With the defaults, `window 50ms` never ran maintenance and Off ran it at most twice.

| Batching | Maintenance | Runs | Max gate wait | Commit p99 | Slowest commit | Most log objects in a generation |
|---|---|---|---|---|---|---|
| `off` | on | 1 / 0 | 82 ms / – | 4.6 / 2.2 ms | 7.5 / 8.9 s | 1009 / 999 |
| `off` | off | 0 / 0 | – | 3.3 / 2.9 ms | 8.2 / 9.3 s | 1488 / 2000 |
| `off` | 200 | 26 / 29 | 437 / 365 ms | 565 / 601 ms | 2212 / 954 ms | 988 / 209 |
| `window 1ms 64 1MiB` | on | 1 / 1 | 31 / 14 ms | 71.3 / 64.9 ms | 382 / 464 ms | 1005 / 1005 |
| `window 1ms 64 1MiB` | off | 0 / 0 | – | 65.6 / 50.3 ms | 230 / 269 ms | 1945 / 1889 |
| `window 10ms 64 1MiB` | on | 1 / 0 | 15 ms / – | 72.8 / 81.1 ms | 528 / 478 ms | 956 / 350 |
| `window 10ms 64 1MiB` | off | 0 / 0 | – | 68.0 / 79.8 ms | 566 / 398 ms | 699 / 354 |
| `window 50ms 64 1MiB` | on | 0 / 0 | – | 74.9 / 110.9 ms | 631 / 466 ms | 447 / 401 |
| `window 50ms 64 1MiB` | off | 0 / 0 | – | 76.8 / 79.0 ms | 425 / 410 ms | 358 / 265 |
| `window 50ms 64 1MiB` | 200 | 1 / 2 | 24 / 41 ms | 120.9 / 119.2 ms | 319 / 248 ms | 202 / 202 |

- Under Window a run holds statements for its gate wait (10.5–41 ms over every round) plus its seal, and the seal grows with the generation.
  - At 1 ms (seal 243–286 ms for about 1150 pages), commit p99 rose by 6–16 ms and the slowest commit by 103–196 ms over four rounds.
  - At 10 ms (seal 467–475 ms for about 2060 pages), the slowest commit rose by about 300 ms in the two earlier rounds (562 and 615 ms against 282 and 239 ms), which fits the seal. In final round a the run without maintenance had its own 566 ms commit. p99 moved by less than 5 ms.
  - At 50 ms (threshold 200) the slowest commit did not rise. p99 was 119–123 ms against 76–79 ms, but runs with the defaults and no maintenance run reached 110.9 ms (final round) and 120.8 ms (the earlier w50-on round, `maintenance_runs=0`), so p99 does not separate them there.
- The engine still restarts the WAL by itself at times under Window (at 10 and 50 ms here), and then maintenance has nothing to do.
- Under Off the defaults ran 0–2 times per 6000 commits, with gate waits of 82 and 203 ms and commit p99 of 2.2–6.4 ms (2.9–3.3 ms without maintenance). Every final Off run's slowest commit (7.5–9.3 s, among the first 8 commits, with or without maintenance) probably includes `bench`'s busy retries (its latency runs from the first attempt; inferred, since the runs report only a `busy_retries` count, not which commits retried) and is start-up lock contention in the engine (inferred).
- Under Off a low threshold is expensive: statements in flight wait in the engine's busy handler while they hold the gate, so each drain took up to 365–437 ms, commit p99 rose to 565–647 ms and throughput fell from 640–667 to 371–407 commits/s. The default threshold keeps runs rare under Off.
- With 8 Off connections the engine also refuses a commit as `database is busy (locked)` now and then, with or without maintenance (3–7 per 6000 commits here). `bench` retries those after a 10 ms pause, for up to 10 s per commit (`busy_retries`); an application has to retry them too.

## CLI

`turso-durable <command> --dir DIR …` (`gc` takes no `--dir`). S3 settings come from the `DURABLE_*` variables. Build it with `cargo build --release --locked -p turso-durable-cli`.

| Command | What it does |
|---|---|
| `restore --dir DIR` | Restores into an empty `DIR`. It prints `restored generation=… log_objects=… gets=… lists=… bytes=… ms=…`. |
| `takeover --dir DIR` | `takeover`: restores into the missing or empty `DIR` and takes the database over there. It prints `took over generation=… epoch=… fence_seq=… writer_id=… quiet_ms=… ms=…`. `sql --dir DIR` then continues as the new writer. |
| `gc` | One GC pass (`gc`) on the bucket prefix, with no directory (`--dir` is a usage error). It prints `gc generation=… trimmed_against=… age_ms=… deleted=… outcome=… ms=…`, with `-` for a missing `trimmed_against` or `age_ms`, and `outcome` `trimmed`, `too_young` or `nothing`. |
| `verify --dir DIR [--acked FILE]` | Opens `DIR` read-only and runs `PRAGMA integrity_check`. With `--acked`, it checks that every id in FILE (lines `ack <id>` or `<id>`) is in table `soak`, and fails if an id was acked twice. `DIR` may be empty, in which case it restores first. Verify never claims, never seals and never writes to the bucket. |
| `sql --dir DIR [--claim] [--batching SPEC] SQL…` | Opens `DIR` as the writer, runs each SQL argument, and prints rows tab-separated. Without `--claim`, it runs only on a directory that already holds a writer's own database. For any other directory (missing, empty, restored and never opened, or holding an interrupted takeover) it exits 1 without touching the bucket, because opening it would restore or create the database and take it over from any running writer. It suggests `verify` or `--claim`. With `--claim`, such a directory is taken over exactly as `takeover` does it (or created, when the prefix holds no database), and an interrupted takeover resumes. There is no read-only fallback; use `verify` to read. |
| `stats --dir DIR [--claim] [--batching SPEC] SQL…` | Opens `DIR` as the writer like `sql` (the same `--claim` rule), runs each SQL argument as its own `execute`, then prints `Database::upload_stats()` as `key=value` lines: `commits_acked`, `commits_uploaded`, `log_puts`, `log_put_bytes`, `commits_per_log_put`, `bytes_per_log_put`, `ack_wait_p50_ms`, `ack_wait_p95_ms`, `ack_wait_p99_ms`. |
| `soak --dir DIR [--checkpoint passive\|full\|restart\|truncate\|reopen] [--every N] [--start ID] [--count N] [--page-put-delay-ms N] [--connections N] [--batching SPEC] [--batching-file PATH] [--stats-every N]` | Commits one 1 KB row at a time into table `soak` and prints `ack <id>` after each acked commit. With `--checkpoint`, it checkpoints (or closes and reopens) every N commits (`--every`, default 100); without it, it never checkpoints itself. Ids start at 1 by default. It runs until killed unless `--count` is given. It retries on errors, printing `error <message>`. More below. |
| `bench --dir DIR --commits N [--connections N] [--batching SPEC]` | Runs N single-row commits of 1 KB, shared by `--connections` threads (default 1), and prints `key=value` lines: commit p50/p95/p99/max, `busy_retries` (commits the engine refused as `database is busy`, each retried after 10 ms for up to 10 s, after which bench fails with the error; latency runs from the first attempt), commits/s, the `stats` keys for the run, `log_puts_per_commit`, `bucket_puts`, `page_puts`, each seal's ms and page count, then the maintenance checkpoint's `maintenance_runs` (runs started), `maintenance_sealed`, `maintenance_skipped`, `maintenance_gate_wait_max_ms` (the longest drain of a run that sealed), and `generation_log_objects_max` (the highest log seq seen before the close: the most log objects a generation held, which a restore of it reads). |

`SPEC` is the PRAGMA's text, `off` or `window <max_wait> <max_commits> <max_bytes>` (for example `'window 10ms 64 1MiB'`). `--batching` sets it with `Database::set_batching` after open; without it, `DURABLE_BATCHING` applies.

`soak`'s other options and lines:
- `--connections N`: N threads, one connection each. Thread `i` commits ids `start + i + k·N`.
- `--batching-file PATH`: re-read before each commit when the file's mtime or length changed. A valid setting is set on the database and printed as `batching <setting>`. An unreadable file or a bad setting prints one `error: …` line on stderr and keeps the current setting.
- `--stats-every N`: every N acks, prints the upload stats as `stats <key>=<value>` lines (the `stats` keys), taken while no commit runs.
- It prints `event seal-started` and `event seal-finished` around each seal, and `event checkpoint <mode>` after each `--checkpoint` call (`passive`, `full`, `restart`, `truncate` or `reopen`). With `--batching` or `--batching-file` it also prints:
  - `event batch-waiting <n>` when a Window commit joins a batch of n, and `event log-uploaded <commits>` (commit numbers such as `4,5,6`, or `-` for none) when a log record is confirmed;
  - for the maintenance checkpoint, `event maintenance-started log_objects=<n> wal_bytes=<n>`, `event maintenance-finished gate_wait_ms=<ms> ms=<ms>` (it sealed; the drain and the whole run, 3 decimals) and `event maintenance-skipped <reason>`.

Every command exports telemetry when `DURABLE_OTEL=1` (see Telemetry). If the export cannot start, it prints `warning: telemetry off: …` on stderr and runs without it.

Exit codes: 0 on success, 1 on an error (one `error: …` line on stderr), 2 on bad arguments.

**Warning:** `takeover`, and `sql --claim` on a restored or empty directory, take the database over. The old writer is fenced, and its next commit fails. `soak` and `bench` always open as the writer, so run them only on their own prefix.

## Settings

`Config::from_env(dir)` reads these variables. `Config::new(dir, endpoint, bucket, access_key, secret_key)` uses the same defaults, and every `Config` field is public.

| Variable | `Config` field | Default | Notes |
|---|---|---|---|
| `DURABLE_ENDPOINT` | `endpoint` | required | `http(s)://host[:port]`, with no path. The scheme is case-insensitive, and a default port (`:443` for https, `:80` for http) is dropped so the signed host is what clients send. |
| `DURABLE_CA_BUNDLE` | `ca_bundle` | unset (empty = unset) | A PEM file of the CA certificates that sign the endpoint's certificate. They replace the system trust store, so the endpoint's certificate must chain to one of them. Refused with an `http://` endpoint (`Config`), and when the file is missing or holds no certificate. Unset, the platform's verifier and trust store are used. |
| `DURABLE_BUCKET` | `bucket` | required | |
| `DURABLE_ACCESS_KEY` | `access_key` | required | |
| `DURABLE_SECRET_KEY` | `secret_key` | required | Redacted from `Debug`. |
| `DURABLE_REGION` | `region` | `us-east-1` | Used for SigV4. |
| `DURABLE_PATH_STYLE` | `path_style` | `true` | `endpoint/bucket/key`. Set it to `false` for virtual-hosted style. Accepts `0`/`1`, `true`/`false` or `on`/`off`. |
| `DURABLE_PREFIX` | `prefix` | `""` (bucket root) | Every key goes under `<prefix>/`. Use one prefix per database. Only `A-Z a-z 0-9 . _ - /` are allowed, with no leading or trailing `/`, no `//` and no `.` or `..` segment; anything else fails with `Config`. |
| `DURABLE_RETRY_DEADLINE_MS` | `retry_deadline` | 30 s | How long one bucket job may keep retrying transient errors: a commit's log upload, a seal, or a page fetch. |
| `DURABLE_REQUEST_TIMEOUT_MS` | `request_timeout` | 10 s | One HTTP request. |
| `DURABLE_UPLOAD_CONCURRENCY` | `upload_concurrency` | 16 | Parallel page PUTs in a seal, and parallel log GETs in a restore. Must be at least 1. |
| `DURABLE_TRIM_GRACE_SECS` | `trim_grace` | 10 min | How long an unreferenced object stays before GC deletes it. GC trims against a manifest only once the bucket's own clock says it is `trim_grace` + `retry_deadline` + `request_timeout` + 1 s old (10 min 41 s with the defaults; see Guarantees).<br>• It must exceed `lease` + `retry_deadline` + `request_timeout` (70 s with the defaults). `Config` refuses a smaller or equal value, `0` included.<br>• Against another process's old writer, it protects only if it also exceeds that process's `lease` + `retry_deadline` + `request_timeout`. The same settings on every process satisfy this; nothing in the bucket records another process's settings, so nothing enforces it. |
| `DURABLE_LEASE_MS` | `lease` | 30 s | How long a writer trusts its last confirmation that the head is still its own. Past it, on the monotonic or the wall clock, whichever is further, the next append, and each page or manifest PUT attempt of a seal, GETs the head first (see Guarantees). The takeover quiet is `lease` + `request_timeout`. `0` checks before every append and every round of page PUTs (tests). |
| `DURABLE_GC_AFTER_SEAL` | `gc_after_seal` | `true` | A writer open runs GC itself, on its own thread (`turso-durable-trimmer`): at open, after each seal and when the next manifest comes of age. Off, only `gc` deletes anything. Accepts `0`/`1`, `true`/`false` or `on`/`off`. |
| `DURABLE_PREFETCH` | `prefetch` | `false` | After open, fetch every sealed page in the background. Accepts `0`/`1`, `true`/`false` or `on`/`off`. |
| `DURABLE_BATCHING` | `batching` | `off` | The database's batching setting at open, in the PRAGMA's text: `off` or `window <max_wait> <max_commits> <max_bytes>` (see Batching). A bad value fails with `Config`. |
| `DURABLE_MAINTENANCE_LOG_OBJECTS` | `maintenance_log_objects` | 1000 | A maintenance checkpoint seals the generation once its log holds this many objects (see "Maintenance checkpoint"). 0 turns this measure off. |
| `DURABLE_MAINTENANCE_WAL_MIB` | `maintenance_wal_bytes` | 64 MiB | … or once the local `-wal` reaches this size. The variable is in MiB; the field is in bytes. 0 turns this measure off; with both 0, maintenance is off. |
| `DURABLE_MAINTENANCE_GATE_WAIT_MS` | `maintenance_gate_wait` | 500 ms | The longest a maintenance run holds new statements back while it waits for those in flight (and open transactions) to end; then it reopens the gate and retries later. `Config` refuses 0 while maintenance is on. |
| `DURABLE_ENCRYPTION_KEY_FILE` | `encryption_key_file` | unset (empty = unset) | A local file holding the database's encryption key as hex, either case, with whitespace around it ignored: 32 bytes (64 hex digits), or 16 bytes (32 digits) for `aes128gcm` and the `aegis128*` ciphers. Set, the database is encrypted (see Guarantees, Encryption). Read at open, never uploaded or logged. A file that cannot be read, or holds anything else, fails `open` with `Config`, which names the file and never its content. The setting must match the database: a key on an unencrypted one, or none on an encrypted one, fails `open` with `Encryption`. |
| `DURABLE_ENCRYPTION_CIPHER` | `encryption_cipher` | `aegis256` (empty = default) | Any cipher name turso accepts: `aegis256`, `aegis256x2`, `aegis256x4`, `aegis128l`, `aegis128x2`, `aegis128x4`, `aes256gcm` or `aes128gcm`, any case, also with `-` or `_` (`aes-256-gcm`). Anything else fails with `Config`, and so does a value other than the default without a key file. It must be the cipher the database was created with (`Encryption` otherwise). |

`TelemetryConfig::from_env()` reads the telemetry variables (`Config::from_env` does not):

| Variable | `TelemetryConfig` field | Default | Notes |
|---|---|---|---|
| `DURABLE_OTEL` | `enabled` | off | Export spans and log records. Accepts `0`/`1`, `true`/`false` or `on`/`off`. `./scripts/test.sh` turns it on. |
| `DURABLE_OTEL_ENDPOINT` | `endpoint` | `http://localhost:4317` | The OTLP/gRPC collector. Only `http://` (no TLS); anything else fails with `Config`. Set in code, so `OTEL_EXPORTER_OTLP_*ENDPOINT` cannot redirect it, but `OTEL_EXPORTER_OTLP_*HEADERS` are still sent. |
| `DURABLE_OTEL_LEVEL` | `level` | `info` | The least severe log record exported: `trace`, `debug`, `info`, `warn` or `error`. Spans are always INFO. |
| `DURABLE_TEST_RUN_ID` | `test_run_id` | unset | Added as the `test_run_id` attribute to every span and log record. An empty value counts as unset. |

Where the defaults live:
- `crates/turso_durable/src/config.rs`, `mod defaults`, holds every default above.
- The retry backoff is fixed and cannot be configured. It starts at 50 ms (`FIRST_BACKOFF`), doubles, and is capped at 2 s (`MAX_BACKOFF`). Both are in `mod defaults` too. `RetryPolicy::default` in `crates/turso_durable/src/store/retry.rs` takes all three retry values from there.
- The batching default, `Off`, is `BATCHING` in `mod defaults`. It stays `Off` until there are numbers from real S3. The 60 s cap on a window's `max_wait` is `MAX_BATCH_WAIT`, there too; it cannot be configured. The telemetry defaults are there too (`OTEL`, `OTEL_ENDPOINT`, `OTEL_LEVEL`), with the fixed export limits: one export times out after 1 s (`OTEL_EXPORT_TIMEOUT`), shutdown waits at most 2 s (`OTEL_SHUTDOWN`), and each queue holds 2048 spans or log records (`OTEL_QUEUE`).
- The maintenance defaults are `MAINTENANCE_LOG_OBJECTS`, `MAINTENANCE_WAL_BYTES` and `MAINTENANCE_GATE_WAIT` in `mod defaults`, with the fixed retry delays: 1 s after a skipped or failed run (`MAINTENANCE_RETRY`), doubled for gate timeouts and busy refusals in a row up to 30 s (`MAINTENANCE_RETRY_MAX`).
- The lease, trim grace and GC defaults are `LEASE`, `TRIM_GRACE` and `GC_AFTER_SEAL` in `mod defaults`, with two fixed limits: a takeover's fence record steps over at most 64 log positions (`FENCE_MAX_STEPS`), and a seal goes on past its retry deadline for at most 3 rounds in a row in which the store only answered (412s and HEADs) without storing anything (`MAX_ANSWER_ONLY_ROUNDS`). A failed GC pass on a writer's thread is retried at the next seal or after 30 s (`MAINTENANCE_RETRY_MAX`).
- The longest `seal_quiet_ms` a local state may carry is one day (`MAX_SEAL_QUIET` in `mod defaults`); it cannot be configured. A larger value makes `open` fail with `Corrupt`.
- The encryption cipher default is `ENCRYPTION_CIPHER` in `mod defaults`.
- Every boolean variable (`DURABLE_PATH_STYLE`, `DURABLE_PREFETCH`, `DURABLE_GC_AFTER_SEAL`, `DURABLE_OTEL`) accepts `0`/`1`, `true`/`false` or `on`/`off`, case-insensitive; anything else fails with `Config`.
- The busy timeout between connections is fixed at 5 s. It lives in `crates/turso_durable/src/conn.rs` (`BUSY_TIMEOUT`).

## Telemetry

The library reports only through `tracing`. `telemetry::init(TelemetryConfig)` exports it over OTLP/gRPC to `endpoint` (`http://localhost:4317` by default), with `service.name = turso-durable`, through a global `tracing` subscriber. It is off unless `enabled` (`DURABLE_OTEL=1`). The CLI calls it at start.

**Spans.** These names and attributes are a contract: the HyperDX searches depend on them. Every span is INFO, with target `turso_durable`.

| Span | Parent | Attributes |
|---|---|---|
| `durable.commit` | the caller's span | `durable.batching` (the setting's text), `durable.commit_no`, `durable.bytes`, `durable.outcome` (`ok` or `error`) |
| `durable.local_write` | `durable.commit` | |
| `durable.local_fsync` | `durable.commit` | |
| `durable.ack_wait` | `durable.commit` | The wait for the commit's log record: inside the WAL sync for an Off commit (or a Window commit on the Off path), after the engine returns for a Window commit. |
| `durable.upload` | the first commit's `durable.commit` | One per log PUT: `durable.commits`, `durable.bytes`, `durable.generation`, `durable.seq`, `durable.retries`, `s3.status` |
| `durable.checkpoint_upload` | the caller's span | One per seal: `durable.generation`, `durable.seq` (the seal-start record's), `durable.pages`, `durable.retries`, `s3.status` |
| `durable.restore` | the caller's span | `durable.generation`, `durable.seq` (the highest log seq replayed, 0 when the log is empty), `durable.log_objects`, `durable.gets`, `durable.bytes`, `durable.retries`, `s3.status` |
| `durable.maintenance` | none (the maintenance thread) | One per maintenance run: `durable.generation`, `durable.log_objects`, `durable.wal_bytes` (the measures that made it due), `durable.gate_wait_ms` (the drain; absent when the run did not start), `durable.outcome` (`sealed`, `bucket_failing`, `takeover_quiet`, `gate_timeout`, `busy`, `closed` or `error`). The seal's `durable.checkpoint_upload` is its descendant. |
| `durable.takeover` | the caller's span (`takeover`, or the `open` that takes a directory over) | One per takeover, around its head write, attach and fence record (the restore before it has its own `durable.restore`), and one around the fence of a takeover an `open` resumes (its head write landed, its fence did not): `durable.generation` (the restored generation), `durable.epoch` (the new epoch, recorded once the head write has landed, so a takeover that fails after it still shows it), `durable.seq` (the fence record's), `durable.retries`, `s3.status`, `durable.outcome` (`ok` or `error`) |
| `durable.fence` | the caller's span (`durable.takeover`, or the writer open that appends one) | One per fence record: `durable.generation`, `durable.seq`, `durable.epoch`, `durable.retries`, `s3.status` |
| `durable.gc` | the caller's span for `gc`; none on a writer's GC thread | One per GC pass: `durable.generation` (the head's), `durable.manifest` (the manifest trimmed against; absent otherwise), `durable.age_ms` (as `GcReport::age`), `durable.deleted`, `durable.outcome` (`trimmed`, `too_young`, `nothing` or `error`) |

Every span and log record also carries `test_run_id` when one is set (`DURABLE_TEST_RUN_ID`).

**Log records** have the target `turso_durable`: `database opened`, `seal finished` and `restore finished` (INFO), `log upload failed` (WARN) and `writer fenced` (ERROR). A takeover logs `took over` (INFO, with `generation`, `epoch`, `fence_seq` and `writer_id`), and `takeover quiet: the old writer's seal may still be uploading` (INFO) when it sets a quiet; an open that finds a quiet still pending logs `takeover quiet: pending at open, waited again in full` (INFO, with `generation` and `ms`); a seal that waits one out logs `seal waits out the takeover quiet` (INFO). An open that finds its own earlier process's seal landed logs `a seal of an earlier process landed during the open; attaching again` (INFO). A failed GC pass on a writer's thread logs `gc pass failed` (WARN). Maintenance logs `maintenance checkpoint not started; retrying later` and `maintenance checkpoint skipped; retrying later` (INFO), and `maintenance checkpoint failed; retrying later` and `maintenance checkpoint panicked; retrying later` (WARN).
- Each record keeps its own time. Without one it gets its observed time, and without that the current time.
- Log records carry no trace or span ids. Adding them would need an experimental OpenTelemetry feature.

**What is exported.** Only targets that start with `turso_durable`: spans at INFO and above, and log records at `level` and above. turso_core's own `tracing` output is never exported, and neither is the exporter's own stack (`opentelemetry*`, `tonic`, `h2`, `hyper`, `tower`, `reqwest`).

**A collector that is down, slow or missing never slows or fails a commit.**
- Export runs on background threads, through bounded queues (2048 spans, 2048 log records) that drop when full. Each export times out after 1 s.
- No connect timeout is set on the exporter. A collector that drops SYNs can hold one export longer than 1 s. Commits are unaffected, and the queues drop when full.
- Dropping the `TelemetryGuard` (or `shutdown()`) flushes and stops the export, and returns within 2 s even when the collector does not answer.
- The system test `telemetry_collector_down_and_blackhole` runs with a collector that refuses connections and one that accepts them and never answers.

**Subscribers.**
- `init` sets the global subscriber, and fails with `Config` if one is already set.
- A process that instead uses only scoped subscribers (`tracing::subscriber::with_default`) can lose spans to tracing's callsite cache: a callsite first hit on a thread with no subscriber is cached as disabled for every thread. `init`'s global subscriber avoids this.

**The ClickHouse check.** `scripts/otel-check.sh RUN_ID` checks that one run's telemetry reached the observability stack.
- It runs read-only `SELECT`s, as the stack's read-only ClickHouse user, on `otel.otel_traces` and `otel.otel_logs`, for `ServiceName = 'turso-durable'` and that `test_run_id`.
- It passes when every span name above except `durable.maintenance` (its short workload never makes a generation due) has at least one span, at least one log record is dated today, and no log record is dated before 2000-01-01. It polls for up to 60 s.
- Settings: `DURABLE_CH_URL` (default `http://localhost:8123`), `DURABLE_CH_USER` (default `hyperdx_ro`) and `DURABLE_CH_PASSWORD`. Without `DURABLE_CH_PASSWORD`, it reads the password at run time from the stack's users SQL file (`~/Dev/p/sre/stack/clickhouse/00_database_users.sql`). The password reaches curl only on stdin; it is never printed, written to a file or put on a command line.
- It prints one line: `otel check PASSED: …` (exit 0), `otel check FAILED: …` (exit 1), or, when ClickHouse or the collector port 4317 does not answer, `otel check SKIPPED: observability stack not running` (exit 0). A missing `nc` is `otel check FAILED: nc not found`.

## Bucket format v1

Everything lives under `<prefix>/`. With an empty prefix, keys have no leading `/`.

| Key | Holds |
|---|---|
| `head` | Where the database is now. |
| `log/<generation:010>/<seq:020>` | One log object per upload: one WAL fsync when Off, several when commits share a window. Or a fence record. `seq` starts at 1 and has no gaps within a generation. |
| `pages/<generation:010>/<page:010>` | One page, as sealed at the end of `generation`. |
| `manifest/<generation:010>` | Page map written when `generation` was sealed. |

**Log object.** All integers are little-endian.
- The magic `b"TDLOG\0\0\x01"`.
- `kind: u8`: 1 = WAL extents, 2 = fence (below). Stages 1 and 2 read a fence as `Fenced`.
- `writer_id`: a `u8` length, then ASCII.
- `sync_count: u32`. A log object holds one or more syncs: one per commit when Off, several when commits share a window. The seal-start record (see Guarantees) is the exception: it holds zero syncs, so it adds nothing to the generation's WAL image. Stage 2's claim record was one too.
- For each sync, `op_count: u32`, then the ops:
  - tag 1, a write: `[u64 offset][u32 len][bytes]`;
  - tag 2, a truncate: `[u64 len]`.
- A trailing `sha256` of all preceding bytes. Nothing may follow the last op: trailing bytes are `Corrupt`.

**Fence record** (kind 2). A takeover appends one at the next free seq of the head's generation.
- The magic, `kind = 2`, then the writer id as above (the new writer's).
- `sync_count: u32 = 0`.
- `epoch: u64`: the head's epoch that the takeover wrote.
- The trailing `sha256`. A fence with a non-zero sync count, without its epoch, or with trailing bytes is `Corrupt`.

Restore replays a generation's records under one rule. Before the first fence, each writer's records form one run, and no other writer's record follows one of the head's writer (logs from stages 1 and 2 hold no fence and follow only this). After a fence by writer `w` at epoch `k`, every Extents record must be `w`'s. A later fence needs an epoch above `k`, or `k` again from `w` (a resumed takeover). Anything else is `Fenced`. A fence adds nothing to the image.

**Manifest.**
- The magic `b"TDMAN\0\0\x01"`.
- `page_size: u32`, `page_count: u32`.
- `run_count: u32`, then runs of `[first_page u32][count u32][generation u64]`, sorted and non-overlapping.
- A trailing `sha256`.

Reading a page inside `1..=page_count` that no run covers fails with `Corrupt`.

**Head.** Flat JSON with keys in this order. Unknown keys are ignored when it is read.

```json
{"format":1,"generation":3,"manifest":"manifest/0000000002","writer_id":"<32 hex>","epoch":4}
```

`manifest` is `null` until the first seal. Restore takes pages from `manifest` and replays `log/<generation>/*`.

**The epoch.** A head without `epoch` (written by stages 1 and 2) reads as epoch 0.
- A new database starts at epoch 1.
- A takeover writes the head with the new writer id at the epoch above the one it read.
- Every other writer open raises the epoch by one and keeps its writer id (one head PUT per writer open; a read-only open never writes). So does a writer whose seal's head write had no definite answer, once a head GET shows the write has not landed.
- The raise is a `PUT` with `If-Match` on the head's ETag, so a head PUT an earlier process (or an earlier attempt) still had in flight carries the old ETag and gets 412 instead of landing.
- A writer open is refused with `Fenced` while the head names another writer id, or an epoch below the one the directory holds.

**Local state `<dir>/app.db-durable`.**

```json
{"format":1,"writer_id":"…","generation":3,"complete":true,"claimed":true,"head_etag":"…","epoch":4,"claim_id":"","seal_not_before_ms":0,"seal_quiet_ms":0}
```

- It is written to a temp file, fsynced, renamed, and then the directory is fsynced.
- `complete=false` means pages not yet read since this process opened come from the bucket. Only a finished prefetch in a writer open writes `complete=true`, at once; a seal also writes the current value. A read-only open's prefetch keeps it in memory only, because a read-only open never checkpoints its WAL into `app.db`. Taking in a newer generation on a directory no writer has claimed sets `complete=false`.
- `claimed=false` with `head_etag` marks a restored directory that no writer has opened yet. An older file without these two keys reads as `claimed=true` with an empty `head_etag`. `head_etag` is kept, but a takeover checks the restored generation, not this ETag (see Guarantees).
- `epoch`: the head's epoch as this directory last wrote or restored it. Written after the head, so it never exceeds the head's.
- `claim_id`: the new writer id of a takeover in progress, persisted before its head write and emptied once the takeover is recorded. While it is set, the next writer open resumes the takeover under it.
- `seal_not_before_ms`: the end of the takeover quiet in unix ms (see Guarantees), 0 for none. Only whether it is 0 counts at a reopen: the reopen waits the quiet's full length again. A seal of the generation writes 0.
- `seal_quiet_ms`: the takeover quiet's length in ms, 0 for none. A seal of the generation writes 0.
- A file without `epoch`, `claim_id`, `seal_not_before_ms` or `seal_quiet_ms` reads them as 0, `""`, 0 and 0. A pending quiet (`seal_not_before_ms` not 0) without `seal_quiet_ms` lasts the reopening process's `lease` + `request_timeout`.

**Writer id.** 16 random bytes in hex. A new database and every takeover get a new id.

## S3 features needed

| Feature | Used for |
|---|---|
| AWS Signature V4 (header auth, signed payload hash) | Every request. |
| Path-style addressing (the default) or virtual-hosted style | `DURABLE_PATH_STYLE`. |
| `PUT` with `If-None-Match: *` (412 when the key exists) | Log objects and fence records. This is what fences a second writer. Also the first PUT of each page and manifest in a seal. |
| `PUT` with `If-Match: <etag>` (412 when the ETag changed) | `head` (seals, epoch raises, takeovers), and a page or manifest PUT over an object of a known ETag (this process's own, or one a HEAD read). A 404 whose error code is `NoSuchKey` also counts as a failed precondition, because RustFS answers that way for a missing key. A 404 `NoSuchBucket` is a permanent error that names the bucket, and so is a 404 with no recognisable code (a proxy or a wrong endpoint is no evidence that the key is gone); neither is reported as `Fenced`. A 409 is retried. |
| `ETag` on every PUT response and GET | Checking `head` before moving it, and the condition of the next PUT of a page or manifest. |
| `HEAD` with `ETag`, `Last-Modified` and `Date` | The ETag of a leftover page or manifest, and GC's age of a manifest by the bucket's clock (`Date` − `Last-Modified`, 1 s resolution). |
| `GET` | Restore, lazy page reads. |
| `ListObjectsV2` (`list-type=2`, continuation tokens) | Listing a generation's log, and trimming. |
| `DELETE` | Trimming. |
| HTTPS (TLS 1.2+, rustls/ring) or plain HTTP | `https://` verifies the endpoint's certificate against the system store, or against `DURABLE_CA_BUNDLE`. A certificate failure is `Permanent` (the error names `DURABLE_CA_BUNDLE`), so `open` fails at once instead of retrying to the deadline. |

Stores known to support both conditional PUTs (`If-None-Match: *` and `If-Match`): AWS S3 (`If-None-Match` since August 2024, `If-Match` since November 2024), and RustFS 1.0.0, which passed this repo's gate (`results/rustfs-gate.txt`). Fencing depends on them, so check any other store with the gate first.

`scripts/s3-gate.sh` checks the conditional-PUT behaviour, and (check 5, `head_dates`) that HEAD returns the PUT's ETag with `Last-Modified` and `Date`, against the local RustFS. Its results for RustFS 1.0.0 are in `results/rustfs-gate.txt`, and all checks passed.

## Guarantees and limits

**What an ack means.**
- When `execute`, `execute_batch`, `transaction`, `transaction_with` or `COMMIT` returns `Ok`, the commit is in the bucket and in the local fsynced WAL, under every batching setting. A restore on another machine includes it.
- `synchronous` is FULL only. `PRAGMA synchronous` set to `OFF`, `NORMAL`, `0` or `1` is refused with `Unsupported("synchronous below FULL")`. A connection whose mode drops anyway has its statement refused, and FULL is put back.
- A commit that returns an error was not acknowledged. With `Off` (or a Window commit that took the Off path), it is rolled back locally and the connection stays usable; a Window commit whose ack failed stays in the local database instead (see Batching). Its upload may still have landed, though, because a request can succeed after its reply is lost. Such a record is settled at the same log position before anything else is written, so the log never forks. Treat an error as "outcome unknown", as with any remote database.

**What is refused.** The `Connection` wrapper parses every SQL string with `turso_parser` and refuses these before the engine sees them (D5, D7, D16):

| Statement | Error |
|---|---|
| `PRAGMA journal_mode` set to `mvcc` or `experimental_mvcc` | `Unsupported("MVCC")` |
| `PRAGMA journal_mode` set to anything but `wal` | `Unsupported("journal_mode other than WAL")` |
| `PRAGMA synchronous` below FULL | `Unsupported("synchronous below FULL")` |
| `ATTACH` | `Unsupported("ATTACH")`. Writes to an attached database would not be uploaded. |
| `PRAGMA hexkey` or `PRAGMA cipher` set to a value | `Unsupported("encryption PRAGMAs (use DURABLE_ENCRYPTION_KEY_FILE)")`. The engine would swap the connection's key or cipher (translate/pragma.rs:644-655). Reading them is allowed. |

MVCC is unsupported. It is also refused at open: from bytes 18/19 of a local `app.db` or of the manifest's page 1, and from `Database::mvcc_enabled()`. Opening an MVCC log file (`*.db-log`) is refused at the IO layer as a backstop.

**Encryption (D16).** With `DURABLE_ENCRYPTION_KEY_FILE` set, turso's own page codec encrypts the database with an authenticated cipher (AEGIS or AES-GCM).
- What is encrypted: every page the engine writes to `app.db` (storage/database.rs:279-330) and every WAL frame body (storage/wal.rs:3204-3260). A seal uploads `app.db`'s bytes and a log record carries the `-wal`'s, so every page and every WAL frame body in the bucket is ciphertext. A test searches every object of an encrypted bucket (Off and Window commits, an index, a seal, a takeover, GC) for the rows, `CREATE TABLE`, the SQLite header and the key, and finds none (`encrypted_bucket_has_no_plaintext`); without encryption the same search finds the rows and the schema (`plaintext_bucket_shows_the_schema`).
- What stays in clear: the WAL header and each frame's 24-byte header (page number, database size, salts, checksums); page 1's bytes 16-99, the database header (page size, page count, schema cookie, the MVCC bytes 18/19), which the cipher authenticates but does not encrypt, while bytes 0-15 become `Turso`, a version byte and the cipher id (storage/encryption.rs:668-682, 819-865); and everything turso_durable writes itself: the head, the manifests, the log records' framing (writer ids, offsets, lengths), fence records, the key names (generations, seqs and page numbers), object sizes and the timing of uploads.
- The key never leaves the machine. It is read from the local file at open, held as turso's `EncryptionKey` (redacted from `Debug`, zeroed on drop: storage/encryption.rs:143-170), and never uploaded or logged; errors name the file or the cipher, never the key. A lost key loses the database: nothing in the bucket recovers it.
- No rekey. The key and the cipher are the database's for its life, and `PRAGMA hexkey` and `PRAGMA cipher` are refused (above). To change them, copy the data into a new database under a new prefix.
- An existing plaintext database cannot be encrypted in place. turso takes an existing database's reserved bytes from its header (database.rs:3119-3180), and a plaintext one has none for the nonce and tag, so opening it with a key is refused with `Encryption`. Use a new prefix and copy the data.
- The check, before anything is written to the bucket: a writer open compares page 1 with the setting before its claim, epoch raise or fence, and refuses with `Encryption` a plaintext page 1 with a key, an encrypted one without a key, another cipher, or a key that does not decrypt page 1 (`EncryptionContext::decrypt_page`). Page 1 comes from the local files, or from what the open's own restore fetched (no extra GET); only the first writer open of a directory restored earlier (by `restore`) whose page 1 is only in the bucket GETs it (head, manifest, page 1: 3 GETs), encrypted or not. The engine open checks again, on page 1 as the engine reads it.
- `restore` needs no key: it copies bytes, and seeds `app.db` with the WAL's encrypted page-1 frame. `open`, `verify` and `sql` need it.
- `takeover` with a key checks it the same way before its head write, on the page 1 its restore fetched or seeded (no extra GET): a wrong key, another cipher, or a key on a plaintext database fails with `Encryption`, nothing is written to the bucket, the restored files are removed, and the running writer is not fenced. Without a key, `takeover` cannot tell (it never opens the engine): it takes an encrypted database over, and the first open of its directory then needs the key.
- A damaged page now fails authentication when it is read (`Engine("Decryption failed for page=…")`) instead of reading as garbage, and a damaged page 1 is refused at open like a wrong key.
- An encrypted page or frame is the size of a plaintext one: the nonce and tag take reserved bytes inside the page. Batching, the maintenance checkpoint, seals, lazy fetches, prefetch, GC and the MVCC refusal work as before; the unit tests run encrypted variants of them.

**One writer.**
- A second process that opens the same directory gets `Locked`. A second open in the same process gets `AlreadyOpen`. Readers in another process are not supported (D4).
- The bucket allows one writer per prefix. A writer whose log position or head was taken by another writer gets `Fenced` and stays fenced.

**Takeover, the lease and GC.**

*Restore.*
- `restore` is read-only on the bucket. It costs `3 + log_objects` GETs when a manifest exists (head, manifest, page 1 for the MVCC check, then the log), or `1 + log_objects` for a prefix never sealed (page 1 is seeded from the WAL), plus one LIST. Pages are fetched later, on first read, or in the background with `prefetch`. The first writer open of a directory restored this way whose page 1 is only in the bucket GETs the head, the manifest and page 1 again before it takes over, for the encryption check (see Encryption); an open or `takeover` that restores by itself reuses its restore's page 1.
- A restored directory is *unclaimed* until the first writer open takes it over.
- `verify` (and the hidden `open_read_only`) never takes over, never seals and never writes to the bucket.
- When the bucket holds no manifest yet, restore seeds `app.db` with the WAL's first page-1 frame. turso treats a zero-size database file as new and would delete its WAL (database.rs:738-760, 2223-2246).

*Takeover (D11).* One mechanism serves `takeover` (library and CLI), a writer `open` (or `sql --claim`) of an empty directory whose prefix holds a database, and the first writer `open` of a restored directory. After the restore:
1. A new writer id, the claim id, is persisted in the directory before anything is sent.
2. The head is read. One that already names the claim id is this takeover, landed before a crash: it resumes. One of another generation than the restored one means the old writer sealed since the restore: `Fenced("… restore again")`, and nothing is written. Otherwise the head is written with the claim id at the next epoch, with `If-Match` on the ETag just read. A 412 reads it again under the same rules, within one retry deadline.
3. The directory records the new writer id and epoch.
4. A fence record goes to the generation's next free seq (`If-None-Match: *`). A seq the old writer took first, or a fence of a lower epoch (a takeover that lost the head), is stepped over, at most 64 times (`FENCE_MAX_STEPS`) and within the retry deadline. Past that the takeover fails with `Timeout`. The head already names the new writer, and the error says to open the directory to resume: ``open this directory to resume the takeover (`open`, or `turso-durable sql --claim --dir DIR 'SELECT 1'`)``.
5. The takeover quiet, when another writer's last record before the fence is a seal-start record (below).
6. Reconcile, after the fence: what the old writer acked between the restore and the fence comes into the directory.

More on takeover:
- `takeover` restores first. If the old writer sealed in between (step 2's "restore again"), it removes the restored files and restores again, at most 3 times in all. A directory that holds only the `durable.lock` of a failed takeover counts as empty.
- The head is written first, then the fence. Only one of two racing takeovers wins an epoch's `If-Match`, so a fence is only ever written by its epoch's winner, and restore can check fences strictly. Two takeovers can still both succeed, one after the other, at distinct epochs; the lower one's directory then fails `Fenced` at open.
- Step 2 checks the restored *generation*, not the restore's ETag: every writer open raises the epoch, so the ETag also changes when the old writer merely restarts. A head of the same generation still has the restored generation's log, and reconcile takes in what was acked since.
- A writer open of its own directory also appends a fence when the generation's last record is another writer's (a takeover whose fence never landed).
- A writer open whose epoch raise finds the head of its own crashed process's seal (that seal's head PUT landed during the open) attaches again once, at the sealed generation.

*What the old writer sees.* After a takeover, nothing the old writer sends is acked or published, except the residual below (a seal PUT sent after a pause between its lease check and its send):
- Its next append collides with the fence record (or a later record of the new writer's) and fences it. If `log/G` was trimmed meanwhile, its lease check reads the moved head first and fences it (below).
- Its next seal starts with an empty *seal-start* record at its next seq (`If-None-Match: *`, one extra log PUT per seal), then a GET of `head` (one extra GET per seal). The record collides with the fence, or the head check sees the new writer, and it is fenced before it uploads a page. A seal-start record whose upload is ambiguous is settled at the same seq, like any other in-doubt record, before anything else is written.
- A seal already past that gate sends each page and manifest request only inside its lease, and each PUT is conditional (below). The new writer does not seal the generation until such a seal's requests have ended (the takeover quiet, below), also when the writer left mid-seal is several takeovers back.
- After a restart, it is fenced at open: the head names another writer id.
- Its records before the fence stay in `log/G`. Restore accepts "the old writer's records, the fence, the new writer's records" (see Bucket format), and reconcile takes them into the new writer's directory, so nothing it acked is lost.

*The lease (D13).* A writer trusts its last confirmation that the head is its own for `lease` (30 s, `DURABLE_LEASE_MS`).
- A confirmation is a confirmed log record or head write, open's head read, or a head GET that found its own head. A failed or ambiguous request confirms nothing.
- Its age is the larger of the monotonic and the wall clock's: a machine that slept (the monotonic clock stands still) re-reads the head on waking, and a wall clock stepped back never makes it younger.
- Past the lease, the next append GETs the head first. Its own head renews the lease; anything else is `Fenced`, and nothing is written. The cost: one GET on the first commit after `lease` of idle (see Costs).
- In a seal, every page and manifest PUT attempt, retries included, is sent only inside the lease. At a lapse the workers stop, the head is read (renewing the lease, or fencing the writer), and the pages not yet uploaded resume. So no seal request goes out more than `lease` after the writer last saw its own head, unless the writer is paused between a request's lease check and its send. Such a late PUT fails its condition (below), except in the residual below. A lapsed lease before the head PUT also GETs the head first, but that costs one GET and guards nothing the head's `If-Match` does not already refuse: a writer paused anywhere between its manifest PUT landing and its head PUT's send can publish its head late (see GC below).
- A record that lands more than `lease` + `retry_deadline` after the last confirmation (its writer was paused between its lease check and its PUT) counts only after a head GET finds the head still the writer's own. Before this rule, a paused old writer got 400 commits acked into a log that GC had deleted (found by the system test `paused_old_writer_is_fenced_after_resume`).

*Conditional seal PUTs.* Each page and manifest PUT has a condition:
- a key this seal has not written: `If-None-Match: *`;
- a key it wrote: `If-Match` on the ETag its PUT got back. A 412 there means someone wrote the key after it, so the head is read: a moved head fences the writer, and its own head lets the seal go on;
- after a 412 on a key it never wrote (a leftover of an earlier attempt, a crashed process or the old writer): a HEAD for the current ETag, then `If-Match` on it.

So once a new writer has written a key, a stale writer's late PUT fails its condition, except in the residual below. The normal path sends the same requests as plain PUTs.

*The takeover quiet.* When the fence lands, the new writer reads every record of the generation before it. When any other writer's last record there is an empty record (a seal-start record: a writer appends nothing once its seal has started), or a record cannot be read, that writer's seal may still be uploading. The new writer then does not seal that generation until `lease` + `request_timeout` after its fence (40 s with the defaults). The old writer sent its last request within `lease` of its last head check, which came before the takeover, and that request ends within `request_timeout`.
- Every writer counts, not only the one just before the fence: in `[A's seal-start, B's fence, B's commits, C's fence]`, A's seal may still be uploading when C takes over, so C waits too.
- The quiet is persisted with its length (`seal_not_before_ms`, `seal_quiet_ms`), so a reopen keeps it. A reopen (the CLI's `sql` or `open` after `takeover` is one, and so is a restart after a crash) that finds it still pending, with no seal of the generation since, waits its full persisted length again from the open, on the monotonic clock. It never trusts the wall-clock remainder: a wall clock stepped forward since cannot shorten the quiet, one stepped back cannot stretch it, and a reopen configured with a shorter `lease` cannot cut it. Within a process it runs on the monotonic clock only, so a wall clock step changes neither the seal's wait nor maintenance's view of it. `TakeoverReport::quiet` and the CLI's `quiet_ms` show it.
- A seal of that generation waits it out (`seal waits out the takeover quiet`). So does a close-time seal, so a close can take that long. Maintenance does not start a run meanwhile (`takeover quiet`).
- Commits can stall for what is left of the quiet, up to 40 s with the defaults: any seal of that generation waits, including the one in the commit that crosses the engine's automatic checkpoint (see "Known cost: the commit that crosses a generation boundary"). With a 2.5 s quiet, a commit after a takeover stalled 2.36 s. After a reopen the stall can be the full persisted length, since the reopen waits it again in full. A local state whose `seal_quiet_ms` is over a day (24 h, `MAX_SEAL_QUIET`, fixed) is refused at open as `Corrupt`.
- It assumes the old writer's `lease` and `request_timeout` are no larger than the new writer's. Nothing in the bucket records another process's settings.
- The cost: a takeover of a writer that crashed or was killed mid-seal holds the new writer's first seal back by up to 40 s with the defaults. Deciding it costs a LIST and one GET per log object of the generation before the fence, at each takeover (reconcile then reads them again).

*The epoch (D12).* Every writer open raises it (see Bucket format). A head PUT that a crashed process still had in flight carries the ETag from before the raise, so it gets 412 instead of publishing a generation without the commits acked since the restart.

*GC (D14).* A pass works on the bucket's own clock:
1. It GETs the head and LISTs `manifest/`.
2. It HEADs the manifests up to the head's, newest first, until one is old enough: at least `trim_grace` + `retry_deadline` + `request_timeout` + 1 s (10 min 41 s with the defaults), measured as the HEAD's `Date` − `Last-Modified`. A manifest that is gone by then (HEAD 404) is skipped. A HEAD without `Last-Modified` or `Date`, or with one it cannot parse, fails the pass with `Bucket`.
3. With manifest `m` chosen, it deletes what the head `(m + 1, m)` no longer reaches: the logs of generations up to `m`, the manifests below `m`, and the pages below `m` that manifest `m` does not map to.

More on GC:
- Why it is safe: what that head no longer reaches has been unreferenced since the head after `manifest/m` was published, within `retry_deadline` + `request_timeout` of the manifest's write; the 1 s covers the dates' resolution. So each object has been unreferenced for longer than `trim_grace`, which exceeds an old writer's `lease` + `retry_deadline` + `request_timeout` (`Config` enforces it for this process's settings). A restore of any head of the last `trim_grace` finds what it reads, with one exception. A writer paused anywhere between its manifest PUT landing and its head PUT's send can publish its head late; GC (in any process) may then at once trim what the replaced head referenced. No acked data is lost, since the new head references every sealed page, but a restore or lazy read of the just-replaced head can get `NotFound`.
- Only manifests are aged, so head rewrites (epoch raises) do not reset the clock.
- It runs in `gc` (library and CLI), and, with `gc_after_seal` (on by default), on the writer's GC thread: at open, after each seal (seals that queue up make one pass) and when the next manifest comes of age. So a process of any lifetime that opens a database sealed long ago trims it at open.
- On the writer's thread, a pass whose chosen manifest is the one it last trimmed against stops there, before the manifest GET and the LISTs of the trim. A closing writer skips the pass for its last seal; the next open or `gc` trims it.
- Against another process's old writer, the grace protects only if this process's `trim_grace` also exceeds that writer's `lease` + `retry_deadline` + `request_timeout`. The same settings on every process satisfy it; nothing enforces it.

**The limits stage 2 listed, closed or not.** The bucket format stays v1: the head gains `epoch` and the log may hold fence records, and nothing else changes.

Stage-2 binaries must not run against a prefix a stage-3 writer has touched. A stage-2 writer has no `confirm_landed` (a record that lands late counts without a head GET), and a stage-2 restore refuses fence records.

| Stage-2 limit | Now |
|---|---|
| An old writer's page PUTs still in flight during a takeover | Closed by the lease (every page and manifest request is sent inside it), the takeover quiet and conditional PUTs, except the residual below. |
| A suspended or paused old writer | Closed, except the residual below (a seal PUT sent after a pause between its lease check and its send): the lease runs on two clocks, every request is lease-checked, and a record landing late counts only after a head GET. Tested with SIGSTOP (`paused_old_writer_is_fenced_after_resume`). |
| A crash between the claim's head write and the local-state write | Closed: the claim id is persisted before the head write, and the next open resumes the takeover. |
| Short-lived processes never trim | Closed: GC on the bucket's clock at open, after seals, and in `gc`. |
| A head PUT in flight at a crash | Closed by the epoch: every writer open raises it, so the late PUT's `If-Match` gets 412. |
| Between the restore and the claim record, the old writer can win a seq race | Closed: the fence record steps over the old writer's records, and reconcile takes them in. |

**The residual (known, accepted).** On S3 and RustFS a single-part PUT's ETag is the MD5 of its body, so equal bytes get equal ETags. A late PUT of old writer A lands over new writer B's object when:
- A was paused for longer than the takeover quiet, between its lease check and the send of a `put_if_match` on an ETag it already held;
- and B's page (or manifest) is byte-identical to what A conditioned on.

A is fenced at its next request, but the object it overwrote stays. A possible future fix: a takeover could seal into a generation number the old writer never used, so B never writes keys under A's generation G, and A's late conditional PUTs cannot touch anything B references. It needs a change to how generations are numbered at takeover, which is not in this stage.

**Test and diagnostics hooks.** `Config::on_event`, `turso_durable::Event` and `turso_durable::EventHook` are `#[doc(hidden)]`. They exist for the tests and for the CLI's `soak` event lines, and are not part of the supported API: they may change without notice.

**Known cost: the commit that crosses a generation boundary.**
- Pages are uploaded at the generation boundary (D6): the write that restarts the WAL. After an automatic checkpoint, or a `Passive`, `Full` or `Restart` checkpoint, that write happens in the next commit, which waits for the whole generation's page upload, the manifest and the head. `CheckpointMode::Truncate` and close seal inside the checkpoint instead.
- Stage 1: with turso's automatic checkpoint (every 1000 WAL frames), the bench crossed a boundary about every 500 commits of 1 KB.
- Stage 1: in the 5000-commit bench, the commit p50 was 1.33 ms, while the slowest commit took 69.8 ms. That commit is most likely the one that crossed the first boundary (bench does not record which commit triggered a seal), whose seal took 39.4 ms for 169 pages.
- Stage 1: each in-run seal took 37–44 ms for 169–172 pages.
- You can move this wait off a latency-sensitive path by calling `Connection::checkpoint(CheckpointMode::Truncate)` at a quiet moment.
- A maintenance checkpoint seals inside its own TRUNCATE too. No commit carries that seal; new statements wait at the gate while it runs instead (see "Maintenance checkpoint" for the bound).

**Limits.**
- Without `prefetch`, a restored database fetches pages lazily in every process that opens it, until a prefetch completes. Each process fetches each page it reads once. Only a finished prefetch in a writer open marks the directory `complete`.
- `close` waits for a GC pass in progress. A short-lived process that opens a database with a large backlog starts a long pass at open (5095 deletes took 7.0 s on the laptop, see Costs), and its close waits for it. Set `gc_after_seal` off for such processes and run `gc` elsewhere.
- `gc` does not retry a failed bucket call; run it again. A writer's GC thread retries a failed pass at its next seal or after 30 s.
- Memory use grows with the WAL. A seal reads the whole local `-wal` into memory and holds every dirty page of the generation while it uploads them. A WAL that grows large makes the seal large too. The maintenance checkpoint seals once the `-wal` reaches `maintenance_wal_bytes` (64 MiB by default), but a transaction held open past `maintenance_gate_wait` keeps it from running (it retries with backoff). `CheckpointMode::Truncate` at quiet moments keeps generations small.
- Never copy a live database directory (`cp -r`, a VM or disk clone). The copy has the same writer id, so fencing cannot tell the two processes apart. Restore into a new directory instead.

**Edge cases.**
- `PRAGMA wal_checkpoint` reports an upload failure as a busy row, not as an error (vdbe/execute.rs:796-803). Use `Connection::checkpoint` to get the error.
- `close` ignores a failed close-time checkpoint (pager.rs:5458-5472). Nothing acked is lost: the log already holds every acked commit, and the next open or restore replays it.
- If the local fsync fails, the engine panics (pager.rs:4723-4733). This is a local disk failure. For an Off commit the upload was already confirmed, so the commit is in the bucket; restore the database elsewhere. A Window commit is queued before its fsync, so its upload may not have happened yet: after a restore elsewhere it may or may not be there, and it was never acked.
- VACUUM is untested.
- The figures here are from RustFS on a laptop; nothing has been measured on real S3.
- Closing during a bucket outage can block for up to the retry deadline.

## Costs (measured)

### Stage 3: takeover, lease and GC

Source: `results/writer-lease.txt`. These are laptop figures (Apple M4 Max, RustFS in podman on the same Mac, release build), not production numbers, with telemetry off. The file holds two runs of the same code: this table quotes run 2, and run 1 (an earlier round) is kept raw beside it; each measure ran once per run. The database is the 5000-commit bench database of stage 1's workload (generation 13 at the takeover). Request counts come from `crates/turso-durable-cli/examples/stage3_costs.rs`, which wraps the S3 store in a counter.

| Measure | Result |
|---|---|
| Takeover (`takeover`, into an empty directory) | 67 ms in all, of which the restore took 18 ms with 4 GETs (`3 + 1` log object) and 1 LIST. 2 PUTs in all: the head (`If-Match`) and the fence record (`If-None-Match: *`). 10 GETs and 3 LISTs in all: the restore's, then one head GET before the head write, the attach (head, manifest, LIST), the record before the fence (for the quiet), and reconcile's LIST and log GETs. Measured before the final fix wave, which reads every record before the fence for the quiet: one LIST more here, and one GET per log object of the generation in general. `quiet_ms=0`: the record before the fence was the bench's last log record, not a seal-start record. A second takeover, through the CLI, also took 67 ms. |
| Writer open of the taken-over directory | 1 PUT (the epoch raise), 7 GETs and 2 LISTs, 16–17 ms, with `DURABLE_GC_AFTER_SEAL=off` so the GC thread's requests are not counted. Only the raise is new: stage 2's open code made the same GETs and LISTs (not measured). |
| A commit right after another | 1 PUT (its log record), no GET. |
| A commit after a 31 s pause (default 30 s lease) | 1 PUT and 1 GET: exactly one extra head GET. |
| `gc` with the defaults, right after | `outcome=too_young` (the oldest manifest was 39 s old), 6 ms. |
| `gc` after 10 s, with `DURABLE_TRIM_GRACE_SECS=5`, `DURABLE_LEASE_MS=1000`, `DURABLE_RETRY_DEADLINE_MS=1000`, `DURABLE_REQUEST_TIMEOUT_MS=1000` (minimum age 8 s) | Trimmed against manifest 13: `deleted=5095` in 7035 ms, one DELETE at a time. A second pass deleted 0 in 165 ms. Afterwards a fresh restore passed `verify` (`PRAGMA integrity_check`), and `sql` on the writer's own directory counted 5000 rows in `bench`. |
| The 5000-commit bench itself | Commit p50 / p99 1.29 / 2.09 ms, 679 commits/s, 6772 PUTs in all of which 1720 page PUTs. Each in-run seal of 170–172 pages took 44–54 ms (stage 1: 37–44 ms). Its page and manifest PUTs are now conditional; in the normal path they are the same requests as plain PUTs, with a condition header. An earlier round of the same code measured 1.20 / 1.76 ms, 724 commits/s and 44–50 ms per seal. |

- The takeover quiet was not exercised here. When it applies (the old writer's last record is a seal-start record), it holds the new writer's first seal of that generation back for `lease` + `request_timeout`, 40 s with the defaults.
- GC's request count grows with the bucket: per pass one head GET, one LIST of `manifest/`, one HEAD per manifest it ages, then the manifest GET, the LISTs of `log/`, `manifest/` and `pages/`, and one DELETE per object.

### Stage 2: batching

The maintenance checkpoint's costs are under Batching, "Maintenance checkpoint".

Source: `results/batching-knob.txt`. These are laptop figures (Apple M4 Max, RustFS in podman on the same Mac), not production numbers. Each run made 2000 commits of `INSERT OR REPLACE` with a 1 KB blob, on 1 or 8 connections, with telemetry off (`DURABLE_OTEL` unset). There was one run per row, so small differences are noise.

| Connections | Batching | Commit p50 / p95 / p99 | Commits/s | Log PUTs per acked commit | `commits_per_log_put` | Ack wait p50 / p95 / p99 |
|---|---|---|---|---|---|---|
| 1 | `off` | 1.19 / 1.60 / 2.39 ms | 721 | 1.002 | 1.000 | 1.15 / 1.34 / 1.66 ms |
| 1 | `window 1ms 64 1MiB` | 2.59 / 3.04 / 3.35 ms | 363 | 1.000 | 1.002 | 2.56 / 3.07 / 3.33 ms |
| 1 | `window 10ms 64 1MiB` | 16.8 / 19.6 / 20.7 ms | 60 | 1.000 | 1.002 | 17.4 / 20.5 / 21.5 ms |
| 1 | `window 50ms 64 1MiB` | 63.0 / 67.8 / 69.1 ms | 16 | 1.000 | 1.002 | 63.5 / 69.6 / 69.6 ms |
| 8 | `off` | 1.32 / 1.75 / 3.45 ms | 631 | 1.002 | 1.000 | 1.28 / 1.47 / 1.73 ms |
| 8 | `window 1ms 64 1MiB` | 5.02 / 15.0 / 49.1 ms | 1038 | 0.323 | 3.10 | 3.20 / 4.35 / 4.86 ms |
| 8 | `window 10ms 64 1MiB` | 15.3 / 33.7 / 74.1 ms | 394 | 0.166 | 6.03 | 12.8 / 19.5 / 21.5 ms |
| 8 | `window 50ms 64 1MiB` | 62.7 / 70.0 / 75.0 ms | 124 | 0.127 | 7.91 | 57.3 / 69.6 / 73.7 ms |

- "Log PUTs per acked commit" is `log_puts` / 2000 bench commits. `upload_stats` itself counted 2002–2005 commits per run, because each WAL restart's header record counts as a commit. The ack-wait columns come from the histogram since open, so they include the setup `CREATE TABLE`'s one ack.
- Off makes one log PUT per acked commit with 1 or 8 connections (1.002: one per commit, plus one per WAL restart).
- One connection gains nothing from a window: one commit per PUT, and every commit waits out the window.
- With 8 connections, a window shares PUTs: 3.1, 6.0 and 7.9 commits per PUT at 1, 10 and 50 ms. Each connection has at most one commit waiting, so a PUT held at most about 8 here. Only the 1 ms window also raised throughput over Off (1038 against 631 commits/s).
- At 10 and 50 ms, acks waited several ms longer than the window plus one PUT. The cause was not investigated. A review found no cause in the code; the excess grows with the window (about 0.4, 5.6 and 11.8 ms at 1, 10 and 50 ms), which fits macOS timer coalescing on the uploader's `recv_timeout` (a hypothesis, unverified).
- Checkpoint uploads: with one connection, each in-run seal took 41–45 ms (Off) and 43–75 ms (Window) for 170 pages. With 8 Window connections, no WAL restart happened during the run, so the run's 696–728 pages were sealed once, at close, in 177–186 ms. These runs predate the maintenance checkpoint, which now seals such a generation during the run (see "Maintenance checkpoint").
- Restore of the 1-connection Off database (generation 6): 24 ms, 4 GETs (`3 + 1` log object), 1 LIST, 4.4 KB downloaded. A full `verify` of the fresh restore took 0.23 s of wall time.

### Stage 1

Source: `results/durable-core.txt`. These are laptop figures (Apple M4 Max, macOS, RustFS 1.0.0 in podman on the same Mac, release build), not production numbers. The workload was one connection running `INSERT OR REPLACE` of a 1 KB blob per commit.

| Measure | 500 commits | 5000 commits |
|---|---|---|
| Commit p50 / p95 / p99 | 1.28 / 1.79 / 2.83 ms | 1.33 / 1.69 / 2.05 ms |
| Slowest commit | 3.7 ms | 69.8 ms (commit #502, most likely at the first boundary) |
| Commits per second | 744 | 674 |
| Log PUTs per acked commit | 1.000 | 1.002 (one per commit, plus one per WAL restart) |
| Seals | 1 (at close) | 11 (10 in the run, 1 at close) |
| Page PUTs per seal | 169 | 169–172 in the run, 8 at close |
| Seal time | 44 ms | 37–44 ms in the run, 4.4 ms at close |

These figures predate the seal-start record, the lease and the conditional seal PUTs. Each seal now also makes one extra log PUT and one head GET before its page PUTs. The first commit after `lease` of idle (30 s by default) makes one head GET; the bench never idles that long. Stage 3's bench figures are above.

Restoring the 5000-commit database (generation 12, 1673 pages, 6.9 MB):
- The restore itself took 33 ms. It made 4 GETs (`3 + 1` log object) and 1 LIST, and downloaded 4.6 KB.
- A full `verify` of a fresh restore (`PRAGMA integrity_check`, which reads every page lazily) took 0.52 s of wall time, including process start.

## Decisions

D1–D10 are the design decisions behind stage 1; their evidence is turso_core 0.8.3 from crates.io, with paths relative to its crate root. D11–D14 are stage 3's, and D15–D16 are stage 4's; their evidence is this repository's code, except D15's, which is reqwest 0.13.2, hyper-rustls 0.27.10 and tokio-rustls 0.26.6 from crates.io, and D16's, which is turso_core 0.8.3 as well.

| # | Decision | Evidence |
|---|---|---|
| D1 | Use turso_core `=0.8.3` from crates.io with features `fs`, `json`, `uuid`; `fs` alone does not build. The `encryption` feature is on (audit row 3); reqwest's `rustls`/`default-tls` (aws-lc) features need a re-audit. | Newest stable release, vcs `607bff9fd`. |
| D2 | Use only public APIs: `IO`/`File`, `DatabaseStorage`, `OpenOptions::storage`, `Database::open`/`connect`/`mvcc_enabled`, `Connection::prepare`/`checkpoint`/`close`/`set_sync_mode`, and `turso_parser`. D16 adds turso's encryption API (`DatabaseOpts::with_encryption`, `OpenOptions::db_opts`/`encryption`, `Database::connect_with_encryption`, `CipherMode`, `EncryptionKey`, `EncryptionContext::decrypt_page`). | io/mod.rs:424-484, 153-214; storage/database.rs:106-132; database.rs:240, 254, 1216, 2383, 3445; connection.rs:1049, 2380, 2439, 4012. |
| D3 | A failed upload fails the commit without the fsync panic. The WAL file's `sync` uploads before it creates any completion and returns `Err` synchronously, so the engine rolls the commit back. | pager.rs:4711-4733, 4486-4488; vdbe/mod.rs:3400-3409, 3609-3612; wal.rs:4271-4278. |
| D4 | One process. An explicit exclusive lock on `durable.lock` gives `Locked`, and a process-wide registry gives `AlreadyOpen`. The multiprocess WAL is never enabled. | database.rs:80, 876-879, 1288-1305; io/unix.rs:67-71, 278-297. |
| D5 | MVCC is refused. The SQL guard refuses the PRAGMA before the engine writes the 255/255 header, and open checks the header and `mvcc_enabled()`. `.db-log` is refused as a backstop. Other journal modes and ATTACH are refused too. | sqlite3_ondisk.rs:200-228; vdbe/execute.rs:19165-19192; journal_mode.rs:19-27, 90-95. |
| D6 | Pages upload when the WAL restarts (the generation boundary), not at each checkpoint. A seal uploads the union of the in-memory dirty set, the persisted `app.db-dirty`, and every page in the local WAL's frame headers. It first appends an empty seal-start record and checks the head (`Bucket::seal_start`), then uploads the pages, then writes the manifest, then moves the head with `If-Match`. | wal.rs:5171-5180, 5289-5310, 6024-6050; pager.rs:4514-4516, 5458-5472. |
| D7 | `synchronous` is FULL only. The guard refuses lower values, and each statement is checked after `prepare`. | lib.rs:225-229; pager.rs:4711; translate/pragma.rs:662-676. |
| D8 | Typed errors cross the engine through a thread-local stash. `Connection::checkpoint` uses the engine call that returns `Err`, because the PRAGMA reports busy. | vdbe/execute.rs:796-803; connection.rs:2432-2434. |
| D9 | On restart on its own disk, the writer reconciles the local WAL with the bucket's generation image: level, behind (take the bucket's), ahead (upload the difference), or a new generation. A different writer id gives `Fenced`. | `crates/turso_durable/src/reconcile.rs` |
| D10 | Trimming deletes older logs, manifests and superseded pages, and never touches a generation at or above the head's manifest. When it runs, and against which head, is D14's choice. | `crates/turso_durable/src/trim.rs` |
| D11 | Takeover and the claim of a restored copy are one mechanism: `takeover`, a writer `open` of an empty or restored directory, and the CLI's `takeover` and `sql --claim` all write the head with a new writer id at the next epoch (`If-Match`), then append a fence record at the next free seq. The head goes first: only one of two racing takeovers wins an epoch's `If-Match`, so only that epoch's winner writes its fence, and restore checks fences strictly. Records the old writer landed meanwhile are stepped over and taken in by reconcile. The claim id is persisted first, so a takeover resumes after a crash. | `crates/turso_durable/src/takeover.rs`; `db.rs` (`claim`, `attach_existing`); `bucket.rs` (`take_over_counted`, `append_fence`) |
| D12 | The head carries an epoch (absent = 0). A new database starts at 1; a takeover writes the next one with its new writer id; every other writer open, and a writer that finds its ambiguous seal head write did not land, raises it keeping its id. A late head PUT then fails its `If-Match`. A writer open is refused while the head names another writer or an epoch below the directory's. Cost: one head PUT per writer open. | `crates/turso_durable/src/format/head.rs`; `bucket.rs` (`raise_epoch`, `check_head`, `attach`) |
| D13 | A lease replaces stage 2's idle check: 30 s on the larger of the monotonic and the wall clock. Past it, an append GETs the head first; a seal sends each page and manifest PUT attempt only inside it (checked just before the send, so a pause between the check and the send gets one request out late; see the residual under Guarantees); a record that lands more than `lease` + `retry_deadline` after the last confirmation counts only after a head GET. Seal PUTs are conditional. A fence before which another writer's last record is a seal-start record starts the takeover quiet (`lease` + `request_timeout`), persisted in the directory with its length, waited again in full at a reopen that finds it pending, and waited out on the monotonic clock only. A lapsed lease before a seal's head PUT GETs the head first (one GET; it does not stop a late head, see GC under Guarantees). | `crates/turso_durable/src/clock.rs`; `bucket.rs` (`check_lease`, `confirm_landed`, `retrying_in_lease`, `seal_put_step`, `set_seal_not_before`); `db.rs` (`set_quiet`) |
| D14 | GC runs on the bucket's clock: it ages manifests by HEAD (`Date` − `Last-Modified`), takes the newest one up to the head's that is `trim_grace` + `retry_deadline` + `request_timeout` + 1 s old, and trims against the head that followed it. It runs in `gc`, and on the writer's thread at open, after seals and when the next manifest comes of age. `trim_grace` must exceed `lease` + `retry_deadline` + `request_timeout`, with no exception for 0. | `crates/turso_durable/src/gc.rs`, `trim.rs`, `store/s3.rs` (`stat`), `config.rs` (`validate`) |
| D15 | HTTPS goes through reqwest's `rustls-no-provider` with the ring provider installed by `http_client` (a host app's earlier install wins), because reqwest panics at `build()` without a provider. Without `DURABLE_CA_BUNDLE` it uses the platform verifier; with it, `tls_certs_only` makes the bundle the only roots (webpki). `http://` passes through the same connector. A certificate failure sits two `io::Error`s deep in the cause chain (`io::Error::source` skips the error it wraps), so `tls_failure` descends them with `get_ref`; `InvalidCertificate`, `NoCertificatesPresented` and `InvalidCertRevocationList` are `Permanent`. | reqwest-0.13.2 Cargo.toml:169,174; async_impl/client.rs:2458-2464 (no-provider panic), :751-753 (platform verifier), :780-783 (certs-only webpki); blocking/client.rs:812; tls.rs:195; hyper-rustls-0.27.10 src/connector.rs:88-92,126-132 (http passes through); tokio-rustls-0.26.6 src/common/mod.rs:115 |
| D16 | Encryption is turso_core's own page codec, set by a local key file and a cipher. turso needs all three of its switches: `DatabaseOpts::with_encryption(true)` (the pager refuses a key without it), `OpenOptions::encryption(EncryptionOpts { cipher, hexkey })` at open (the database's cipher, and a new database's reserved bytes), and `Database::connect_with_encryption(Some(key))` on every connection, since each connection builds its own pager from the key it is given. `EngineDb::connect` makes every engine connection (`Database::connect` and the maintenance checkpoint); without a key it passes `None`, as turso's plain `connect()` does. The transform sits in turso's `DatabaseFile` and WAL, below `BucketDbStorage` and `ShippingFile`, so seals, log records and lazy fetches carry ciphertext with no change of their own. Page 1 is checked before a writer open writes to the bucket and again before the engine opens: a plaintext database cannot take a key (its header has no reserved bytes for the nonce and tag), and a missing, wrong or other-cipher key is a clear `Encryption` error instead of turso's `Engine("File is not a database")` or `Engine("Decryption failed for page=1")`. The guard refuses `PRAGMA hexkey` and `PRAGMA cipher`. Restore copies bytes and needs no key; `takeover` with a key checks it on the restored page 1 before its head write, so an operator's wrong key cannot fence the running writer. | turso_core Cargo.toml:48; storage/encryption.rs:143-170 (key redacted, zeroed), 668-682 and 819-865 (page 1's header), 728-1031 (encrypt and decrypt, `encryption` feature only); database.rs:108-111, 185-188, 277-280, 748-756, 1486-1490, 2383-2395, 2465-2495, 3119-3180; storage/pager.rs:6146-6152; storage/database.rs:147-232, 279-330; storage/wal.rs:3204-3260; translate/pragma.rs:644-655. This repository: `crates/turso_durable/src/encryption.rs`; `engine.rs` (`EngineDb`, `open_engine`); `db.rs` (`refuse_encryption_mismatch`); `takeover.rs` (`takeover_with_store`); `restore.rs` (`restore_staged`'s page 1); `guard.rs` (`check_pragma`) |

## Test command

- `./scripts/test.sh unit`: fmt, clippy (`-D warnings`) and unit tests. These include the in-memory takeover tests in `crates/turso_durable/tests/fencing.rs`, each under Off and a Window setting.
- `./scripts/test.sh`: the above, then empties and starts RustFS (`scripts/rustfs.sh reset`) and runs the RustFS-backed `--ignored` tests with `--test-threads=1`. These include the CLI system tests (crash matrix, faults, restart, second process, MVCC, trimming, the same under Window batching, batching stats, the maintenance checkpoint under sustained Window load and a crash in the middle of one, and collector-down runs). Stage 3 adds: a takeover of a running `soak` under Off and under a Window setting (`takeover_fences_a_running_soak`, `takeover_window_fences_a_running_soak`), an old writer paused with SIGSTOP across a takeover and GC (`paused_old_writer_is_fenced_after_resume`), `gc` after short-lived processes (`gc_cli_trims_after_short_lived_processes`), Window crash rows for PASSIVE, FULL and RESTART, and the S3 test `stat_reports_etag_and_age`.
  - It runs them with `DURABLE_OTEL=1` under one `DURABLE_TEST_RUN_ID` (`turso-durable-<timestamp>-<pid>`, printed as `test run id: …`).
  - Then it runs a short workload under that run id (`bench --commits 50 --batching "window 5ms 8 1MiB" --connections 4`, then `restore`, `takeover` and `gc`), so every span kind except `durable.maintenance` is exported.
  - It ends with the ClickHouse check, `scripts/otel-check.sh <run id>` (see Telemetry). The check prints `otel check PASSED: …` or `otel check FAILED: …`, or `otel check SKIPPED: observability stack not running` when the stack is down, which still passes.
  - On exit, pass or fail, it empties RustFS's storage again (`scripts/rustfs.sh reset`). A failed reset is reported on stderr and the exit code stays the tests'.
  - Peak disk of a full run (Task 9's measurement): 1010 MB, against stage 2's 779 MB, mostly RustFS's trash of deleted objects, which it keeps until it restarts.
  - Each RustFS-backed test deletes its bucket prefix when it ends. `DURABLE_KEEP_FAILED=1` keeps a failed test's prefix and prints its name on stderr (until the next `test.sh` run resets the storage).
- `scripts/rustfs.sh up|down|reset|stop|start|pause|unpause|env` manages the local RustFS container (`turso-durable-rustfs`, port 9290). `down` removes the container and its volume `turso-durable-objects`; `reset` is `down` then `up`, an empty bucket. `eval "$(scripts/rustfs.sh env)"` sets the `DURABLE_*` variables for it.
- Always pass `--locked` to cargo. On macOS the script sets `SDKROOT`, and it sets `SOURCE_DATE_EPOCH`.

## Layout

- `crates/turso_durable`: the library.
- `crates/turso-durable-cli`: the `turso-durable` binary, the system tests and `examples/stage3_costs.rs` (the stage-3 request counts).
- `scripts/`: `test.sh`, `rustfs.sh` (local S3), `s3-gate.sh`, `otel-check.sh` (the ClickHouse check).
- `results/`: `rustfs-gate.txt` (conditional-PUT gate), `durable-core.txt` (stage 1 measurements), `batching-knob.txt` (stage 2 measurements) and `writer-lease.txt` (stage 3 measurements).
- `LICENSES/`: third-party licences.

## Provenance

- Built on turso_core 0.8.3 (MIT).
- Parts of the IO layer, the object store, retry and restore code are derived from the tursodatabase/turso `spike/objstore` branch, commit `417bd81c0` (MIT). Each such file starts with `// Derived from tursodatabase/turso spike/objstore (MIT), commit 417bd81c0.`
- turso's licence is in [`LICENSES/turso-MIT.md`](LICENSES/turso-MIT.md).

## Dependency audits

Always build with `--locked`. Any `Cargo.lock` change needs a re-audit through the rust-supply-chain-auditor.

| # | Date | Scope | Crates and versions | Verdict |
|---|------|-------|---------------------|---------|
| 1 | 2026-10-16 | turso-durable workspace (turso_durable, turso-durable-cli): full resolved `Cargo.lock` (sha256 `ff8bc95a6ecea426e0d43a5db3601c23f1c6d3a0ffc101f83d1d620a5b22539e`) | 261 crates.io packages, no git/path deps (201 compiled on aarch64-apple-darwin). Direct: turso_core =0.8.3 (features `fs`,`json`,`uuid`; vcs 607bff9fd, published by penberg 2026-10-12), turso_parser =0.8.3, reqwest =0.13.2 (`blocking`, no TLS crates in tree), hmac =0.13.0, sha2 =0.11.0, hex =0.4.3, dev tempfile =3.20.0. Notable transitive: aegis 0.9.20 (pure-rust on macOS, so no C build), aristo/aristo-macros 0.4.1 (aretta-ai; macros only validate and pass items through with default features), uuid 1.26.1 + sha1_smol 1.0.1, syn 3.0.6 (via bytemuck_derive 1.12.0), tokio 1.53.1, hyper 1.11.1, crossbeam-epoch 0.9.21. No sqlite/libsql. | APPROVED. cargo-audit (RustSec db 479c888c, 2026-10-16): 0 vulnerabilities, 0 warnings. The earlier known advisories (anyhow 1.0.98, crossbeam-epoch 0.9.18, memmap2, lru, rkyv) are not in this tree. Lock checksums match crates.io; publishers are the expected maintainers or trusted publishing. Build scripts and proc-macros were reviewed: no network, no credential paths, no obfuscation. |
| 2 | 2026-10-17 | OTLP traces+logs over gRPC/tonic (no TLS): full resolved `Cargo.lock` (sha256 `7b6cedde8c53e01af4ebe4c58636e7349a765a60af2095244263fdc7f9678df1`) | 290 packages (27 added, none removed; no existing version changed). Direct: tracing =0.1.44, tracing-subscriber =0.3.23 (`registry`, `std`, no default features), opentelemetry =0.31.0, opentelemetry_sdk =0.31.0 (`trace`, `logs`; dev adds `testing`), opentelemetry-otlp =0.31.1 (`grpc-tonic`, `trace`, `logs`), tracing-opentelemetry =0.32.1 (no default features), opentelemetry-appender-tracing =0.31.1, tokio =1.53.1 (`rt-multi-thread`, `net`, `time`). Notable transitive: tonic 0.14.6 + tonic-prost (`channel`, `codegen`; no `tls*`), prost 0.14.4, h2 0.4.20, opentelemetry-proto 0.31.0 (pre-generated, no protoc), anyhow 1.0.104 (the only new build.rs). No rustls, ring, aws-lc, openssl, native-tls, sqlite/libsql. | APPROVED (tracing-subscriber pinned to =0.3.23 to match turso_core's resolution). cargo-audit (RustSec db 479c888c, 2026-10-16): 0 vulnerabilities, 0 warnings. |
| 3 | 2026-10-18 | HTTPS through rustls (ring) and turso_core `encryption`: full resolved `Cargo.lock` (sha256 `e8252f5f8f31d64a3ccdf79c672b47a267c8623b693146a840330ebc3f6a03bc`) | 328 packages (38 added, none removed; no existing version changed). Direct: rustls =0.23.46 (`std`, `tls12`, `ring`; no default features), reqwest =0.13.2 gains `rustls-no-provider`, turso_core =0.8.3 gains `encryption` (the cipher crates are already in row 1). Compiled on aarch64-apple-darwin: rustls 0.23.46, rustls-webpki 0.103.15, rustls-pki-types 1.15.1, ring 0.17.14, untrusted 0.9.0, hyper-rustls 0.27.10, tokio-rustls 0.26.6, rustls-platform-verifier 0.6.2, webpki-root-certs 1.0.9, zeroize 1.9.1, core-foundation 0.10.1, security-framework 3.7.0, security-framework-sys 2.17.0. Lock-only (Linux, Android, Windows targets): rustls-native-certs 0.8.5, openssl-probe 0.2.1, walkdir 2.5.0, same-file 1.0.6, winapi-util 0.1.11, rustls-platform-verifier-android 0.1.1, jni 0.21.1, jni-sys 0.3.1 and 0.4.1, jni-sys-macros 0.4.1, combine 4.6.9, cesu8 1.1.0, thiserror and thiserror-impl 1.0.69, schannel 0.1.30, windows-sys 0.45.0 and 0.52.0, windows-targets 0.42.2 and windows_{aarch64_gnullvm,aarch64_msvc,i686_gnu,i686_msvc,x86_64_gnu,x86_64_gnullvm,x86_64_msvc} 0.42.2. No aws-lc-rs, aws-lc-sys, cmake, openssl, native-tls. | APPROVED. rustls is pinned to 0.23.46 because 0.23.45 has RUSTSEC-2026-0348 and RUSTSEC-2026-0349. cargo-audit (RustSec db 999ec89d, 2026-10-17): 0 vulnerabilities, 0 warnings. |

Conditions:
- turso_core 0.8.3 does NOT build with only `["fs"]`. `incremental/dbsp.rs` uses `uuid` without a feature gate, and `vdbe/execute.rs` has a type error in its non-json path. The minimal feature set is `default-features = false, features = ["fs", "json", "uuid"]` (the `encryption` feature is on, row 3; reqwest's `rustls`/`default-tls` (aws-lc) features need a re-audit). Verified with `cargo check --offline --locked --workspace --all-targets`. This adds uuid 1.26.1 and sha1_smol 1.0.1, both reviewed and clean.
- Copy the audited `Cargo.lock` with the manifests unchanged, and always build/test with `--locked`. Any lock change (cargo update, new dep or feature) needs a re-audit.
- turso_core's build.rs runs `git rev-parse`/`git show` only to embed a commit hash and timestamp. Set `SOURCE_DATE_EPOCH` for reproducible builds, which also skips the git call.
- Cargo.lock lists cfg-gated crates that normal builds never compile (cfg(antithesis/loom/shuttle): antithesis_sdk 0.3.0, loom, shuttle, generator) plus wasm/windows targets. antithesis_sdk 0.3.0 was not downloaded or reviewed; do not build with `--cfg antithesis`.
- aegis 0.9.20 ships prebuilt wasm static libs (`wasm-libs/*.a`) that are linked only for wasm32. Re-audit before targeting wasm.
- Row 2: no TLS feature on opentelemetry-otlp or tonic, and no `http-proto`, `http-json`, `reqwest-*`, `hyper-client` or `metrics`, nor the opentelemetry_sdk features `rt-tokio`, `experimental_*` or `spec_unstable_*`, in normal dependencies; `opentelemetry_sdk/testing` stays dev-only. Set the OTLP endpoint in code (it beats `OTEL_EXPORTER_OTLP_*ENDPOINT`; `OTEL_EXPORTER_OTLP_*HEADERS` are still merged in, so never log the exporter config). Keep the targets `opentelemetry*`, `tonic`, `h2`, `hyper`, `tower` and `reqwest` out of the log bridge. web-time and the wasm paths of the new crates were not reviewed.
- Row 3: never enable reqwest's `rustls`, `default-tls`, `rustls-tls` or `native-tls` features (aws-lc, openssl), nor rustls's `aws_lc_rs` or `fips`. ring is the only crypto provider and is installed explicitly. Any bump of rustls, rustls-webpki or ring needs a re-audit.
