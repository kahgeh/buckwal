# turso-durable

Make a tursodb database durable in an S3 bucket. A Rust library (`turso_durable`) and a CLI (`turso-durable`).

Your app keeps a normal local database directory. Every commit is uploaded to the bucket before it returns, so the commit survives the loss of the machine. A new machine can restore the database from the bucket alone.

This is stage 1: one writer process, plain HTTP, tested against RustFS on a laptop (not on AWS S3).

## How it works

- **Commits.** Each commit writes WAL frames. Before the WAL's fsync, the library uploads the bytes written since the last fsync as one log object (`log/<generation>/<seq>`). Only when that object is in the bucket does the commit return `Ok`.
- **Generations.** When the engine restarts its WAL after a checkpoint, the old WAL becomes a finished *generation*. The library then *seals* it. It first appends an empty *seal-start* log record and checks that `head` has not moved, so a writer that was taken over stops before it uploads anything. Then it uploads every page that changed in that generation (`pages/…`), then a manifest that maps each page to the generation holding its latest copy, then moves `head` to the next generation.
- **Restore.** Restore reads `head`, the newest manifest and the current generation's log. It rebuilds the WAL from the log and fetches pages from the bucket only when the engine first reads them.
- **Trimming.** After each seal, a background thread deletes log objects, manifests and pages that the new head no longer needs. It waits for the trim grace period first.

## Use

The library is not on crates.io. Add it by path or git:

```toml
[dependencies]
turso_durable = { path = "…/crates/turso_durable" }
```

```rust
use turso_durable::{CheckpointMode, Config, Value};

fn main() -> turso_durable::Result<()> {
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
| `open(Config) -> Result<Database>` | Opens `config.dir` as the database's one writer (see the table below). On an empty or restored directory it takes the database over (see the warning below). |
| `is_claimed(dir) -> Result<bool>` | `true` when `dir` already holds a writer's own database, so `open` continues it. `false` for a missing or empty directory, or one restored and never opened, where `open` would take the database over. It reads only local files. |
| `Database::connect() -> Result<Connection>` | A new connection. Open one per thread. |
| `Database::generation()` | The generation the bucket's head is at, as this writer last saw it. |
| `Database::bucket_stats()` | PUTs, GETs, LISTs, DELETEs and bytes sent so far (`store::StatsSnap`). |
| `Database::close()` | Waits for pending uploads, stops the trimmer, releases the directory. It fails with `Config` while a connection is open. Dropping the last handle and connection also closes it. |
| `Connection::execute(sql, params) -> Result<u64>` | Runs one statement and returns the number of changed rows. `sql` must hold exactly one statement. |
| `Connection::execute_batch(sql)` | Runs several statements, with no parameters. If the guard refuses any statement, none of them runs. |
| `Connection::query(sql, params) -> Result<Vec<Vec<Value>>>` | Runs one statement and collects its rows. |
| `Connection::transaction(\|tx\| …)` | `BEGIN`, your closure (`tx.execute`, `tx.query`), then `COMMIT`. It runs `ROLLBACK` if the closure or the commit fails. |
| `Connection::checkpoint(CheckpointMode)` | `Passive`, `Full`, `Restart` or `Truncate`. `Truncate` ends the generation inside the call: its pages upload (a seal), and an upload failure is returned as an error. `Restart` only resets the WAL in memory, so the seal happens in the next commit, which also gets any upload error. |
| `Connection::close()` | Closes the connection. The writer's last connection runs a TRUNCATE checkpoint, which seals. Dropping a connection also closes it. |
| `restore(&Config, dir) -> Result<RestoreReport>` | Restores the bucket's current state into `dir`, which must hold no database. It writes nothing to the bucket. `config.dir` is ignored; `dir` is used. |
| `Value` | `turso_core::Value`, re-exported. |

What `open` does depends on the directory:

| Local directory | Bucket prefix | `open` |
|---|---|---|
| empty or missing | no `head` | Creates a new database with a new writer id. The head starts at generation 1. |
| empty or missing | has a `head` | Restores into the directory, then claims it (see Guarantees). |
| restored by `restore`, never opened | — | Claims it, then continues. |
| this library's directory | — | Continues it. It first reconciles the local WAL with the bucket (D9). |
| an `app.db` this library did not create | — | `Error::Config`. An existing SQLite or tursodb file cannot be adopted. |

**Warning: opening a restored or empty directory as the writer takes the database over.** `open` claims the database under a new writer id, and the old writer, if it is still running anywhere, is fenced for good: its next commit fails with `Fenced`. Open such a directory only on purpose, for example to move the database to a new machine. To read the database without taking it over, use `verify` (CLI). Call `is_claimed(dir)` first to tell the cases apart.

The files in the directory:

| File | What it is |
|---|---|
| `app.db`, `app.db-wal` | The database and its WAL. After a restore, `app.db` fills in lazily as pages are read. |
| `app.db-durable` | Local state as JSON: writer id, generation, whether every page is local, and the claim. |
| `app.db-dirty` | Page numbers checkpointed since the last seal, fsynced before `app.db` is. |
| `durable.lock` | The exclusive lock held by the open process. |
| `app.db-restoring` (inside the directory) and `.<dir>.restore-*` (staging directories next to it) | Exist only while a restore runs. Open or restore cleans up after an interrupted restore. |

Errors are `turso_durable::Error`:

| Variant | When |
|---|---|
| `Fenced` | Another writer owns the database now, or the bucket changed since this directory was restored ("restore again"). |
| `Timeout` | A bucket operation kept failing transiently for the whole retry deadline. A commit that fails this way is rolled back locally. |
| `UploadFailed` | The bucket refused an upload permanently, or the uploader stopped. |
| `NotFound` | A key that must exist is missing: no database at this prefix (restore, read-only open), or a page the manifest names. |
| `Unsupported` | A refused feature: `https`, `MVCC`, `journal_mode other than WAL`, `synchronous below FULL`, `ATTACH`. |
| `Locked` | Another process has the directory open. |
| `AlreadyOpen` | This process already has the directory open. |
| `Corrupt` | A bucket object or local file failed its checks. |
| `Bucket` | A permanent S3 error outside an upload, such as 403 on a GET. |
| `Engine` | An error from turso_core, such as an SQL error. |
| `Io` | A local file error. |
| `Config` | Bad settings, a closed database, or connections still open at `close`. |

## CLI

`turso-durable <command> --dir DIR …`. S3 settings come from the `DURABLE_*` variables. Build it with `cargo build --release --locked -p turso-durable-cli`.

| Command | What it does |
|---|---|
| `restore --dir DIR` | Restores into an empty `DIR`. It prints `restored generation=… log_objects=… gets=… lists=… bytes=… ms=…`. |
| `verify --dir DIR [--acked FILE]` | Opens `DIR` read-only and runs `PRAGMA integrity_check`. With `--acked`, it checks that every id in FILE (lines `ack <id>` or `<id>`) is in table `soak`, and fails if an id was acked twice. `DIR` may be empty, in which case it restores first. Verify never claims, never seals and never writes to the bucket. |
| `sql --dir DIR [--claim] SQL…` | Opens `DIR` as the writer, runs each SQL argument, and prints rows tab-separated. Without `--claim`, it runs only on a directory that already holds a writer's own database. For any other directory (missing, empty, or restored and never opened) it exits 1 without touching the bucket, because opening it would restore or create the database and take it over from any running writer. It suggests `verify` or `--claim`. There is no read-only fallback; use `verify` to read. |
| `soak --dir DIR [--checkpoint passive\|full\|restart\|truncate\|reopen] [--every N] [--start ID] [--count N] [--page-put-delay-ms N]` | Commits one 1 KB row at a time into table `soak` and prints `ack <id>` after each acked commit. With `--checkpoint`, it checkpoints (or closes and reopens) every N commits (`--every`, default 100); without it, it never checkpoints itself. Ids start at 1 by default. It runs until killed unless `--count` is given. It retries on errors. |
| `bench --dir DIR --commits N` | Runs N single-row commits of 1 KB and prints `key=value` lines: commit p50/p95/p99/max, commits/s, log PUTs per commit, page PUTs, and each seal's ms and page count. |

Exit codes: 0 on success, 1 on an error (one `error: …` line on stderr), 2 on bad arguments.

**Warning:** `sql --claim` on a restored or empty directory takes the database over. The old writer is fenced, and its next commit fails. `soak` and `bench` always open as the writer, so run them only on their own prefix.

## Settings

`Config::from_env(dir)` reads these variables. `Config::new(dir, endpoint, bucket, access_key, secret_key)` uses the same defaults, and every `Config` field is public.

| Variable | `Config` field | Default | Notes |
|---|---|---|---|
| `DURABLE_ENDPOINT` | `endpoint` | required | `http://host[:port]`. `https://` fails with `Unsupported("https")` until stage 4. |
| `DURABLE_BUCKET` | `bucket` | required | |
| `DURABLE_ACCESS_KEY` | `access_key` | required | |
| `DURABLE_SECRET_KEY` | `secret_key` | required | Redacted from `Debug`. |
| `DURABLE_REGION` | `region` | `us-east-1` | Used for SigV4. |
| `DURABLE_PATH_STYLE` | `path_style` | `true` | `endpoint/bucket/key`. Set it to `false` for virtual-hosted style. Accepts `0`/`1`/`true`/`false`. |
| `DURABLE_PREFIX` | `prefix` | `""` (bucket root) | Every key goes under `<prefix>/`. Use one prefix per database. Only `A-Z a-z 0-9 . _ - /` are allowed, with no leading or trailing `/`, no `//` and no `.` or `..` segment; anything else fails with `Config`. |
| `DURABLE_RETRY_DEADLINE_MS` | `retry_deadline` | 30 s | How long one bucket job may keep retrying transient errors: a commit's log upload, a seal, or a page fetch. |
| `DURABLE_REQUEST_TIMEOUT_MS` | `request_timeout` | 10 s | One HTTP request. |
| `DURABLE_UPLOAD_CONCURRENCY` | `upload_concurrency` | 16 | Parallel page PUTs in a seal, and parallel log GETs in a restore. Must be at least 1. |
| `DURABLE_TRIM_GRACE_SECS` | `trim_grace` | 10 min | How long an unreferenced object stays before it is deleted. A writer idle for half of it checks the head before its next append (one GET). Fencing an old writer after a restore depends on it (see "Restore and the claim"):<br>• Every process that opens a prefix as a writer must use the same value. A claimer's must never be smaller than the old writer's.<br>• It must exceed 2 × (`retry_deadline` + `request_timeout`), 80 s with the defaults. `Config` refuses a smaller value with an error.<br>• `0` (every commit checks the head) is for tests only, so it stays allowed. It is unsafe while an old writer may still be running.<br>Nothing enforces the same-value rule; stage 3's lease replaces it. |
| `DURABLE_PREFETCH` | `prefetch` | `false` | After open, fetch every sealed page in the background. |

Where the defaults live:
- `crates/turso_durable/src/config.rs`, `mod defaults`, holds every default above.
- The retry backoff is fixed and cannot be configured. It starts at 50 ms (`FIRST_BACKOFF`), doubles, and is capped at 2 s (`MAX_BACKOFF`). Both are in `mod defaults` too. `RetryPolicy::default` in `crates/turso_durable/src/store/retry.rs` takes all three retry values from there.
- There is no batching setting in stage 1: every commit is its own upload. The runtime batching setting comes in stage 2.
- The busy timeout between connections is fixed at 5 s. It lives in `crates/turso_durable/src/conn.rs` (`BUSY_TIMEOUT`).

## Bucket format v1

Everything lives under `<prefix>/`. With an empty prefix, keys have no leading `/`.

| Key | Holds |
|---|---|
| `head` | Where the database is now. |
| `log/<generation:010>/<seq:020>` | One log object per WAL fsync. `seq` starts at 1 and has no gaps within a generation. |
| `pages/<generation:010>/<page:010>` | One page, as sealed at the end of `generation`. |
| `manifest/<generation:010>` | Page map written when `generation` was sealed. |

**Log object.** All integers are little-endian.
- The magic `b"TDLOG\0\0\x01"`.
- `kind: u8`: 1 = WAL extents, 2 = fence. Kind 2 is reserved for stage 3; stage 1 reads it as `Fenced`.
- `writer_id`: a `u8` length, then ASCII.
- `sync_count: u32`. Stage 1 writes exactly one sync per object and never batches. The claim record and the seal-start record (below) are the exceptions: they hold zero syncs, so they add nothing to the generation's WAL image.
- For each sync, `op_count: u32`, then the ops:
  - tag 1, a write: `[u64 offset][u32 len][bytes]`;
  - tag 2, a truncate: `[u64 len]`.
- A trailing `sha256` of all preceding bytes.

**Manifest.**
- The magic `b"TDMAN\0\0\x01"`.
- `page_size: u32`, `page_count: u32`.
- `run_count: u32`, then runs of `[first_page u32][count u32][generation u64]`, sorted and non-overlapping.
- A trailing `sha256`.

Reading a page inside `1..=page_count` that no run covers fails with `Corrupt`.

**Head.** Flat JSON with keys in this order. Unknown keys are ignored when it is read.

```json
{"format":1,"generation":3,"manifest":"manifest/0000000002","writer_id":"<32 hex>"}
```

`manifest` is `null` until the first seal. Restore takes pages from `manifest` and replays `log/<generation>/*`.

**Local state `<dir>/app.db-durable`.**

```json
{"format":1,"writer_id":"…","generation":3,"complete":true,"claimed":true,"head_etag":"…"}
```

- It is written to a temp file, fsynced, renamed, and then the directory is fsynced.
- `complete=false` means pages not yet read since this process opened come from the bucket. Only a finished prefetch in a writer open writes `complete=true`, at once; a seal also writes the current value. A read-only open's prefetch keeps it in memory only, because a read-only open never checkpoints its WAL into `app.db`. Taking in a newer generation on a directory no writer has claimed sets `complete=false`.
- `claimed=false` with `head_etag` marks a restored directory that no writer has opened yet. An older file without these two keys reads as `claimed=true` with an empty `head_etag`.

**Writer id.** 16 random bytes in hex. A new database and every claim get a new id.

## S3 features needed

| Feature | Used for |
|---|---|
| AWS Signature V4 (header auth, signed payload hash) | Every request. |
| Path-style addressing (the default) or virtual-hosted style | `DURABLE_PATH_STYLE`. |
| `PUT` with `If-None-Match: *` (412 when the key exists) | Log objects. This is what fences a second writer. |
| `PUT` with `If-Match: <etag>` (412 when the ETag changed) | `head`. A 404 whose error code is `NoSuchKey` also counts as a failed precondition, because RustFS answers that way for a missing key. A 404 `NoSuchBucket` is a permanent error that names the bucket, and so is a 404 with no recognisable code (a proxy or a wrong endpoint is no evidence that the key is gone); neither is reported as `Fenced`. A 409 is retried. |
| `ETag` on every PUT response and GET | Checking `head` before moving it. |
| `GET` | Restore, lazy page reads. |
| `ListObjectsV2` (`list-type=2`, continuation tokens) | Listing a generation's log, and trimming. |
| `DELETE` | Trimming. |
| Plain HTTP | Stage 1 only (no TLS until stage 4). |

`scripts/s3-gate.sh` checks the conditional-PUT behaviour against the local RustFS. Its results for RustFS 1.0.0 are in `results/rustfs-gate.txt`, and all checks passed.

## Guarantees and limits

**What an ack means.**
- When `execute`, `execute_batch`, `transaction` or `COMMIT` returns `Ok`, the commit is in the bucket and in the local fsynced WAL. A restore on another machine includes it.
- `synchronous` is FULL only. `PRAGMA synchronous` set to `OFF`, `NORMAL`, `0` or `1` is refused with `Unsupported("synchronous below FULL")`. A connection whose mode drops anyway has its statement refused, and FULL is put back.
- A commit that returns an error was not acknowledged. It is rolled back locally and the connection stays usable. Its upload may still have landed, though, because a request can succeed after its reply is lost. Such a record is settled at the same log position before anything else is written, so the log never forks. Treat an error as "outcome unknown", as with any remote database.

**What is refused.** The `Connection` wrapper parses every SQL string with `turso_parser` and refuses these before the engine sees them (D5, D7):

| Statement | Error |
|---|---|
| `PRAGMA journal_mode` set to `mvcc` or `experimental_mvcc` | `Unsupported("MVCC")` |
| `PRAGMA journal_mode` set to anything but `wal` | `Unsupported("journal_mode other than WAL")` |
| `PRAGMA synchronous` below FULL | `Unsupported("synchronous below FULL")` |
| `ATTACH` | `Unsupported("ATTACH")`. Writes to an attached database would not be uploaded. |

MVCC is unsupported. It is also refused at open: from bytes 18/19 of a local `app.db` or of the manifest's page 1, and from `Database::mvcc_enabled()`. Opening an MVCC log file (`*.db-log`) is refused at the IO layer as a backstop.

**One writer.**
- A second process that opens the same directory gets `Locked`. A second open in the same process gets `AlreadyOpen`. Readers in another process are not supported (D4).
- The bucket allows one writer per prefix. A writer whose log position or head was taken by another writer gets `Fenced` and stays fenced.

**Restore and the claim.**
- `restore` is read-only on the bucket. It costs `3 + log_objects` GETs when a manifest exists (head, manifest, page 1 for the MVCC check, then the log), or `1 + log_objects` for a prefix never sealed (page 1 is seeded from the WAL), plus one LIST. Pages are fetched later, on first read, or in the background with `prefetch`.
- A restored directory is *unclaimed*. The first writer open claims it with "a head write, then one log record":
  1. It writes `head` with a new writer id, using `If-Match` on the ETag the restore read.
  2. It appends one empty log record at the generation's next seq. Stage 3's fence record will reuse this slot.
- If the head moved since the restore, the claim fails with `Fenced("… restore again")`. Restore into a new directory.
- An old writer that is still running is fenced at its next append, and nothing it appends after the claim is acked:
  - If it appends within the trim grace, that append collides with the claim record (or a later record of the claimer's).
  - Later than that, the claimer may have sealed the generation and its trimmer may have deleted `log/G`, so a record would land where no restore reads it. Every writer therefore checks the head before an append once its idle timer is `trim_grace / 2` old. The check is one GET of `head`, retried within that commit's deadline. If the head still has this writer's ETag (or is this writer's own late head, see item 1 below), the append proceeds. Anything else is `Fenced`, and nothing is written.
  - The idle timer is refreshed by a confirmed log record or head write, by open's head read, and by a head GET that passes either check: this idle check, or a seal's head check (the seal-start check, or the pending-seal check of a retried seal). A failed or ambiguous request refreshes nothing.
  - The cost: one GET on the first commit after `trim_grace / 2` of idle (5 min by default). With `trim_grace = 0` every commit makes that GET.
  - **This fence holds only under assumptions, most of which nothing enforces:**
    - Every process that opens the prefix as a writer uses the same `trim_grace`, and a claimer's is never smaller than the old writer's. A claimer with a smaller grace (say 0) trims `log/G` before the old writer's check is due, and the old writer's commits are then acked into a log no restore reads.
    - `trim_grace` exceeds 2 × (`retry_deadline` + `request_timeout`); `Config` now enforces this bound. An append that skipped the check (its timer was younger than `trim_grace / 2`) can still land up to `trim_grace / 2` + `retry_deadline` + `request_timeout` after the timer's last refresh. That stays below `trim_grace` only if `trim_grace` > 2 × (`retry_deadline` + `request_timeout`).
    - `trim_grace = 0` is for tests only. It is unsafe while an old writer may still be running.
    - An old writer whose machine was suspended or paused: `Instant` does not advance during system sleep, so its idle timer can stay young and it can skip the idle head check. Stage 3's lease addresses it.
    - Under those assumptions, an old writer can only reach a trimmed `log/G` after being idle for at least the trim grace, because any earlier append collides with the claim record. Stage 3's lease replaces all of this.
- An old writer is also fenced at the start of its next seal, before it uploads a page or a manifest. An old writer that restarts is fenced at open.
- Every seal starts with an empty *seal-start* record at the writer's next seq (`put_if_absent`, one extra log PUT per seal), then a GET of `head` (one extra GET per seal). Page and manifest objects are plain PUTs, so this gate is what stops an old writer from overwriting a generation the claimer already sealed:
  - The old writer's seal-start record lands on the claim record's seq (or a later record of the claimer's). It is fenced and uploads nothing. This covers an idle old writer that closes after the claim (its close-time TRUNCATE seals), and a commit of the old writer that crosses its own generation boundary (the seal runs inside that commit, before its own log record).
  - The head check covers the case where the claimer's trimmer has already deleted `log/G` (the claimer sealed G and the trim grace passed), so the record has nothing to collide with. The head names the claimer, so the old writer is fenced.
  - A seal-start record whose upload is ambiguous is settled at the same seq, like any other in-doubt record, before anything else is written. A retried seal whose earlier head write is now known to be published adopts that head and writes no new record.
- A generation may then hold the old writer's records followed only by the claimer's, and restore accepts that shape.
- With these rules, and the `trim_grace` assumptions above, an old writer cannot get a commit acked, or overwrite a sealed generation, after a claim, except in the races and crash windows below, which stage 3's lease and epoch fencing close.
- One gap remains: between the restore and the claim record, the old writer can still win a seq race. The claimer's open then fails `Fenced`, and nothing acked is lost. Stage 3's lease closes this gap.
- `verify` (and the hidden `open_read_only`) never claims, never seals and never writes to the bucket.
- When the bucket holds no manifest yet, restore seeds `app.db` with the WAL's first page-1 frame. turso treats a zero-size database file as new and would delete its WAL (database.rs:738-760, 2223-2246).

**Known races and crash windows (stage 3's lease and epoch fencing close them).** The bucket format does not change for any of them.
- **An old writer's page PUTs still in flight.** The old writer's seal-start record lands and its head check passes. Then, while its page PUTs are still in flight, another process restores, claims, commits and seals the same generation. The old writer's late page PUTs can overwrite that seal's `pages/G/*` or `manifest/G`. Its head write then fails and it is fenced, but the claimer's pages may already be replaced. Narrowing this needs a lease (stage 3); do not claim a database whose old writer may still be running a seal.
- **A head PUT in flight at a crash.** A writer crashes while its seal's head PUT is in flight (or, in one process, a failed head PUT lands after the retried seal saw the head unchanged). The restarted writer attaches to the old head and acks commits into `log/G`. Then the delayed PUT lands, and the head names generation G+1 with the manifest the crashed attempt uploaded. A restore then misses the commits acked into `log/G` after the restart. They stay unrestorable until that writer's next seal re-uploads G's pages and adopts that head (`crates/turso_durable/src/bucket.rs:507-508`; the seal-start and idle head checks accept this writer's own late head). They are lost for good if, before that seal, the writer restarts again (reconcile replaces the local WAL with the G+1 image) or another process restores and claims.
- **A crash between the claim's head write and the local-state write** (`crates/turso_durable/src/db.rs:353-359`). The head then names a writer id that nobody holds. The new directory still has `claimed=false` and the old ETag, so reopening it fails `Fenced("… restore again")`. No claim record was written, so the old writer keeps appending; its commits stay in `log/G` and restore. It is fenced at its next seal, or at its first append after `trim_grace / 2` of idle (the head checks). Nothing acked is lost; restore again into a new directory to continue.

**Test and diagnostics hooks.** `Config::on_event`, `turso_durable::Event` and `turso_durable::EventHook` are `#[doc(hidden)]`. They exist for the tests and for the CLI's `soak` event lines, and are not part of the supported API: they may change without notice.

**Known cost: the commit that crosses a generation boundary.**
- Pages are uploaded at the generation boundary (D6): the write that restarts the WAL. After an automatic checkpoint, or a `Passive`, `Full` or `Restart` checkpoint, that write happens in the next commit, which waits for the whole generation's page upload, the manifest and the head. `CheckpointMode::Truncate` and close seal inside the checkpoint instead.
- With turso's automatic checkpoint (every 1000 WAL frames), the bench crossed a boundary about every 500 commits of 1 KB.
- In the 5000-commit bench, the commit p50 was 1.33 ms, while the slowest commit took 69.8 ms. That commit is most likely the one that crossed the first boundary (bench does not record which commit triggered a seal), whose seal took 39.4 ms for 169 pages.
- Each in-run seal took 37–44 ms for 169–172 pages.
- You can move this wait off a latency-sensitive path by calling `Connection::checkpoint(CheckpointMode::Truncate)` at a quiet moment.

**Limits.**
- Without `prefetch`, a restored database fetches pages lazily in every process that opens it, until a prefetch completes. Each process fetches each page it reads once. Only a finished prefetch in a writer open marks the directory `complete`.
- Short-lived processes never trim. The trimmer waits `trim_grace` (10 min by default) after each seal, and close stops it. A process that exits sooner than `trim_grace` after its seals deletes nothing, so the bucket grows until a long-lived process trims or stage 3's `gc` runs.
- Memory use grows with the WAL. A seal reads the whole local `-wal` into memory and holds every dirty page of the generation while it uploads them. A WAL that grows large (a long-running reader that blocks checkpoints, for example) makes the seal large too. `CheckpointMode::Truncate` at quiet moments keeps generations small.
- Never copy a live database directory (`cp -r`, a VM or disk clone). The copy has the same writer id, so fencing cannot tell the two processes apart. Restore into a new directory instead.

**Edge cases.**
- `PRAGMA wal_checkpoint` reports an upload failure as a busy row, not as an error (vdbe/execute.rs:796-803). Use `Connection::checkpoint` to get the error.
- `close` ignores a failed close-time checkpoint (pager.rs:5458-5472). Nothing acked is lost: the log already holds every acked commit, and the next open or restore replays it.
- If the local fsync fails after a confirmed upload, the engine panics (pager.rs:4723-4733). This is a local disk failure. The commit is in the bucket, so restore the database elsewhere.
- VACUUM is untested.
- Plain HTTP only until stage 4. The figures here are from RustFS on a laptop; nothing has been measured on real S3.
- Closing during a bucket outage can block for up to the retry deadline.

## Costs (measured)

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

These figures predate the seal-start record and the idle head check. Each seal now also makes one extra log PUT and one head GET before its page PUTs. The first commit after `trim_grace / 2` of idle (5 min by default) makes one head GET; the bench never idles that long.

Restoring the 5000-commit database (generation 12, 1673 pages, 6.9 MB):
- The restore itself took 33 ms. It made 4 GETs (`3 + 1` log object) and 1 LIST, and downloaded 4.6 KB.
- A full `verify` of a fresh restore (`PRAGMA integrity_check`, which reads every page lazily) took 0.52 s of wall time, including process start.

## Decisions

These are the design decisions behind stage 1. The evidence is turso_core 0.8.3 from crates.io; paths are relative to its crate root.

| # | Decision | Evidence |
|---|---|---|
| D1 | Use turso_core `=0.8.3` from crates.io with features `fs`, `json`, `uuid`; `fs` alone does not build. Never use `encryption`. | Newest stable release, vcs `607bff9fd`. |
| D2 | Use only public APIs: `IO`/`File`, `DatabaseStorage`, `OpenOptions::storage`, `Database::open`/`connect`/`mvcc_enabled`, `Connection::prepare`/`checkpoint`/`close`/`set_sync_mode`, and `turso_parser`. | io/mod.rs:424-484, 153-214; storage/database.rs:106-132; database.rs:240, 254, 1216, 2383, 3445; connection.rs:1049, 2380, 2439, 4012. |
| D3 | A failed upload fails the commit without the fsync panic. The WAL file's `sync` uploads before it creates any completion and returns `Err` synchronously, so the engine rolls the commit back. | pager.rs:4711-4733, 4486-4488; vdbe/mod.rs:3400-3409, 3609-3612; wal.rs:4271-4278. |
| D4 | One process. An explicit exclusive lock on `durable.lock` gives `Locked`, and a process-wide registry gives `AlreadyOpen`. The multiprocess WAL is never enabled. | database.rs:80, 876-879, 1288-1305; io/unix.rs:67-71, 278-297. |
| D5 | MVCC is refused. The SQL guard refuses the PRAGMA before the engine writes the 255/255 header, and open checks the header and `mvcc_enabled()`. `.db-log` is refused as a backstop. Other journal modes and ATTACH are refused too. | sqlite3_ondisk.rs:200-228; vdbe/execute.rs:19165-19192; journal_mode.rs:19-27, 90-95. |
| D6 | Pages upload when the WAL restarts (the generation boundary), not at each checkpoint. A seal uploads the union of the in-memory dirty set, the persisted `app.db-dirty`, and every page in the local WAL's frame headers. It first appends an empty seal-start record and checks the head (`Bucket::seal_start`), then uploads the pages, then writes the manifest, then moves the head with `If-Match`. | wal.rs:5171-5180, 5289-5310, 6024-6050; pager.rs:4514-4516, 5458-5472. |
| D7 | `synchronous` is FULL only. The guard refuses lower values, and each statement is checked after `prepare`. | lib.rs:225-229; pager.rs:4711; translate/pragma.rs:662-676. |
| D8 | Typed errors cross the engine through a thread-local stash. `Connection::checkpoint` uses the engine call that returns `Err`, because the PRAGMA reports busy. | vdbe/execute.rs:796-803; connection.rs:2432-2434. |
| D9 | On restart on its own disk, the writer reconciles the local WAL with the bucket's generation image: level, behind (take the bucket's), ahead (upload the difference), or a new generation. A different writer id gives `Fenced`. | `crates/turso_durable/src/reconcile.rs` |
| D10 | Trimming runs after each seal, once the grace period has passed. It deletes older logs, manifests and superseded pages, and never touches a generation at or above the head's manifest. | `crates/turso_durable/src/trim.rs` |

## Test command

- `./scripts/test.sh unit`: fmt, clippy (`-D warnings`) and unit tests.
- `./scripts/test.sh`: the above, then starts RustFS (`scripts/rustfs.sh up`) and runs the RustFS-backed `--ignored` tests with `--test-threads=1`. These include the CLI system tests (crash matrix, faults, restart, second process, MVCC, trimming).
- `scripts/rustfs.sh up|down|stop|start|pause|unpause|env` manages the local RustFS container (`turso-durable-rustfs`, port 9290). `eval "$(scripts/rustfs.sh env)"` sets the `DURABLE_*` variables for it.
- Always pass `--locked` to cargo. On macOS the script sets `SDKROOT`, and it sets `SOURCE_DATE_EPOCH`.

## Layout

- `crates/turso_durable`: the library.
- `crates/turso-durable-cli`: the `turso-durable` binary and the system tests.
- `scripts/`: `test.sh`, `rustfs.sh` (local S3), `s3-gate.sh`.
- `results/`: `rustfs-gate.txt` (conditional-PUT gate) and `durable-core.txt` (stage 1 measurements).
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

Conditions:
- turso_core 0.8.3 does NOT build with only `["fs"]`. `incremental/dbsp.rs` uses `uuid` without a feature gate, and `vdbe/execute.rs` has a type error in its non-json path. The minimal feature set is `default-features = false, features = ["fs", "json", "uuid"]` (no "encryption"). Verified with `cargo check --offline --locked --workspace --all-targets`. This adds uuid 1.26.1 and sha1_smol 1.0.1, both reviewed and clean.
- Copy the audited `Cargo.lock` with the manifests unchanged, and always build/test with `--locked`. Any lock change (cargo update, new dep or feature) needs a re-audit.
- turso_core's build.rs runs `git rev-parse`/`git show` only to embed a commit hash and timestamp. Set `SOURCE_DATE_EPOCH` for reproducible builds, which also skips the git call.
- Cargo.lock lists cfg-gated crates that normal builds never compile (cfg(antithesis/loom/shuttle): antithesis_sdk 0.3.0, loom, shuttle, generator) plus wasm/windows targets. antithesis_sdk 0.3.0 was not downloaded or reviewed; do not build with `--cfg antithesis`.
- aegis 0.9.20 ships prebuilt wasm static libs (`wasm-libs/*.a`) that are linked only for wasm32. Re-audit before targeting wasm.
- Row 2: no TLS feature on opentelemetry-otlp or tonic, and no `http-proto`, `http-json`, `reqwest-*`, `hyper-client` or `metrics`, nor the opentelemetry_sdk features `rt-tokio`, `experimental_*` or `spec_unstable_*`, in normal dependencies; `opentelemetry_sdk/testing` stays dev-only. Set the OTLP endpoint in code (it beats `OTEL_EXPORTER_OTLP_*ENDPOINT`; `OTEL_EXPORTER_OTLP_*HEADERS` are still merged in, so never log the exporter config). Keep the targets `opentelemetry*`, `tonic`, `h2`, `hyper`, `tower` and `reqwest` out of the log bridge. web-time and the wasm paths of the new crates were not reviewed.
