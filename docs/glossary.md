# Glossary

One line per term, as the code on this branch uses it. The README has the full rules.

- **durable**: a commit is durable when a log record holding it is in the bucket (and in the local fsynced file). `execute` and `COMMIT` return `Ok` only then.
- **head**: the bucket object `head`, flat JSON: `format`, `generation`, `manifest`, `writer_id`, `epoch` (and `journal_mode` in format 2). It says where the database is now (`format/head.rs`).
- **generation**: one stretch of the log between two seals. Log records live in `log/<generation>/<seq>`, and a restore reads only the head's generation.
- **seal**: the end-of-generation upload: a seal-start record, then every page that changed in the generation (`pages/…`), then a manifest, then the head moved to the next generation with `If-Match`. WAL mode seals when the WAL restarts, MVCC mode in `LogDurableStorage::truncate`.
- **seal-start record**: an empty log record (zero syncs) a seal appends first, so a writer that was taken over collides with the fence before it uploads a page.
- **manifest**: `manifest/<generation>`, written at a seal: the page size and count and the generation that holds each page's latest copy.
- **log record**: one object `log/<generation>/<seq>`: magic, kind, writer id, syncs of ops, `sha256`. Kind 1 holds `app.db-wal` extents, kind 2 is a fence record, kind 3 (format 2 only) holds `app.db-log` extents.
- **epoch**: a counter in the head. A new database starts at 1, a takeover writes the next one, every other writer open raises it by one, so a late head PUT from an earlier process fails its `If-Match`.
- **fence**: a record (kind 2) a takeover appends to the log. Any writer whose append collides with it, or whose head check finds another writer, gets `Fenced` and stays fenced.
- **lease**: how long (default 30 s, `DURABLE_LEASE_MS`) a writer trusts its last confirmation that the head is its own. Past it, the next append reads the head first.
- **quiet (takeover quiet)**: the wait (`lease` + `request_timeout`) before the new writer seals a generation whose old writer may still be uploading a seal.
- **takeover**: restoring a database and writing the head with a new writer id at the next epoch, then a fence record. The old writer is fenced for good.
- **claim**: a writer open of a restored or empty directory takes the database over with a new writer id (the claim id, persisted before the head write so a crash resumes it).
- **writer id**: 16 random bytes in hex that name one writer in the head and in every record. A new database and every takeover get a new one.
- **restore**: rebuilding a database directory from the bucket alone: the head, the manifest and the generation's log. It writes nothing to the bucket, and pages come lazily on first read.
- **reconcile**: on a writer open, comparing each local file with the bucket's image of the generation: **Level** (equal), **Behind** (the local file is shorter, so the bucket's image replaces it), **Ahead** (the local file is longer, so the difference is uploaded) or **NewGeneration** (the bucket sealed since, so the image replaces the file). Done per file (`app.db-wal`, and `app.db-log` in MVCC).
- **GC and trim**: deletion of log records, manifests and pages the head no longer reaches, once the bucket's own clock says they have been unreferenced longer than the trim grace. `gc` runs one pass from anywhere, a writer runs it on its own thread.
- **batching (Off, Window)**: `Off` uploads every commit on its own. `Window` shares one upload between commits waiting up to `max_wait`, `max_commits` or `max_bytes`. Window is refused in MVCC mode.
- **group commit**: the MVCC engine's own batching: concurrent commits are written by one lead with one sync, so one kind 3 record carries the group.
- **MVCC**: turso's concurrent-writes journal mode. A database created new in MVCC mode is durable here (bucket format v2). An existing WAL database is never switched to it.
- **`BEGIN CONCURRENT`**: the MVCC transaction that lets several writers run at once. Write conflicts fail with `Write-write conflict` and are retried. Use it for every write in MVCC mode.
- **hybrid design**: the MVCC design of `docs/mvcc-recon.md` (hybrid C): the commit point at the `.db-log` sync through a file wrapper, the seal in a `DurableStorage` wrapper, the WAL carrying only checkpoint frames.
- **`LogShippingFile`**: the wrapper around `app.db-log`. It records writes as ops and at `sync` uploads the unconfirmed ones as a kind 3 record before the local fsync (`io/log_file.rs`).
- **`DurableStorage` wrapper**: `LogDurableStorage`, given to the engine with `OpenOptions::durable_storage`. It delegates to the engine's `Storage`, except that `truncate` seals first and a checkpoint failure after a seal poisons the database (`io/log_storage.rs`).
- **confirmed-sync rule**: a `.db-log` sync returns `Ok` only when every op recorded so far is confirmed in the bucket. Unsent ops are kept for the next sync and in-doubt ones are settled first.
- **speculative read**: in MVCC mode a read inside `BEGIN` or `BEGIN CONCURRENT` can see another connection's commit while its upload is in flight. Autocommit reads wait out the upload.
- **outcome unknown**: what an error from a commit means: it may or may not be in the bucket. In MVCC mode the commit also stays in the local database and the next commit ships it.
- **format v1 / v2**: the bucket format. v1 is every WAL bucket (record kinds 1 and 2). v2 is MVCC only: the head has `"format":2` and `"journal_mode":"mvcc"`, and kind 3 records exist. A v1 build refuses a v2 head.
- **maintenance checkpoint**: a background `TRUNCATE` checkpoint that seals a generation the engine never restarts, triggered by log objects or by bytes (the `-wal` in WAL mode, the `app.db-log` in MVCC mode).
- **dirty file**: `app.db-dirty`, the page numbers checkpointed since the last seal, fsynced before `app.db` is, so a seal after a crash still uploads them.
- **local state**: `app.db-durable`, JSON in the database directory: writer id, generation, completeness, claim, epoch and the takeover quiet (`"format":2` for an MVCC directory).
- **replica**: not a library concept. The code has only restored copies of a database, and the server's `replication_index`, which is always `null` in the cursor's last line.
