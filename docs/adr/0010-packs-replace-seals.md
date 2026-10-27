# 0010. Packs replace seals

- Status: Accepted
- Date: 2026-10-26
- Spec: `packer` (rev 31, sha256 313b6589c4a9)
- Decision record in the README: How it works, Packs, Bucket format v5, D33
- Replaces: ADR 0002 in part, ADR 0003 in part and ADR 0004. ADR 0005 stays.

## Context

A seal uploaded every page that changed in a generation as its own object, then a manifest, then the head. The commit that restarted the WAL waited for all of it. On a 1 GB database with 10% of the pages changed, that commit waited 43 s at 20 ms added per request and 97 s at 50 ms, for 25,615 page objects. A restore read one object per page: a full read of a restored 1 GB database took 256,647 requests, about 97 minutes at 20 ms. Both costs come from the seal.

## Decision

- **A generation switch replaces the seal.** At a WAL restart the writer uploads the seal-start record of the old generation. Then it moves the head to the next generation with `If-Match`. It uploads no page and no manifest.
- In format 5 the seal-start record is log record kind 4. It names the next generation, under the record's integrity code in an encrypted bucket.
- The writer runs the engine's PASSIVE checkpoint often, so the WAL restarts by itself when no reader holds it. The maintenance checkpoint runs only at the WAL size limit and uploads nothing.
- The fsync of a checkpoint fsyncs the local file only. It never ships an operation of another connection.
- **Packs hold the database file.** A pack is one object, `pack/<G>/<run id>`, that holds the whole database file. It holds every commit of every generation below its pack generation `G`. It is uploaded as an S3 multipart upload in 16 MiB parts, 8 at once.
- The pack codes object (`pack/<G>/<run id>.codes`) has one code per 1 MiB block. The code is the SHA-256 in a plaintext bucket. It is an integrity code under the database integrity key (ADR 0009) in an encrypted bucket.
- The pack pointer `pack-latest` names the newest pack. It moves only with `If-Match` (`If-None-Match` for the first pointer).
- **The packer makes packs.** It applies a generation only when the head names a later generation and the generation's last record is a seal-start record (the terminal record).
- It follows the chain of terminal records from the pack generation to the head's generation. A broken chain stops the run as `failed` with `log_incomplete`. The packer never writes the head or the log.
- It runs as a thread in the writer by default (`BUCKWAL_PACK=0` turns it off). It also runs as `buckwal packer` or `buckwal pack` on any machine that holds the key file.
- **The pack key is bound relative to the prefix.** The pointer stores the pack key and the codes key relative to the database's prefix (for example `pack/15/<run id>`).
- The block codes and the pointer's code bind that relative key, never the prefix or the bucket name. ADR 0009 leaves the prefix out of every code in the same way.
- So a database copied with its head, pointer, pack, codes and log to another prefix or bucket still opens and restores. A pack moved to another relative key fails its codes.
- **Every format 5 head and every pointer carry the `database_id`**, in a plaintext bucket too. A reader accepts a pointer only when its `database_id` is the head's. It treats any other pointer as no pointer.
- A create that finds no head deletes a leftover pointer with `If-Match`. The creator whose head lands writes its pointer again with a new `write_id`. So a late delete by a slow creator fails with 412.
- **A new MVCC database starts with a pack.** It is built in a staging directory. Then it is published in this order: `pack/2/<run id>`, its codes, the first pointer with `If-None-Match`, and the head (generation 2) with `If-None-Match`. Last, the pointer is written again with a new `write_id`.
- **GC keeps what a reader needs.** The GC base is the newest pack, at or below the pointer's pack generation, whose codes object is older than the pack keep time.
- The pack keep time is 4 times the newest pack's download time at 60 MB/s, at least the trim grace. GC deletes packs and log only below the GC base.
- GC deletes an orphan pack after the orphan grace (6 hours). A pack run gives up at half of it.
- **Older buckets convert on request.** `buckwal upgrade` takes a format 1 to 4 bucket over. It builds the first pack from the pages, the manifest and the log, and writes the head at format 5.
- Until then such a bucket keeps its seals, with a fixed trigger at 1,000 log objects.
- **A vacuum runs only on `buckwal vacuum`**, in place on the writer's own file. A vacuum in the packer would apply later log records to moved page numbers.

## What it replaces

- **ADR 0002.** The MVCC seal in `LogDurableStorage::truncate` becomes a generation switch in format 5. It still runs before the engine cuts the log, so a failed switch leaves the log and the `-wal` whole, and a failure after a switch still poisons the database. MVCC buckets of formats 2 and 4 seal there as before until `buckwal upgrade`.
- **ADR 0003.** Format 2 for MVCC buckets only gives way to format 5 for both journal modes. The head keeps `journal_mode`, and kind 3 records stay. Formats 1 to 4 are still read.
- **ADR 0004.** The first seal at generation 2 becomes a first pack: pack generation 2 and head generation 2. The staging directory stays.
- **ADR 0005 stays.** The generation floor, the takeover quiet and the integrity codes are unchanged, and the packer obeys them. A takeover keeps the generation, and the packer applies the records after a fence as a restore does. In format 5 a reader finds a skipped generation from the seal-start record, not from the head's floor.

## Consequences

- A commit no longer waits for page uploads. On RustFS with 50 ms added and a 1 GB database, the longest small commit waited 113 ms [132 ms] (1.94 [2.13] small requests), outside the size-limit pause. The commit that restarts the WAL with batching Off waited 237 to 278 ms, 4.08 to 4.51 small requests, within the goal of its request count plus 1.5 (5.5). The size-limit pause was 319 ms [338 ms]. The README, Costs, gives every figure.
- A restore reads one pack in 16 MiB ranges, then the log since it. A 1 GB pack with no log after it was fully local in 3.5 s [3.6 s] at 20 ms, and the open returned at 0.6 s [0.7 s].
- The bucket holds the whole database file as one object. In a plaintext bucket it opens in any SQLite tool.
- Each pack run uploads the whole file. Its kept copy saves the download of the pack.
- Every machine that runs a packer needs the writer's key file.
- With no packer, GC deletes no log and a restore gets slower.
- **The MVCC create gap.** An MVCC create can stop after its head landed and before its pointer rewrite succeeded. The pointer is then absent or another database's, and a restore or a pack run of that bucket fails with `Corrupt`. No committed data is lost: the create never returned `Ok`, and nothing can commit before the create returns. The way out is to delete the object `head` under the prefix and create the database again. The error names this way out only when the head is at generation 2, epoch 1, and its log holds no object.
- An MVCC restore of an ended generation downloads the whole pack: turso 0.8.3 reads every page when it opens an MVCC database.
- Builds before format 5 refuse a format 5 head before they read or write anything.
