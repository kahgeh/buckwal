# Packer, stage 1: what was measured

Date: 2026-10-25. Sizes: in the multipart tables, "MB" and "GB" mean MiB and GiB, so a "100 MB" object is 104,857,600 bytes. Every other size is in decimal MB. Machine: Apple M4 Max Mac, macOS, home internet link. Stores: RustFS 1.0.0 on this Mac, and AWS S3 in ap-southeast-2.

**This report is partial.** The user asked to stop the measurements on 2026-10-25 and to start stage 2. The section "Not measured" lists what stage 1 did not do. The probes were throwaway code and are not in the repository.

## Result

| Item | Result |
|---|---|
| RustFS completes a multipart upload and answers a range GET | Yes |
| turso 0.8.3 in-place `VACUUM` on plain, encrypted and MVCC databases | Works. The file reopens and passes `PRAGMA integrity_check` |
| `VACUUM INTO` on an encrypted database | **Blocker.** The copy is a plaintext SQLite file |
| buckwal on real AWS S3 | 9 of 9 non-TLS S3 store tests pass, after a test-only change. `create_bucket` fails outside us-east-1 |
| Go or no-go | The user gave the go for stage 2 on 2026-10-25 without waiting for the rest of the numbers |

## Real AWS S3, ap-southeast-2, from this Mac

Probe: Python standard library with SigV4, one keep-alive connection per thread. HTTPS to `s3.ap-southeast-2.amazonaws.com`.

| Request | Median | Min | Max |
|---|---|---|---|
| GET 4 KiB (30 requests) | 49.0 ms | 35.5 ms | 54.1 ms |
| GET of a missing key (15) | 37.1 ms | 28.7 ms | 56.8 ms |
| PUT 4 KiB (30) | 50.4 ms | 44.4 ms | 60.9 ms |
| PUT with `If-None-Match: *` (20) | 49.0 ms | 43.5 ms | 55.7 ms |
| PUT with `If-Match` (20) | 60.5 ms | 56.4 ms | 132.0 ms |

A PUT with a wrong `If-Match` etag and a PUT with `If-None-Match: *` on an existing key both answer 412.

Multipart upload in 16 MiB parts, 8 parts at once, then a range GET download of the same object, 16 MiB ranges, 8 at once:

| Object | Run | Upload | Range GET download |
|---|---|---|---|
| 100 MB | 1 | 9.52 s (10.5 MB/s) | 2.58 s (38.7 MB/s) |
| 100 MB | 2 | 9.31 s (10.7 MB/s) | 1.93 s (51.8 MB/s) |
| 100 MB | 3 | 9.78 s (10.2 MB/s) | 3.50 s (28.5 MB/s) |
| 1 GB | 1 | 81.54 s (12.6 MB/s) | 14.95 s (68.5 MB/s) |
| 1 GB | 2 | 82.79 s (12.4 MB/s) | 17.07 s (60.0 MB/s) |
| 1 GB | 3 | 82.64 s (12.4 MB/s) | 15.20 s (67.4 MB/s) |

The upload speed is the speed of this Mac's link. A server in the same AWS region would be faster. Stage 1 did not measure that.

**Today's design on 100 MB (AWS)**, with buckwal's own library: 100 MB filled in 108.7 s (25,120 PUTs). A commit that changed 10% of the pages took 1.60 s. The next commit, which sealed the restarted WAL, waited **8.85 s** (2,571 PUTs, 25.3 MB). Later small commits took 0.05 to 0.06 s. A restore took 0.5 s. The full read of the restored copy (about 26,000 GETs) did not finish and is not measured.

**buckwal's S3 store tests on AWS.** The 9 non-TLS tests of `crates/buckwal/tests/s3_rustfs.rs` ran against the AWS bucket with `BUCKWAL_PATH_STYLE=false`, and passed (118 s). They need a test-only change first (the test skips `create_bucket`, because the bucket already exists): `S3Store::create_bucket` sends no `LocationConstraint`, so AWS answers `IllegalLocationConstraintException` outside us-east-1. The 4 TLS tests need local certificates and did not run. buckwal ran on AWS S3 for the first time in this stage, over the store and the open, commit, close, restore path.

## RustFS checks

- RustFS completes a multipart upload (3 parts of 16 MiB, 16 MiB and 6.4 MB; 40 MB in all) and answers `Range` with 206 and the right bytes.
- On RustFS direct (no added delay): 4 KiB GET median 0.6 ms, PUT 1.2 ms, `If-None-Match` 1.1 ms, `If-Match` 1.4 ms. A 100 MB multipart upload took 0.80 s (125 MB/s) and its range download 0.24 s.
- Today's design on a 20 MB database, no delay: the sealing commit waited 0.15 s (530 PUTs). This shows the same mechanism as the 2026-10-25 figures in the spec: the commit after a WAL restart uploads the changed pages.

## turso 0.8.3 `VACUUM` (turso_core, `DatabaseOpts::with_vacuum(true)`)

A table of 4,000-byte blobs, half the rows deleted, then a TRUNCATE checkpoint, then the statement. The 1 GB numbers are shown (1,000 MB of rows). The times come from the first 1 GB run. The reopen and header checks come from a second run. A run at 100 MB gave the same results.

| Database | Statement | Time | Result | Peak WAL |
|---|---|---|---|---|
| plain | `VACUUM` | 1.11 s | 1,051 MB to 526 MB, integrity ok after reopen, 128,000 rows | 529 MB |
| encrypted (aegis256) | `VACUUM` | 2.86 s | 1,051 MB to 526 MB, integrity ok after reopen, 128,000 rows | 529 MB |
| MVCC | `VACUUM` | 1.02 s | 1,047 MB to 526 MB, integrity ok after reopen, 128,000 rows | 529 MB |
| plain | `VACUUM INTO` | 1.02 s | copy opens, integrity ok | none |
| MVCC | `VACUUM INTO` | 1.21 s | copy opens, integrity ok | none |
| encrypted | `VACUUM INTO` | 0.97 s | **copy is plaintext and does not open with the key** | none |

After the in-place `VACUUM` of the encrypted database, the file still has no plaintext header. The in-place `VACUUM` writes a WAL about as large as the new file. **The `VACUUM INTO` copy of an encrypted database starts with `SQLite format 3` (plaintext), and opening it with the key fails with `Decryption failed for page=1`.** Stage 2 must use the in-place `VACUUM` on the packer's working copy and never `VACUUM INTO` on an encrypted database.

## Not measured

- The packer's steps on RustFS with 0, 20 and 50 ms added (download, apply, upload, pointer; local apply of one, two and five generations). Stage 2 measures the real packer instead.
- The pause that stays: the size-limit checkpoint at a 64 MiB and a 256 MiB WAL with readers open.
- The check that a PASSIVE checkpoint with a reader open never waits for the reader (the probe was stopped before it ran).
- The full read of a restored 100 MB copy on AWS (about 26,000 GETs).
- 1 GB on RustFS with added delay (the spec's figures of 2026-10-25 stand).

## AWS cost

The probe counted its own requests. The runs of the buckwal library were added as planned amounts, so every figure here is an upper estimate. Prices are the ap-southeast-2 list prices: PUT, POST and LIST at US$0.0055 per 1,000, GET and HEAD at US$0.00044 per 1,000. Transfer out is at US$0.12 per GB and is always charged.
- 34,016 PUT, POST and LIST requests: US$0.19.
- 58,242 GET and HEAD requests: US$0.03. About 27,300 of them are DELETE requests from the cleanup, which AWS does not charge. The count also holds 28,000 GETs reserved for the stopped full read.
- 3.69 GB downloaded: US$0.44.

The total is at most **US$0.66**, under the limit of US$5. After the cleanup the bucket holds no object and no open multipart upload.
