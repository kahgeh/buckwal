# 0008. Background commits are a batching setting

- Status: Accepted
- Date: 2026-10-25
- Spec: `background-commit`
- Decision record in the README: Decisions, D32

## Context

Every commit that returns `Ok` is durable: its log record is in the bucket. For an app that keeps its own replayable message log, that wait buys nothing, because the app can replay what a lost machine loses. The wait is the whole commit latency: 1.19 ms of 1.19 ms at p50 on a laptop against RustFS.

## Decision

A commit that returns before its upload is a new `Batching` variant, `Background`, and not a separate call. It gathers commits like `Window`. The WAL sync fsyncs locally and queues the record. The call returns without an ack, and waits only while more than `max_behind` background commits wait. The uploader acks the commit when its record is in the bucket, and publishes the highest such commit number as the durable position.

A returned `Ok` that is not an ack is a new meaning. A setting keeps it out of the default path: the default stays `Off`, and approving the spec was the explicit opt-in. `max_behind` bounds the loss of a lost machine. While the uploader is degraded, a commit made with the setting takes the `Off` path, so an outage shows at once. MVCC refuses the setting, as it refuses `Window`.

## Consequences

- A lost machine or disk loses at most `max_behind` of the newest commits, and never leaves a gap, because the log is uploaded in order.
- `Off` and `Window` keep every guarantee they have.
- Statements that said "a returned `Ok` means in the bucket" now say "except a background commit".
- The app must keep its own record of what it saved, and trim it only up to the durable position (the how-to and the explanation).
- `buckwal serve` allows the setting and adds `PRAGMA buckwal_durable_commit`, so a client of the serverless drivers can learn the position.
- A fence drops the waiting background commits and counts the drops in its `writer fenced` record.
