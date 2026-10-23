# 0006. A third IO mode for replicas

- Status: Accepted
- Date: 2026-10-23
- Spec: `read-replicas`
- Decision record in the README: Replicas (and Limits, in that section)

## Context

The library's IO layer has a writer mode (uploads, seals, claims, GC) and a read-only mode (refuses local writes). A replica follows the bucket by applying frames to its local database and checkpointing it, so it needs local writes but must never write to the bucket.

## Decision

Add a replica mode beside the other two. Local WAL and `app.db` writes are allowed for the follower only, pages are read lazily from the bucket until the prefetch has them, and nothing is uploaded, sealed, claimed or deleted. A replica sends GET, HEAD and LIST requests only. The follower lives inside `buckwal` because the engine type it needs is crate-private, and it uses turso_core's `conn_raw_api` frame calls, pinned by `turso_core =0.8.3`.

## Consequences

- Neither existing mode is relaxed, so the writer's and `verify`'s guarantees are unchanged.
- The bucket receives zero PUTs and zero DELETEs from a replica, and a test counts them.
- The design depends on a non-public engine API: the probes must be re-run on any turso_core upgrade.
