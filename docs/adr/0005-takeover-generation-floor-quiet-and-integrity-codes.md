# 0005. Takeover generation floor, takeover quiet from both writers, and integrity codes on encrypted buckets

- Status: Accepted
- Date: 2026-10-23
- Spec: `durable-hardening`
- Decision record in the README: "A late PUT from the fenced writer" in Guarantees and limits, the takeover quiet, Encryption (D16) and Bucket format v3

## Context

A fenced old writer can still send late PUTs after a takeover. On S3 and RustFS a single-part PUT's ETag is the MD5 of its body, so a late conditional PUT can replace a key of the new writer when the bytes are identical. The takeover quiet was sized from the new writer's own timeouts only. Encrypted pages are bound neither to their page number nor to a version, so someone with write access to the bucket can swap or replay objects without an error. An older build would ignore new head fields and misread or corrupt a bucket with integrity codes.

## Decision

- The takeover's head write carries `next_generation_floor` = `max(G + 1, replaced head's floor) + 1`, kept also in the local state file. A seal moves the head to `T = max(G + 1, floor)` and writes its pages and manifest under `T - 1`, as a seal always has (a seal of G writes `pages/G` and `manifest/G`, then moves the head to G+1). With no takeover `T - 1` is G. After a takeover the new writer's first seal writes under G+1 and moves the head to G+2 naming manifest G+1, so the keys the fenced writer can write (under G) are never referenced; GC trims them later. The floor stays in every later head, and GC ignores any manifest below `floor - 1` while the field is present, so it never trims against the fenced writer's leftover `manifest/G` (which would otherwise be a candidate once the head names manifest G+1 and could delete pages the new manifest maps). A head without the field keeps today's rule. No log object is ever written under G+1. The WAL seal and the MVCC `DurableStorage::truncate` seal with its adopt-a-published-head recovery use the same rule. Every reader accepts a head whose generation went up by 2.
- Every head carries the writer's `lease_ms` and `request_timeout_ms`. The takeover quiet is the larger of the old writer's sum and its own, capped at `MAX_SEAL_QUIET`.
- Encrypted buckets carry a 32-byte HMAC-SHA256 integrity code on each page object, log record, manifest and head, under an integrity key derived from the encryption key with HKDF-SHA256 (written over `hmac` and `sha2`, checked against RFC 5869 vectors). A failed check is `Corrupt("integrity: <object key>")` and an ERROR record.
- A format 3 head carries `key_check`, 16 bytes of HMAC-SHA256 under the integrity key over a fixed label, compared at open before any integrity code. A mismatch is an `Encryption` wrong-key error with no ERROR record and no alert; only a matching `key_check` makes a bad code `Corrupt` and the alert. A key against a format 1 or 2 head is also an `Encryption` error. These errors apply only at open (a one-shot GC run counts as an open). After a good open, a head read that finds a format 1 or 2 head (a downgrade), a changed `key_check` or a bad head code is `Corrupt` plus the ERROR record and the alert.
- An encrypted bucket's head is `"format":3`, so older builds refuse it before reading or writing. Encrypted buckets from before are refused, not migrated.

## Consequences

- A late PUT from a fenced writer can only land on keys nothing references. GC trims them.
- Between a takeover and the new writer's first seal GC trims nothing (every manifest is below `floor - 1`).
- Generation numbers of heads and manifests are no longer consecutive; nothing may assume `G + 1`, and a manifest generation can be missing.
- Replaying a whole older consistent bucket state is not detected; that needs state kept outside the bucket.
- Bucket format gains a third head format number; each reader must name the formats it accepts.
- One alert, severity `page` (any integrity failure outside tests), is added; it is expected never to fire.
