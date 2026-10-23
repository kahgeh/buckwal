# 0007. The notification listener reuses the server crate's listener

- Status: Accepted
- Date: 2026-10-23
- Spec: `read-replicas`
- Decision record in the README: Replicas, Where the notification comes from

## Context

Replicas are woken by a bucket event. On AWS, through an SNS HTTPS subscription (and on RustFS, untested), that arrives as an HTTP request, so the replica side needs a listener. `buckwal-server` already has one with TLS, a bind check and a token, and the library has no async runtime.

## Decision

The listener lives in `crates/buckwal-server` as a `notify` module and reuses that crate's listener, bind check and token. The library exposes `Replica::waker()` and gains no server code. It has no default address: it runs only when `BUCKWAL_REPLICA_NOTIFY_LISTEN` is set. A loopback address needs neither TLS nor the token, and a non-loopback address is refused without both. The token is compared in constant time, never logged, and a bad request gets 401 and wakes nothing. A notification is only a hint: it starts a sync pass that reads the log by seq.

## Consequences

- One listener and one set of security rules for every server in the repo.
- `buckwal-cli` depends on the server crate to run `follow` with a listener.
- Lost, late, duplicate and out-of-order events are harmless, so the slow poll keeps the `trim_grace` bound.
