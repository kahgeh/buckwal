# 0009. Integrity codes are bound to a database id

- Status: Accepted
- Date: 2026-10-25
- Spec: `bind-objects-to-database`
- Decision record in the README: Encryption (what the codes do not cover) and Bucket format v3

## Context

Integrity codes bind a page, a log record or a manifest to its generation, page number and seq, but not to its database. Two encrypted databases that share a key file therefore accept each other's objects at the same coordinates, in another prefix or in another bucket. The workaround was one key file per database.

The prefix cannot be the binding: a whole database moved to another prefix or bucket (for example with `aws s3 sync`) must still open.

## Decision

- A new encrypted database gets a random database id (16 bytes, hex) at its first head write. Every later head keeps it. The head becomes format 4: format 3 plus `database_id`.
- Every integrity code of a format 4 database (page, log record, manifest, head) is made under a database integrity key, derived with HKDF-SHA256 from the integrity key and the id. The parts of each code do not change. `key_check` stays under the integrity key, so a wrong key is still an `Encryption` error and never an integrity failure.
- The keys object learns the format and the id from the first head it reads or writes, and every clone shares that knowledge. A later head with another id, or of the other format, is tampering: `Corrupt("integrity: <head key>")`, and `format downgraded` for a format 3 head over a format 4 database.
- Format 3 databases stay format 3, with the old limit. There is no migration: an upgrade would need a full seal of every page, a new command and its own crash matrix.
- A whole head copied over another database's head carries its own id and verifies. The failure comes at the first read of an object that the target wrote under its own id. If the target holds no object, the open shows an empty database that is now the source's. This case is accepted and documented.

## Consequences

- An object copied in from another database fails its code, even under one key file.
- A whole database copied with its head to another prefix or bucket still opens and restores.
- Older builds refuse a format 4 head before they read or write anything.
- `buckwal verify` prints the head's format and, for format 4, the database id.
- Existing encrypted buckets keep format 3 and its documented limit.
- Two encrypted MVCC creators that race at one prefix: the loser's pages and manifest, coded under its own id, can land after the winner's. The winner puts its own again once its head has landed, so the objects the head names are its own. A loser slow enough to land after that leaves an empty database that fails its first restore with `integrity`; it holds no data, and a create at an emptied prefix replaces it.
