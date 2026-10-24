# Background saves

A background save is a commit made with the `background` batching setting. This page gives the reasons behind the steps in [Catch up after background saves](../how-tos/catch-up-after-background-saves.md). The guarantees are in the README, Guarantees, and the setting is in the README, Background commits.

## What changes, and what does not

An `Off` commit returns `Ok` only when its log record is in the bucket. A background commit returns `Ok` once its frames are fsynced on the local disk, and its record is uploaded behind the call. The upload takes about as long as an `Off` commit takes, so the call is faster by that time. On a laptop against RustFS, the commit p50 fell from 1.32 ms to 0.049 ms (`results/background-commit.txt`).

The returned `Ok` is not an ack. A commit is acked when its record is in the bucket, and `Database::durable_commit` names the highest such commit. Every guarantee of an acked commit stays as it is.

## Why the bucket holds a prefix

The uploader uploads records in commit order. A record is uploaded only after every record before it, and a record that fails stays owed and goes first in the next upload. The bucket therefore holds every commit up to some point and none after it. A restore returns exactly that prefix.

## What a lost machine loses

A lost machine loses only the newest background commits. Once `max_behind` background commits wait for the bucket, the next commit waits for the oldest to land. So the loss is bounded by `max_behind` commits.

A process that dies on a disk that survives loses nothing. The local `-wal` holds every fsynced commit, and the next writer open uploads the commits missing from the bucket before it opens.

If another machine takes over while background commits wait, the fenced old writer never uploads those commits. The new writer's restore holds everything up to the durable position.

## Why trimming stops at the durable position

A message that the app drops from its own log must be in the bucket. Only then can a restore on a new machine return it. The app trims a message once the durable position reaches the commit that applied it. A message trimmed earlier could be in neither the bucket nor the log if the machine were lost.

## Why the message id is in the same transaction

The message id and the data commit together, so they are always in the same commit. After a restore, the database holds the id of the last message that its restored data contains. The app replays the messages after that id. It does not need commit numbers: they restart at 1 with every open.

## While the bucket fails

During a bucket outage, a commit made with the background setting is an `Off` commit. It uploads before its local fsync, behind the commits still owed, and returns `Ok` only once it is acked. An outage therefore shows at once as errors, and the gap does not grow. See also the README, Guarantees.
