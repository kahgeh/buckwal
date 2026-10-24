# Catch up after background saves

This page gives the steps for an app that keeps its own replayable message log and commits with the `background` batching setting. The steps keep every message: a message is either in the database or still in the app's log. The reasons are in [Background saves](../explanations/background-saves.md). The setting is described in the README, Background commits.

The app pattern has three parts. The first two run in the app's code. The third trims the app's log.

## 1. Record the message id in the same transaction as each save

Each transaction that applies messages also updates one row that holds the id of the last message applied.

```rust
conn.execute_batch("CREATE TABLE IF NOT EXISTS replay(id INTEGER PRIMARY KEY CHECK (id = 1), last_message INTEGER NOT NULL)")?;
conn.execute_batch("INSERT OR IGNORE INTO replay VALUES (1, 0)")?;

// For each message, the data and the message id commit together.
conn.transaction(|tx| {
    tx.execute("INSERT INTO orders(id, item) VALUES (?, ?)", &[Value::from_i64(id), Value::from_text(item)])?;
    tx.execute("UPDATE replay SET last_message = ? WHERE id = 1", &[Value::from_i64(message_id)])?;
    Ok(())
})?;
```

Commit numbers are not the app's position. They restart at 1 with every open, so the app never stores a commit number as its position.

## 2. Catch up on every start

App catch-up is a normal step of every start, and not a recovery mode. It reads `last_message` from the database, compares it with the end of the message log, and replays every message after it.

```rust
let db = buckwal::open(config)?;               // or a restore on a new machine, then a takeover
let conn = db.connect()?;
conn.set_batching(Some("background 5ms 64 1MiB 1000".parse()?))?;

let rows = conn.query("SELECT last_message FROM replay WHERE id = 1", &[])?;
let last = rows[0][0].as_int().unwrap_or(0);
for message in message_log.after(last) {
    apply(&conn, message)?;                    // step 1, one transaction per message
}
```

- After a normal restart on the same disk, catch-up finds nothing to replay. A restart on the same disk keeps every commit: the open uploads the commits found in the local `-wal` before it returns.
- After a restore on a new machine, catch-up replays exactly the commits that never reached the bucket.
- For an app that talks to `buckwal serve`, catch-up also runs on every reconnect. A reconnect covers the case where the server moved to a new machine while the app kept running. A changed `open_id` (step 3) tells the client that the server reopened.

## 3. Trim the message log only up to the durable position

Once the durable position reaches the commit that applied a message, the message may be dropped from the app's log. The durable position is for trimming only. The app does not need it before a write, and it does not need it for catch-up.

With the library, the app takes the commit number from `Connection::last_commit()` after its commit and compares it with `Database::durable_commit()`:

```rust
conn.transaction(|tx| { /* step 1 */ Ok(()) })?;
let commit = conn.last_commit().expect("the transaction made a commit");
pending.push_back((commit, message_id));            // numbers belong to this open

// Later, on any thread:
let durable = db.durable_commit();
while let Some(&(commit, message_id)) = pending.front() {
    if commit > durable { break; }
    message_log.trim_up_to(message_id);
    pending.pop_front();
}
```

- `Database::wait_durable(commit, timeout)` blocks until a commit is durable. It returns `Timeout`, `UploadFailed` while the bucket fails, or `Fenced`.
- Commit numbers belong to one open. The app forgets the commit numbers it holds at every start, and trims the older messages once a later commit is durable.

Over HTTP, `PRAGMA buckwal_durable_commit` returns one row, `open_id`, `last_commit` and `durable_commit`. The client reads it in the same pipeline as a write, so it costs no extra request, and `last_commit` is always that write's. The data and the message id commit together, in one transaction.

```json
{"requests": [
  {"type": "execute", "stmt": {"sql": "BEGIN"}},
  {"type": "execute", "stmt": {"sql": "INSERT INTO orders(id, item) VALUES (7, 'book')"}},
  {"type": "execute", "stmt": {"sql": "UPDATE replay SET last_message = 7 WHERE id = 1"}},
  {"type": "execute", "stmt": {"sql": "COMMIT"}},
  {"type": "execute", "stmt": {"sql": "PRAGMA buckwal_durable_commit"}},
  {"type": "close"}
]}
```

The client keeps the `open_id` it saw. A different `open_id` means that the server reopened: the client runs step 2, forgets the commit numbers it holds, and keeps its own position in the database.

## What buckwal does not do

buckwal never leaves a gap. The bucket always holds every commit up to some point and none after it, so catch-up replays a contiguous run of messages. A gap appears only when a message is trimmed before its commit is durable. If the machine is then lost, that message is in neither the bucket nor the log.
