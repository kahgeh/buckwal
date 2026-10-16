//! [`Connection`]: the only way SQL reaches the engine. Every call clears the error stash,
//! runs the SQL guard, prepares, checks the sync mode (D7), binds, steps to completion, and on
//! failure returns the typed error the IO layer stashed (D8).
//!
//! Every public call runs the engine inside a commit context (`batching::ctx`) holding its
//! effective batching setting, which the WAL sync reads. A call returns `Ok` only once every
//! commit it made is confirmed in the bucket (`await_acks`).
use crate::batching::{ctx, Batching, PendingAck};
use crate::config::Event;
use crate::db::{lock, Shared};
use crate::engine::engine_error;
use crate::error::{Error, Result};
use crate::guard;
use crate::io::db_storage::READ_ONLY;
use crate::io::stash;
use std::num::NonZero;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use turso_core::{Statement, StepResult, SyncMode, Value, IO};

/// How long a statement waits for another connection's lock before it fails.
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// How much a [`Connection::checkpoint`] does; the modes of SQLite's `wal_checkpoint`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointMode {
    Passive,
    Full,
    Restart,
    Truncate,
}

impl CheckpointMode {
    fn core(self) -> turso_core::CheckpointMode {
        match self {
            CheckpointMode::Passive => turso_core::CheckpointMode::Passive {
                upper_bound_inclusive: None,
            },
            CheckpointMode::Full => turso_core::CheckpointMode::Full,
            CheckpointMode::Restart => turso_core::CheckpointMode::Restart,
            CheckpointMode::Truncate => turso_core::CheckpointMode::Truncate {
                upper_bound_inclusive: None,
            },
        }
    }
}

/// One connection to a [`crate::Database`]; open one per thread. Dropping it closes it.
pub struct Connection {
    /// None once closed.
    core: Option<Arc<turso_core::Connection>>,
    io: Arc<dyn IO>,
    /// This connection's own setting; `None` falls back to the database's.
    batching: Mutex<Option<Batching>>,
    /// Declared last, so dropped last: when this holds the last `Arc<Shared>`, its drop
    /// releases `durable.lock` and the process registry, and every engine file handle (the
    /// connection's, and the IO's `app.db` and `-wal` through its storage) must be closed
    /// by then.
    shared: Arc<Shared>,
}

impl Connection {
    /// Wrap a new engine connection: busy timeout, `synchronous=FULL`, and on a read-only
    /// open no automatic checkpoint or WAL restart (so its close never checkpoints).
    pub(crate) fn new(
        shared: Arc<Shared>,
        core: Arc<turso_core::Connection>,
        io: Arc<dyn IO>,
    ) -> Connection {
        core.set_busy_timeout(BUSY_TIMEOUT);
        core.set_sync_mode(SyncMode::Full);
        if shared.read_only {
            core.wal_auto_actions_disable();
        }
        Connection {
            core: Some(core),
            io,
            batching: Mutex::new(None),
            shared,
        }
    }

    /// Commit with `b` from this connection's next commit on; `None` falls back to the
    /// database's setting ([`crate::Database::set_batching`]). A commit already waiting keeps
    /// the setting it was made with. `Config` when `b` is invalid.
    pub fn set_batching(&self, b: Option<Batching>) -> Result<()> {
        if let Some(b) = &b {
            b.validate()?;
        }
        *lock(&self.batching) = b;
        Ok(())
    }

    /// This connection's own setting ([`Connection::set_batching`]).
    pub fn batching(&self) -> Option<Batching> {
        *lock(&self.batching)
    }

    /// The setting the next commit uses: the connection's, else the database's.
    fn effective_batching(&self) -> Batching {
        // Task 6: a transaction's own setting comes first.
        self.batching().unwrap_or_else(|| self.shared.batching())
    }

    /// Run `f` (one engine call) committing with `b`. On `Ok`, wait for every commit it
    /// made; on `Err`, its commits are not acked (their uploads go on) and `f`'s error is
    /// returned. The context is cleared on every path, a panic included.
    fn call<T>(&self, b: Batching, f: impl FnOnce() -> Result<T>) -> Result<T> {
        let _ctx = ctx::enter(b);
        let v = f()?;
        self.await_acks()?;
        Ok(v)
    }

    /// Wait, in commit order, for every commit the current call made. Each counts as acked
    /// (`UploadStats::commits_acked`, the wait histogram, `Event::CommitAcked`) only when all
    /// of them succeeded; otherwise the first error is returned.
    fn await_acks(&self) -> Result<()> {
        let mut acked = Vec::new();
        let mut first_err = None;
        for ack in ctx::take() {
            match ack {
                PendingAck::Done {
                    commit,
                    waited,
                    batching,
                } => acked.push((commit, waited, batching)),
                PendingAck::Waiting { ticket, batching } => {
                    let (commit, submitted) = (ticket.commit, ticket.submitted);
                    match ticket.wait() {
                        Ok(_) => acked.push((commit, submitted.elapsed(), batching)),
                        Err(e) => {
                            first_err.get_or_insert(e);
                        }
                    }
                }
            }
        }
        if let Some(e) = first_err {
            return Err(e);
        }
        for (commit, waited, batching) in acked {
            self.shared.stats.record_ack(waited);
            self.shared.emit(Event::CommitAcked { commit, batching });
        }
        Ok(())
    }

    fn core(&self) -> &Arc<turso_core::Connection> {
        self.core
            .as_ref()
            .expect("a Connection is open until it is consumed")
    }

    /// D7: a statement that lowered `synchronous` at prepare is dropped and the mode restored.
    fn check_sync_mode(&self) -> Result<()> {
        let core = self.core();
        if core.get_sync_mode() != SyncMode::Full {
            core.set_sync_mode(SyncMode::Full);
            return Err(Error::Unsupported("synchronous below FULL"));
        }
        Ok(())
    }

    /// Guard, prepare, check and bind one statement. `single`: `sql` must hold only that
    /// statement (the engine would silently ignore the rest).
    fn prepare(&self, sql: &str, params: &[Value], single: bool) -> Result<Statement> {
        stash::clear();
        guard::check_sql(sql)?;
        let mut stmt = self.core().prepare(sql).map_err(engine_error)?;
        self.check_sync_mode()?;
        if single && holds_a_statement(sql.get(stmt.tail_offset()..).unwrap_or("")) {
            return Err(Error::Config(
                "execute and query run one statement; use execute_batch for several".into(),
            ));
        }
        if self.shared.read_only && !stmt.get_program().is_readonly() {
            return Err(Error::Unsupported(READ_ONLY));
        }
        let want = stmt.parameters_count();
        if params.len() != want {
            return Err(Error::Config(format!(
                "{} parameters given; the statement takes {want}",
                params.len()
            )));
        }
        for (i, v) in params.iter().enumerate() {
            let at = NonZero::new(i + 1).expect("i + 1 > 0");
            stmt.bind_at(at, v.clone()).map_err(engine_error)?;
        }
        Ok(stmt)
    }

    /// Step `stmt` to completion, handing each row to `on_row`.
    fn step_all(&self, stmt: &mut Statement, mut on_row: impl FnMut(Vec<Value>)) -> Result<()> {
        loop {
            match stmt.step().map_err(engine_error)? {
                StepResult::Done => return Ok(()),
                StepResult::Row => {
                    let row = stmt.row().expect("a Row step has a row");
                    on_row(row.get_values().cloned().collect());
                }
                StepResult::IO | StepResult::Yield => self.io.step().map_err(engine_error)?,
                StepResult::Sleep { duration } => {
                    std::thread::sleep(duration.min(Duration::from_millis(50)));
                    self.io.step().map_err(engine_error)?;
                }
                StepResult::Busy => {
                    return Err(Error::Engine("database is busy (locked)".into()));
                }
                StepResult::Interrupt => return Err(Error::Engine("interrupted".into())),
            }
        }
    }

    /// Run one statement; returns the number of rows it changed.
    pub fn execute(&self, sql: &str, params: &[Value]) -> Result<u64> {
        self.call(self.effective_batching(), || {
            let mut stmt = self.prepare(sql, params, true)?;
            self.step_all(&mut stmt, |_| {})?;
            Ok(stmt.n_change().max(0) as u64)
        })
    }

    /// Run every statement in `sql`, with no parameters. The whole string is guard-checked
    /// first, so a refused statement anywhere means none of them runs.
    pub fn execute_batch(&self, sql: &str) -> Result<()> {
        self.call(self.effective_batching(), || {
            stash::clear();
            guard::check_sql(sql)?;
            if self.shared.read_only {
                return self.execute_batch_read_only(sql);
            }
            let r = self.core().prepare_execute_batch(sql).map_err(engine_error);
            // Defence in depth: the guard above refuses every spelling of a lower
            // `synchronous`, but the batch prepares and runs its statements in one engine
            // call, so this can only look afterwards (and put FULL back for the next call).
            self.check_sync_mode()?;
            r
        })
    }

    /// One statement at a time, so each is checked as read-only before it runs.
    fn execute_batch_read_only(&self, sql: &str) -> Result<()> {
        let mut rest = sql;
        while holds_a_statement(rest) {
            let mut stmt = self.prepare(rest, &[], false)?;
            self.step_all(&mut stmt, |_| {})?;
            let tail = stmt.tail_offset().clamp(1, rest.len());
            rest = &rest[tail..];
        }
        Ok(())
    }

    /// Run one statement and collect its rows.
    pub fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Vec<Value>>> {
        self.call(self.effective_batching(), || {
            let mut stmt = self.prepare(sql, params, true)?;
            let mut rows = Vec::new();
            self.step_all(&mut stmt, |r| rows.push(r))?;
            Ok(rows)
        })
    }

    /// `BEGIN`, then `f`, then `COMMIT`; `ROLLBACK` when `f` or the commit fails, and `f`'s
    /// (or the commit's) error is returned. Each statement is its own call, so the `COMMIT`
    /// returns only once the transaction is confirmed in the bucket.
    pub fn transaction<T>(&self, f: impl FnOnce(&Transaction<'_>) -> Result<T>) -> Result<T> {
        self.execute("BEGIN", &[])?;
        let r = f(&Transaction { conn: self }).and_then(|v| self.execute("COMMIT", &[]).map(|_| v));
        if r.is_err() {
            // A failed commit may already have rolled back; that ROLLBACK's error is moot.
            let _ = self.execute("ROLLBACK", &[]);
        }
        r
    }

    /// Checkpoint the WAL into the database file. TRUNCATE also ends the generation inside
    /// this call: its pages are uploaded first (a seal). RESTART only resets the WAL in memory,
    /// so the seal happens in the next commit (which also gets any upload error).
    pub fn checkpoint(&self, mode: CheckpointMode) -> Result<()> {
        self.call(Batching::Off, || {
            stash::clear();
            if self.shared.read_only {
                return Err(Error::Unsupported(READ_ONLY));
            }
            self.core()
                .checkpoint(mode.core())
                .map(|_| ())
                .map_err(engine_error)
        })
    }

    /// Close the connection. The last connection to close checkpoints the WAL (a writer only).
    pub fn close(mut self) -> Result<()> {
        self.finish()
    }

    fn finish(&mut self) -> Result<()> {
        let Some(core) = self.core.take() else {
            return Ok(());
        };
        let r = self.call(Batching::Off, || {
            stash::clear();
            core.close().map_err(engine_error)
        });
        drop(core);
        self.shared.connection_closed();
        r
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        let _ = self.finish();
    }
}

/// `rest` holds another statement (not just whitespace, `;` or comments). Text that does not
/// parse counts as one, so it is reported rather than skipped.
fn holds_a_statement(rest: &str) -> bool {
    turso_parser::parser::Parser::new(rest.as_bytes())
        .next()
        .is_some()
}

/// A transaction in progress; see [`Connection::transaction`].
pub struct Transaction<'a> {
    conn: &'a Connection,
}

impl Transaction<'_> {
    /// [`Connection::execute`] inside the transaction.
    pub fn execute(&self, sql: &str, params: &[Value]) -> Result<u64> {
        self.conn.execute(sql, params)
    }
    /// [`Connection::query`] inside the transaction.
    pub fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Vec<Value>>> {
        self.conn.query(sql, params)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::store::MemStore;

    /// D7's backstop: a connection whose sync mode was lowered (the guard refuses every SQL
    /// spelling of it) has its next statement refused and the mode put back.
    #[test]
    fn lowered_sync_mode_is_restored_and_refused() {
        let t = tempfile::tempdir().unwrap();
        let c = Config::new(t.path(), "http://127.0.0.1:1", "b", "k", "s");
        let db = crate::open_with_store(c, Arc::new(MemStore::new())).unwrap();
        let conn = db.connect().unwrap();
        conn.core().set_sync_mode(SyncMode::Off);
        assert_eq!(
            conn.query("SELECT 1", &[]),
            Err(Error::Unsupported("synchronous below FULL"))
        );
        assert_eq!(conn.core().get_sync_mode(), SyncMode::Full);
        conn.core().set_sync_mode(SyncMode::Normal);
        assert_eq!(
            conn.execute_batch("SELECT 1"),
            Err(Error::Unsupported("synchronous below FULL"))
        );
        assert_eq!(conn.query("SELECT 1", &[]).unwrap().len(), 1);
    }

    #[test]
    fn parameter_count_must_match() {
        let t = tempfile::tempdir().unwrap();
        let c = Config::new(t.path(), "http://127.0.0.1:1", "b", "k", "s");
        let db = crate::open_with_store(c, Arc::new(MemStore::new())).unwrap();
        let conn = db.connect().unwrap();
        let r = conn.query("SELECT ?", &[]);
        assert!(matches!(r, Err(Error::Config(_))), "{r:?}");
        let r = conn.query("SELECT 1", &[Value::from_i64(1)]);
        assert!(matches!(r, Err(Error::Config(_))), "{r:?}");
        let rows = conn.query("SELECT ? + 1", &[Value::from_i64(41)]).unwrap();
        assert_eq!(rows[0][0].as_int(), Some(42));
    }

    #[test]
    fn execute_and_query_take_one_statement() {
        let t = tempfile::tempdir().unwrap();
        let c = Config::new(t.path(), "http://127.0.0.1:1", "b", "k", "s");
        let db = crate::open_with_store(c, Arc::new(MemStore::new())).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(x);  -- trailing comment\n ; ", &[])
            .unwrap();
        let r = conn.execute("INSERT INTO t VALUES (1); INSERT INTO t VALUES (2)", &[]);
        assert!(matches!(r, Err(Error::Config(_))), "{r:?}");
        let r = conn.query("SELECT 1; SELECT 2", &[]);
        assert!(matches!(r, Err(Error::Config(_))), "{r:?}");
        assert_eq!(
            conn.query("SELECT count(*) FROM t", &[]).unwrap()[0][0].as_int(),
            Some(0)
        );
    }
}
