//! [`Connection`]: the only way SQL reaches the engine. Every call clears the error stash,
//! runs the SQL guard, prepares, checks the sync mode (D7), binds, steps to completion, and on
//! failure returns the typed error the IO layer stashed (D8). [`Connection::describe`] stops
//! after the check: it never binds or steps.
//!
//! Every public call runs the engine inside a commit context (`batching::ctx`) holding its
//! effective batching setting, which the WAL sync reads. A call returns `Ok` only once every
//! commit it made is confirmed in the bucket (`await_acks`).
//!
//! `PRAGMA buckwal_batching` is run here, never by the engine: it reads the effective setting,
//! or sets the current transaction's own (`txn_batching`), which lasts until no transaction
//! is open.
//!
//! Every public call's engine work runs inside the maintenance gate (`maintenance::Gate`),
//! and so does an explicit transaction for as long as it is open (`txn_gate`); the ack wait
//! runs outside it. After each call, maintenance is woken if the generation is due.
use crate::batching::{ctx, record_outcome, Batching, PendingAck};
use crate::config::Event;
use crate::db::{lock, Shared};
use crate::engine::engine_error;
use crate::error::{Error, Result};
use crate::guard::{self, BatchingPragma};
use crate::io::db_storage::READ_ONLY;
use crate::io::stash;
use crate::maintenance::GateGuard;
use std::num::NonZero;
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use turso_core::{QueryMode, Statement, StepResult, SyncMode, Value, IO};

/// How long a statement waits for another connection's lock before it fails.
pub(crate) const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

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

/// One result column of [`Connection::run`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    /// The declared type of the table column it comes from; `None` for an expression.
    pub decltype: Option<String>,
}

/// What [`Connection::run`] returns.
#[derive(Debug, Clone, PartialEq)]
pub struct StmtOutput {
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<Value>>,
    /// Rows the statement changed (0 for a read).
    pub changes: u64,
    /// The connection's last inserted rowid, after the statement.
    pub last_insert_rowid: i64,
}

/// What [`Connection::describe`] returns: a statement's shape, without running it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Description {
    /// One per parameter slot `1..=n`: the name without its `:`, `@` or `$`; `None` for a
    /// positional one (`?`, `?NNN`).
    pub params: Vec<Option<String>>,
    pub columns: Vec<Column>,
    /// `EXPLAIN` or `EXPLAIN QUERY PLAN`.
    pub is_explain: bool,
    pub is_readonly: bool,
}

/// One connection to a [`crate::Database`]; open one per thread. Dropping it closes it.
pub struct Connection {
    /// None once closed.
    core: Option<Arc<turso_core::Connection>>,
    io: Arc<dyn IO>,
    /// This connection's own setting; `None` falls back to the database's.
    batching: Mutex<Option<Batching>>,
    /// The open transaction's own setting ([`Connection::transaction_with`], `PRAGMA
    /// buckwal_batching`), ahead of `batching`. Cleared by every call (or statement of a split
    /// `execute_batch`) that leaves no transaction open.
    txn_batching: Mutex<Option<Batching>>,
    /// The gate entry an open explicit transaction holds from the call that leaves it open
    /// to the call that ends it (or the close): maintenance never checkpoints under it, and
    /// its statements pass a closed gate.
    txn_gate: Mutex<Option<GateGuard>>,
    /// The number of this connection's last commit in this open (0 for none).
    last_commit: AtomicU64,
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
            txn_batching: Mutex::new(None),
            txn_gate: Mutex::new(None),
            last_commit: AtomicU64::new(0),
            shared,
        }
    }

    /// Commit with `b` from this connection's next commit on; `None` falls back to the
    /// database's setting ([`crate::Database::set_batching`]). A commit already waiting keeps
    /// the setting it was made with. `Config` when `b` is invalid.
    pub fn set_batching(&self, b: Option<Batching>) -> Result<()> {
        if let Some(b) = &b {
            b.validate()?;
            b.check_mode(self.shared.journal_mode)?;
        }
        *lock(&self.batching) = b;
        Ok(())
    }

    /// The number of this connection's last commit in this open, if it made one. Commit
    /// numbers restart at 1 with every open; compare this with
    /// [`crate::Database::durable_commit`].
    pub fn last_commit(&self) -> Option<u64> {
        Some(self.last_commit.load(SeqCst)).filter(|&n| n != 0)
    }

    /// This connection's own setting ([`Connection::set_batching`]).
    pub fn batching(&self) -> Option<Batching> {
        *lock(&self.batching)
    }

    /// The setting the next commit uses: the open transaction's, else the connection's, else
    /// the database's.
    fn effective_batching(&self) -> Batching {
        let txn = *lock(&self.txn_batching);
        txn.or_else(|| self.batching())
            .unwrap_or_else(|| self.shared.batching())
    }

    /// No transaction is open (COMMIT, ROLLBACK, the engine's own rollback, or none was):
    /// drop the transaction's setting, and commit the rest of the current call without it.
    fn end_txn_batching(&self) {
        let Some(core) = &self.core else {
            return;
        };
        if !core.get_auto_commit() {
            return;
        }
        let had = lock(&self.txn_batching).take().is_some();
        if had {
            ctx::set(self.effective_batching());
        }
    }

    /// Run `f` (one engine call) committing with `b`, inside the gate. On `Ok`, wait for
    /// every commit it made (outside the gate); on `Err`, its commits are not acked (their
    /// uploads go on) and `f`'s error is returned. The context is cleared on every path, a
    /// panic included, and the transaction's setting on every path that leaves no
    /// transaction open.
    fn call<T>(&self, b: Batching, f: impl FnOnce() -> Result<T>) -> Result<T> {
        let entered = self.enter_gate();
        let _ctx = ctx::enter(b);
        let r = f();
        if let Some(n) = ctx::last_commit() {
            self.last_commit.store(n, SeqCst);
        }
        self.end_txn_batching();
        self.leave_gate(entered);
        self.shared.poke_maintenance();
        let v = r?;
        self.await_acks()?;
        Ok(v)
    }

    /// Enter the gate, unless this connection's open transaction is already inside (`None`).
    fn enter_gate(&self) -> Option<GateGuard> {
        if lock(&self.txn_gate).is_some() {
            return None;
        }
        // Not under `txn_gate`'s lock: this blocks while maintenance runs.
        Some(self.shared.gate.enter())
    }

    /// After the engine work: a transaction left open keeps its entry; otherwise the call's
    /// entry and any transaction's are dropped.
    fn leave_gate(&self, entered: Option<GateGuard>) {
        let open_txn = self.core.as_ref().is_some_and(|c| !c.get_auto_commit());
        let mut held = lock(&self.txn_gate);
        if open_txn {
            if held.is_none() {
                *held = entered;
            }
        } else {
            *held = None;
        }
    }

    /// Wait, in commit order, for every commit the current call made. Each counts as acked
    /// (`UploadStats::commits_acked`, the wait histogram, `Event::CommitAcked`) only when all
    /// of them succeeded; otherwise the first error is returned. Each commit's span closes as
    /// its own ack resolves, with its own outcome; a wait is its `buckwal.ack_wait`.
    fn await_acks(&self) -> Result<()> {
        let mut acked = Vec::new();
        let mut first_err = None;
        for ack in ctx::take() {
            match ack {
                PendingAck::Done {
                    commit,
                    waited,
                    batching,
                    span,
                } => {
                    // Its outcome was recorded when its blocking wait returned, and its span ended
                    // with that sync's `local_fsync`; dropping it here changes neither.
                    drop(span);
                    acked.push((commit, waited, batching));
                }
                PendingAck::Background { max_behind } => {
                    // Its record is acked by the uploader when it lands. The call waits only
                    // while more than `max_behind` such commits wait. An error here comes
                    // after the local fsync: the commit stays in the local database, and a
                    // later upload carries it (outcome unknown, as a Window commit's error).
                    if let Err(e) = self.shared.uploader.wait_behind(max_behind) {
                        first_err.get_or_insert(e);
                    }
                }
                PendingAck::Waiting {
                    ticket,
                    batching,
                    span,
                } => {
                    let (commit, submitted) = (ticket.commit, ticket.submitted);
                    let r = ticket.wait_in(&span);
                    record_outcome(&span, &r);
                    match r {
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

    /// Guard, prepare, check and bind one statement, or run it here when it is `PRAGMA
    /// buckwal_batching` (the engine never sees that one). `single`: `sql` must hold only that
    /// statement (the engine would silently ignore the rest); otherwise it is the first of
    /// the rest of a split `execute_batch`.
    ///
    /// `named`: `None` binds `params` as exactly the statement's parameters (`execute`,
    /// `query`); `Some` binds them first, then the named ones, every slot once (`run`).
    fn prepare(
        &self,
        sql: &str,
        params: &[Value],
        named: Option<&[(String, Value)]>,
        single: bool,
    ) -> Result<Prepared> {
        stash::clear();
        let pragma = if single {
            guard::check_sql(sql, self.shared.guard_target())?;
            guard::batching_pragma(sql).map(|p| (p, sql.len()))
        } else {
            // execute_batch guard-checked the whole string before its first statement.
            guard::first_batching_pragma(sql)
        };
        if let Some((pragma, tail)) = pragma {
            check_params(params.len() + named.map_or(0, <[_]>::len), 0)?;
            let rows = self.batching_pragma(pragma)?;
            return Ok(Prepared::Batching { rows, tail });
        }
        let mut stmt = self.core().prepare(sql).map_err(engine_error)?;
        self.check_sync_mode()?;
        if single {
            check_single(sql, &stmt)?;
        }
        if self.shared.read_only && !stmt.get_program().is_readonly() {
            return Err(Error::Unsupported(READ_ONLY));
        }
        match named {
            None => {
                check_params(params.len(), stmt.parameters_count())?;
                for (i, v) in params.iter().enumerate() {
                    let at = NonZero::new(i + 1).expect("i + 1 > 0");
                    stmt.bind_at(at, v.clone()).map_err(engine_error)?;
                }
            }
            Some(named) => bind_all(&mut stmt, params, named)?,
        }
        Ok(Prepared::Engine(Box::new(stmt)))
    }

    /// Run `PRAGMA buckwal_batching`: a read is one row, the effective setting's text; a set
    /// applies to the open transaction from its next commit on, and returns no rows.
    fn batching_pragma(&self, pragma: BatchingPragma) -> Result<Vec<Vec<Value>>> {
        let b = match pragma {
            BatchingPragma::Read => {
                let text = self.effective_batching().to_string();
                return Ok(vec![vec![Value::from_text(text)]]);
            }
            BatchingPragma::Set(b) => b,
        };
        if self.core().get_auto_commit() {
            return Err(Error::Config(
                "PRAGMA buckwal_batching applies to the current transaction; use \
                 Connection::set_batching"
                    .into(),
            ));
        }
        let b = b?;
        b.validate()?;
        *lock(&self.txn_batching) = Some(b);
        ctx::set(b);
        Ok(Vec::new())
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

    /// Run one statement; returns the number of rows it changed (0 for `PRAGMA
    /// buckwal_batching`).
    pub fn execute(&self, sql: &str, params: &[Value]) -> Result<u64> {
        self.call(self.effective_batching(), || {
            let Prepared::Engine(mut stmt) = self.prepare(sql, params, None, true)? else {
                return Ok(0);
            };
            self.step_all(&mut stmt, |_| {})?;
            Ok(stmt.n_change().max(0) as u64)
        })
    }

    /// Run every statement in `sql`, with no parameters. The whole string is guard-checked
    /// first, so a refused statement anywhere means none of them runs.
    pub fn execute_batch(&self, sql: &str) -> Result<()> {
        self.call(self.effective_batching(), || {
            stash::clear();
            // A transaction with its own setting may end inside the batch, and the commits
            // after that must not keep it.
            let split = self.shared.read_only
                || guard::mentions_batching(sql)
                || lock(&self.txn_batching).is_some();
            if split {
                guard::check_sql_split(sql, self.shared.guard_target())?;
                return self.execute_batch_split(sql);
            }
            guard::check_sql(sql, self.shared.guard_target())?;
            let r = self.core().prepare_execute_batch(sql).map_err(engine_error);
            // Defence in depth: the guard above refuses every spelling of a lower
            // `synchronous`, but the batch prepares and runs its statements in one engine
            // call, so this can only look afterwards (and put FULL back for the next call).
            self.check_sync_mode()?;
            r
        })
    }

    /// One statement at a time, so each is checked as read-only before it runs, a `PRAGMA
    /// buckwal_batching` is run in place, and a transaction's setting ends with it.
    fn execute_batch_split(&self, sql: &str) -> Result<()> {
        let mut rest = sql;
        while holds_a_statement(rest) {
            let tail = match self.prepare(rest, &[], None, false)? {
                Prepared::Engine(mut stmt) => {
                    self.step_all(&mut stmt, |_| {})?;
                    stmt.tail_offset()
                }
                Prepared::Batching { tail, .. } => tail,
            };
            self.end_txn_batching();
            rest = &rest[tail.clamp(1, rest.len())..];
        }
        Ok(())
    }

    /// Run one statement and collect its rows.
    pub fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Vec<Value>>> {
        self.call(self.effective_batching(), || {
            let mut stmt = match self.prepare(sql, params, None, true)? {
                Prepared::Engine(stmt) => stmt,
                Prepared::Batching { rows, .. } => return Ok(rows),
            };
            let mut rows = Vec::new();
            self.step_all(&mut stmt, |r| rows.push(r))?;
            Ok(rows)
        })
    }

    /// Run one statement and return its columns, rows, changed-row count and last inserted
    /// rowid. `args` bind parameters 1..=n in order, then `named` bind the rest by name: with
    /// its prefix (`:a`, `@b`, `$c`, `?4`), or bare, which tries `:`, `@`, `$` in that order.
    /// Every parameter must be bound exactly once, or it is `Config`. The SQL guard, the
    /// maintenance gate and the ack wait are those of [`Connection::execute`].
    pub fn run(&self, sql: &str, args: &[Value], named: &[(String, Value)]) -> Result<StmtOutput> {
        self.call(self.effective_batching(), || {
            let mut stmt = match self.prepare(sql, args, Some(named), true)? {
                Prepared::Engine(stmt) => stmt,
                Prepared::Batching { rows, .. } => {
                    return Ok(StmtOutput {
                        columns: vec![batching_column()],
                        rows,
                        changes: 0,
                        last_insert_rowid: self.core().last_insert_rowid(),
                    });
                }
            };
            let columns = columns(&stmt);
            let mut rows = Vec::new();
            self.step_all(&mut stmt, |r| rows.push(r))?;
            Ok(StmtOutput {
                columns,
                rows,
                changes: stmt.n_change().max(0) as u64,
                last_insert_rowid: self.core().last_insert_rowid(),
            })
        })
    }

    /// Describe one statement without running it: its parameters, its result columns, and
    /// whether it is an `EXPLAIN` and read-only. The SQL guard, the sync-mode check (D7) and
    /// the maintenance gate are those of [`Connection::run`]. The statement is prepared but
    /// never stepped, so nothing commits and nothing waits for an ack; it works on a
    /// read-only database too. `PRAGMA buckwal_batching` is not run, only described as one
    /// `buckwal_batching` column. Setter PRAGMAs that turso_core applies at prepare (e.g.
    /// `query_only`, `foreign_keys`) take effect on describe, as they do on `run`; the
    /// durability ones are refused by the guard. An `EXPLAIN` is read-only, as in SQLite.
    pub fn describe(&self, sql: &str) -> Result<Description> {
        self.call(self.effective_batching(), || {
            stash::clear();
            guard::check_sql(sql, self.shared.guard_target())?;
            if guard::batching_pragma(sql).is_some() {
                return Ok(Description {
                    params: Vec::new(),
                    columns: vec![batching_column()],
                    is_explain: false,
                    is_readonly: false,
                });
            }
            let stmt = self.core().prepare(sql).map_err(engine_error)?;
            self.check_sync_mode()?;
            check_single(sql, &stmt)?;
            let params = stmt.parameters();
            let params = (1..=stmt.parameters_count())
                .map(|i| param_name(params.name(NonZero::new(i).expect("slots start at 1"))))
                .collect();
            let is_explain = stmt.get_query_mode() != QueryMode::Normal;
            Ok(Description {
                params,
                columns: columns(&stmt),
                is_explain,
                is_readonly: is_explain || stmt.get_program().is_readonly(),
            })
        })
    }

    /// No transaction is open: true outside `BEGIN`…`COMMIT`, and again after the engine
    /// rolled a transaction back by itself or an upload failure did. In MVCC mode a `COMMIT`
    /// whose upload failed also leaves this true, but the commit was not rolled back: it is
    /// kept in the local database and shipped by the next commit (outcome unknown).
    pub fn is_autocommit(&self) -> bool {
        self.core.as_ref().is_none_or(|c| c.get_auto_commit())
    }

    /// `BEGIN`, then `f`, then `COMMIT`; `ROLLBACK` when `f` or the commit fails, and `f`'s
    /// (or the commit's) error is returned. Each statement is its own call, so the `COMMIT`
    /// returns only once the transaction is confirmed in the bucket.
    ///
    /// A commit error is rolled back locally in WAL mode with batching Off. It is not in MVCC
    /// mode: there the engine keeps a commit whose upload failed (the transaction has already
    /// ended), and the error means "outcome unknown" (the next commit ships it).
    pub fn transaction<T>(&self, f: impl FnOnce(&Transaction<'_>) -> Result<T>) -> Result<T> {
        self.run_transaction(None, f)
    }

    /// [`Connection::transaction`] committing with `b`, whatever this connection's and the
    /// database's settings are; the next transaction or statement is back to them. `Config`
    /// when `b` is invalid, `Unsupported` for a Window in an MVCC database, both before
    /// anything runs.
    pub fn transaction_with<T>(
        &self,
        b: Batching,
        f: impl FnOnce(&Transaction<'_>) -> Result<T>,
    ) -> Result<T> {
        b.validate()?;
        b.check_mode(self.shared.journal_mode)?;
        self.run_transaction(Some(b), f)
    }

    fn run_transaction<T>(
        &self,
        b: Option<Batching>,
        f: impl FnOnce(&Transaction<'_>) -> Result<T>,
    ) -> Result<T> {
        self.execute("BEGIN", &[])?;
        if b.is_some() {
            *lock(&self.txn_batching) = b;
        }
        let r = f(&Transaction { conn: self }).and_then(|v| self.execute("COMMIT", &[]).map(|_| v));
        if r.is_err() {
            // A failed commit may already have rolled back; that ROLLBACK's error is moot.
            let _ = self.execute("ROLLBACK", &[]);
        }
        // The COMMIT or ROLLBACK cleared it already, unless the ROLLBACK could not run.
        *lock(&self.txn_batching) = None;
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
        // `call` dropped it already (no core is left), unless it panicked.
        *lock(&self.txn_gate) = None;
        self.shared.connection_closed();
        r
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        let _ = self.finish();
    }
}

/// One prepared statement, or a `PRAGMA buckwal_batching` already run.
enum Prepared {
    /// Boxed: a `Statement` is large, and the enum is moved by value.
    Engine(Box<Statement>),
    /// The PRAGMA's rows, and the offset in the text just past it.
    Batching { rows: Vec<Vec<Value>>, tail: usize },
}

/// `sql` holds only `stmt` (the engine would silently ignore the rest).
fn check_single(sql: &str, stmt: &Statement) -> Result<()> {
    if holds_a_statement(sql.get(stmt.tail_offset()..).unwrap_or("")) {
        return Err(Error::Config(
            "execute and query run one statement; use execute_batch for several".into(),
        ));
    }
    Ok(())
}

/// `stmt`'s result columns.
fn columns(stmt: &Statement) -> Vec<Column> {
    (0..stmt.num_columns())
        .map(|i| Column {
            name: stmt.get_column_name(i).into_owned(),
            decltype: stmt.get_column_decltype(i),
        })
        .collect()
}

/// The one column `PRAGMA buckwal_batching` reads.
fn batching_column() -> Column {
    Column {
        name: "buckwal_batching".into(),
        decltype: None,
    }
}

/// A parameter's name as PROTOCOL.md 6.4 gives it: `:id` is `id`; a positional one (`?`,
/// whose engine name is `None`, or `?NNN`) has none.
fn param_name(engine: Option<String>) -> Option<String> {
    engine?.strip_prefix([':', '@', '$']).map(str::to_string)
}

fn check_params(given: usize, want: usize) -> Result<()> {
    if given != want {
        return Err(Error::Config(format!(
            "{given} parameters given; the statement takes {want}"
        )));
    }
    Ok(())
}

/// Bind `args` at 1..=n, then each `named` by its slot; every slot `1..=parameters_count()`
/// must end up bound exactly once.
fn bind_all(stmt: &mut Statement, args: &[Value], named: &[(String, Value)]) -> Result<()> {
    let count = stmt.parameters_count();
    if args.len() > count {
        return Err(Error::Config(format!(
            "{} positional arguments given; the statement takes {count} parameters",
            args.len()
        )));
    }
    let mut bound = vec![false; count + 1];
    let slots = args.iter().enumerate().map(|(i, v)| (i + 1, v));
    let mut binds: Vec<(usize, &Value)> = slots.collect();
    for (name, v) in named {
        let at = param_slot(stmt, name)?;
        binds.push((at, v));
    }
    for (at, v) in binds {
        if at > count || std::mem::replace(&mut bound[at], true) {
            return Err(Error::Config(format!(
                "parameter {at} is bound twice (or is out of range)"
            )));
        }
        let at = NonZero::new(at).expect("slots start at 1");
        stmt.bind_at(at, v.clone()).map_err(engine_error)?;
    }
    if let Some(missing) = (1..=count).find(|i| !bound[*i]) {
        return Err(Error::Config(format!("parameter {missing} is not bound")));
    }
    Ok(())
}

/// The slot `name` stands for: as written when it has a prefix, else with `:`, `@`, `$` tried
/// in that order.
fn param_slot(stmt: &Statement, name: &str) -> Result<usize> {
    let params = stmt.parameters();
    let found = if name.starts_with([':', '@', '$', '?']) {
        params.index(name)
    } else {
        [':', '@', '$']
            .iter()
            .find_map(|p| params.index(format!("{p}{name}")))
    };
    found
        .map(NonZero::get)
        .ok_or_else(|| Error::Config(format!("the statement has no parameter named {name:?}")))
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

    fn open_mem() -> (tempfile::TempDir, crate::Database) {
        let t = tempfile::tempdir().unwrap();
        let c = Config::new(t.path(), "http://127.0.0.1:1", "b", "k", "s");
        let db = crate::open_with_store(c, Arc::new(MemStore::new())).unwrap();
        (t, db)
    }

    fn int(n: i64) -> Value {
        Value::from_i64(n)
    }

    fn named(n: &str, v: i64) -> (String, Value) {
        (n.to_string(), int(v))
    }

    fn ints(rows: &[Vec<Value>]) -> Vec<Vec<i64>> {
        rows.iter()
            .map(|r| r.iter().map(|v| v.as_int().unwrap()).collect())
            .collect()
    }

    #[test]
    fn run_returns_columns_rows_changes_and_rowid() {
        let (_t, db) = open_mem();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, body TEXT)", &[])
            .unwrap();
        let out = conn
            .run(
                "INSERT INTO t(body) VALUES (?)",
                &[Value::from_text("a".to_string())],
                &[],
            )
            .unwrap();
        assert_eq!((out.changes, out.last_insert_rowid), (1, 1));
        assert!(out.columns.is_empty());
        let out = conn
            .run("SELECT id, body AS b, 1+1 FROM t", &[], &[])
            .unwrap();
        let col = |n: &str, d: Option<&str>| Column {
            name: n.to_string(),
            decltype: d.map(str::to_string),
        };
        assert_eq!(
            out.columns,
            vec![
                col("id", Some("INTEGER")),
                col("b", Some("TEXT")),
                col("1+1", None)
            ]
        );
        assert_eq!(
            out.rows,
            vec![vec![int(1), Value::from_text("a".to_string()), int(2)]]
        );
    }

    #[test]
    fn run_binds_named_parameters_with_and_without_prefix() {
        let (_t, db) = open_mem();
        let conn = db.connect().unwrap();
        let named = [
            named("a", 1),
            named("@b", 2),
            named("$c", 3),
            named("?4", 4),
        ];
        let out = conn.run("SELECT :a, @b, $c, ?4", &[], &named).unwrap();
        assert_eq!(ints(&out.rows), vec![vec![1, 2, 3, 4]]);
    }

    #[test]
    fn run_mixes_positional_then_named() {
        let (_t, db) = open_mem();
        let conn = db.connect().unwrap();
        let out = conn
            .run("SELECT ?, :x", &[int(1)], &[named("x", 2)])
            .unwrap();
        assert_eq!(ints(&out.rows), vec![vec![1, 2]]);
    }

    #[test]
    fn run_refuses_bad_bindings() {
        let (_t, db) = open_mem();
        let conn = db.connect().unwrap();
        type Case = (&'static str, Vec<Value>, Vec<(String, Value)>);
        let cases: [Case; 5] = [
            ("SELECT :x", vec![], vec![named("nope", 1)]),
            ("SELECT :x", vec![int(1)], vec![named("x", 2)]),
            ("SELECT ?, ?", vec![int(1)], vec![]),
            ("SELECT ?", vec![int(1), int(2)], vec![]),
            ("SELECT 1", vec![], vec![named("x", 1)]),
        ];
        for (sql, args, named) in cases {
            let r = conn.run(sql, &args, &named);
            assert!(matches!(r, Err(Error::Config(_))), "{sql}: {r:?}");
        }
        assert_eq!(conn.query("SELECT 1", &[]).unwrap().len(), 1);
    }

    #[test]
    fn run_takes_one_statement() {
        let (_t, db) = open_mem();
        let conn = db.connect().unwrap();
        let r = conn.run("SELECT 1; SELECT 2", &[], &[]);
        assert!(matches!(r, Err(Error::Config(_))), "{r:?}");
    }

    #[test]
    fn run_guard_checks_text_whatever_the_args() {
        let (_t, db) = open_mem();
        let conn = db.connect().unwrap();
        let text = |s: &str| ("f".to_string(), Value::from_text(s.to_string()));
        let r = conn.run("ATTACH :f AS x", &[], &[text("x.db")]);
        assert_eq!(r, Err(Error::Unsupported("ATTACH")));
        // The guard reads the text before any binding: a literal it refuses is refused with
        // arguments that bind nothing, too.
        let r = conn.run("PRAGMA journal_mode = delete", &[], &[text("delete")]);
        assert_eq!(r, Err(Error::Unsupported("journal_mode other than WAL")));
        let r = conn.run("PRAGMA hexkey = '00'", &[int(1)], &[text("00")]);
        assert!(matches!(r, Err(Error::Unsupported(_))), "{r:?}");
        // A parameter in a PRAGMA value is not valid SQL for the engine, so it never runs.
        for sql in ["PRAGMA journal_mode = :f", "PRAGMA hexkey = :f"] {
            assert!(conn.run(sql, &[], &[text("delete")]).is_err(), "{sql}");
        }
        conn.execute("BEGIN", &[]).unwrap();
        let r = conn.run("PRAGMA buckwal_batching = off", &[], &[text("off")]);
        assert!(matches!(r, Err(Error::Config(_))), "{r:?}");
        assert!(conn
            .run("PRAGMA buckwal_batching = :f", &[], &[text("off")])
            .is_err());
        conn.execute("ROLLBACK", &[]).unwrap();
        assert_eq!(conn.query("SELECT 1", &[]).unwrap().len(), 1);
    }

    #[test]
    fn run_pragma_buckwal_batching_reads_with_a_column() {
        let (_t, db) = open_mem();
        let conn = db.connect().unwrap();
        let out = conn.run("PRAGMA buckwal_batching", &[], &[]).unwrap();
        assert_eq!(
            out.columns,
            vec![Column {
                name: "buckwal_batching".to_string(),
                decltype: None
            }]
        );
        assert_eq!(out.rows, vec![vec![Value::from_text("off".to_string())]]);
        assert_eq!(out.changes, 0);
    }

    fn col(n: &str, d: Option<&str>) -> Column {
        Column {
            name: n.to_string(),
            decltype: d.map(str::to_string),
        }
    }

    /// A database whose `Event::CommitAcked`s are counted.
    fn open_counting_acks() -> (tempfile::TempDir, crate::Database, Arc<Mutex<u64>>) {
        let t = tempfile::tempdir().unwrap();
        let mut c = Config::new(t.path(), "http://127.0.0.1:1", "b", "k", "s");
        let acks = Arc::new(Mutex::new(0));
        let seen = acks.clone();
        c.on_event = Some(Arc::new(move |e: &Event| {
            if matches!(e, Event::CommitAcked { .. }) {
                *seen.lock().unwrap() += 1;
            }
        }));
        let db = crate::open_with_store(c, Arc::new(MemStore::new())).unwrap();
        (t, db, acks)
    }

    #[test]
    fn describe_gives_params_columns_and_flags() {
        let (_t, db) = open_mem();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT)", &[])
            .unwrap();
        let d = conn
            .describe("SELECT id, name FROM t WHERE id = :id")
            .unwrap();
        assert_eq!(
            d,
            Description {
                params: vec![Some("id".to_string())],
                columns: vec![col("id", Some("INTEGER")), col("name", Some("TEXT"))],
                is_explain: false,
                is_readonly: true,
            }
        );
        let d = conn.describe("DELETE FROM t").unwrap();
        assert!(!d.is_readonly && !d.is_explain, "{d:?}");
        let d = conn.describe("EXPLAIN SELECT 1").unwrap();
        assert!(d.is_explain, "{d:?}");
        let d = conn.describe("SELECT ?, ?2").unwrap();
        assert_eq!(d.params, vec![None, None]);
        let d = conn.describe("SELECT @a, $b, :c").unwrap();
        let names = ["a", "b", "c"].map(|n| Some(n.to_string()));
        assert_eq!(d.params, names);
    }

    #[test]
    fn describe_runs_the_guard_and_keeps_sync_mode_full() {
        let (_t, db) = open_mem();
        let conn = db.connect().unwrap();
        for sql in [
            "PRAGMA synchronous=OFF",
            "PRAGMA journal_mode='mvcc'",
            "ATTACH 'x.db' AS x",
        ] {
            let r = conn.describe(sql);
            assert!(matches!(r, Err(Error::Unsupported(_))), "{sql}: {r:?}");
            assert_eq!(conn.core().get_sync_mode(), SyncMode::Full, "{sql}");
            let rows = conn.query("PRAGMA synchronous", &[]).unwrap();
            assert_eq!(rows[0][0].as_int(), Some(2), "{sql}");
        }
        // D7's backstop runs after the prepare, as for `run`.
        conn.core().set_sync_mode(SyncMode::Off);
        assert_eq!(
            conn.describe("SELECT 1"),
            Err(Error::Unsupported("synchronous below FULL"))
        );
        assert_eq!(conn.core().get_sync_mode(), SyncMode::Full);
    }

    /// The guard runs before the engine's prepare, which would apply a key or cipher change
    /// (translate/pragma.rs:644-655; on a plain database the cipher does change). The
    /// connection keeps its settings and keeps reading and writing.
    #[test]
    fn describe_refuses_encryption_pragmas_before_the_prepare() {
        for encrypted in [false, true] {
            let t = tempfile::tempdir().unwrap();
            let mut c = Config::new(t.path().join("db"), "http://127.0.0.1:1", "b", "k", "s");
            if encrypted {
                let key = t.path().join("key");
                std::fs::write(&key, "11".repeat(32)).unwrap();
                c.encryption_key_file = Some(key);
            }
            let db = crate::open_with_store(c, Arc::new(MemStore::new())).unwrap();
            let conn = db.connect().unwrap();
            conn.execute("CREATE TABLE t(x)", &[]).unwrap();
            conn.execute("INSERT INTO t VALUES (1), (2)", &[]).unwrap();
            let cipher = |c: &Connection| c.query("PRAGMA cipher", &[]).unwrap();
            let before = cipher(&conn);
            let what = "encryption PRAGMAs (use BUCKWAL_ENCRYPTION_KEY_FILE)";
            for sql in [
                "PRAGMA cipher = 'aes256gcm'".to_string(),
                format!("PRAGMA hexkey = '{}'", "22".repeat(32)),
            ] {
                let r = conn.describe(&sql);
                assert_eq!(r, Err(Error::Unsupported(what)), "{encrypted} {sql}");
            }
            assert_eq!(cipher(&conn), before, "{encrypted}");
            let n = conn.query("SELECT count(*) FROM t", &[]).unwrap();
            assert_eq!(n[0][0].as_int(), Some(2), "{encrypted}");
            conn.execute("INSERT INTO t VALUES (3)", &[]).unwrap();
            let n = db.connect().unwrap().query("SELECT count(*) FROM t", &[]);
            assert_eq!(n.unwrap()[0][0].as_int(), Some(3), "{encrypted}");
        }
    }

    /// `EXPLAIN` runs nothing, so it is read-only whatever it explains, as in SQLite.
    #[test]
    fn describe_of_explain_is_readonly() {
        let (_t, db) = open_mem();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(x)", &[]).unwrap();
        for sql in [
            "EXPLAIN DELETE FROM t",
            "EXPLAIN QUERY PLAN DELETE FROM t",
            "EXPLAIN INSERT INTO t VALUES (1)",
        ] {
            let d = conn.describe(sql).unwrap();
            assert!(d.is_explain && d.is_readonly, "{sql}: {d:?}");
        }
    }

    #[test]
    fn describe_never_steps_or_acks() {
        let (_t, db, acks) = open_counting_acks();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(x)", &[]).unwrap();
        let (events, stats) = (*acks.lock().unwrap(), db.upload_stats().commits_acked);
        assert!(events > 0 && stats > 0, "the hooks see a commit");
        let d = conn.describe("INSERT INTO t VALUES (1)").unwrap();
        assert!(!d.is_readonly, "{d:?}");
        assert!(d.params.is_empty() && d.columns.is_empty(), "{d:?}");
        assert_eq!(*acks.lock().unwrap(), events);
        assert_eq!(db.upload_stats().commits_acked, stats);
        let n = conn.query("SELECT count(*) FROM t", &[]).unwrap();
        assert_eq!(n[0][0].as_int(), Some(0));
    }

    #[test]
    fn describe_takes_one_statement() {
        let (_t, db) = open_mem();
        let conn = db.connect().unwrap();
        let r = conn.describe("SELECT 1; SELECT 2");
        assert!(matches!(r, Err(Error::Config(_))), "{r:?}");
        let run = conn.run("SELECT 1; SELECT 2", &[], &[]);
        assert_eq!(r.map(|_| ()), run.map(|_| ()));
    }

    #[test]
    fn describe_of_buckwal_batching_is_fixed_and_applies_nothing() {
        let (_t, db) = open_mem();
        let conn = db.connect().unwrap();
        let fixed = Description {
            params: vec![],
            columns: vec![col("buckwal_batching", None)],
            is_explain: false,
            is_readonly: false,
        };
        let setting = |c: &Connection| c.query("PRAGMA buckwal_batching", &[]).unwrap();
        // Outside a transaction a set is refused by `run`; describe still describes it.
        for sql in ["PRAGMA buckwal_batching", "PRAGMA buckwal_batching = 'off'"] {
            assert_eq!(conn.describe(sql), Ok(fixed.clone()), "{sql}");
        }
        conn.execute("BEGIN", &[]).unwrap();
        let before = setting(&conn);
        let d = conn.describe("PRAGMA buckwal_batching = 'window 40ms 64 1MiB'");
        assert_eq!(d, Ok(fixed));
        assert_eq!(setting(&conn), before);
        assert_eq!(*lock(&conn.txn_batching), None);
        conn.execute("ROLLBACK", &[]).unwrap();
    }

    #[test]
    fn describe_works_on_a_read_only_database() {
        let store = Arc::new(MemStore::new());
        let w = tempfile::tempdir().unwrap();
        let c = Config::new(w.path(), "http://127.0.0.1:1", "b", "k", "s");
        let db = crate::open_with_store(c, store.clone()).unwrap();
        db.connect()
            .unwrap()
            .execute("CREATE TABLE t(x)", &[])
            .unwrap();
        db.close().unwrap();
        let r = tempfile::tempdir().unwrap();
        let c = Config::new(r.path(), "http://127.0.0.1:1", "b", "k", "s");
        let db = crate::open_read_only_with_store(c, store).unwrap();
        let conn = db.connect().unwrap();
        let d = conn.describe("INSERT INTO t VALUES (1)").unwrap();
        assert!(!d.is_readonly, "{d:?}");
        assert!(conn.describe("SELECT x FROM t").unwrap().is_readonly);
        assert_eq!(
            conn.execute("INSERT INTO t VALUES (1)", &[]),
            Err(Error::Unsupported(READ_ONLY))
        );
    }

    #[test]
    fn is_autocommit_tracks_begin_commit_rollback() {
        let (_t, db) = open_mem();
        let conn = db.connect().unwrap();
        assert!(conn.is_autocommit());
        conn.execute("BEGIN", &[]).unwrap();
        assert!(!conn.is_autocommit());
        conn.execute("COMMIT", &[]).unwrap();
        assert!(conn.is_autocommit());
        conn.execute("BEGIN", &[]).unwrap();
        assert!(!conn.is_autocommit());
        conn.execute("ROLLBACK", &[]).unwrap();
        assert!(conn.is_autocommit());
    }

    #[test]
    fn is_autocommit_after_engine_side_rollback() {
        let (_t, db) = open_mem();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, body TEXT)", &[])
            .unwrap();
        conn.execute(
            "CREATE TRIGGER r BEFORE INSERT ON t WHEN NEW.body = 'boom' \
             BEGIN SELECT RAISE(ROLLBACK, 'no'); END",
            &[],
        )
        .unwrap();
        conn.execute("BEGIN", &[]).unwrap();
        conn.execute("INSERT INTO t(body) VALUES ('ok')", &[])
            .unwrap();
        assert!(!conn.is_autocommit());
        let r = conn.execute("INSERT INTO t(body) VALUES ('boom')", &[]);
        assert!(r.is_err(), "{r:?}");
        assert!(conn.is_autocommit());
        let n = conn.query("SELECT count(*) FROM t", &[]).unwrap();
        assert_eq!(n[0][0].as_int(), Some(0));
    }

    #[test]
    fn is_autocommit_after_upload_failure_rollback() {
        use crate::store::fault::FaultStore;
        let t = tempfile::tempdir().unwrap();
        let mem = Arc::new(MemStore::new());
        let fault = Arc::new(FaultStore::new(mem));
        let mut c = Config::new(t.path(), "http://127.0.0.1:1", "b", "k", "s");
        c.retry_deadline = Duration::from_millis(200);
        c.request_timeout = Duration::from_millis(50);
        let db = crate::open_with_store(c, fault.clone()).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("CREATE TABLE t(x)", &[]).unwrap();
        fault.only_keys_containing("log/");
        conn.execute("BEGIN", &[]).unwrap();
        conn.execute("INSERT INTO t VALUES (1)", &[]).unwrap();
        fault.fail_next_puts_permanently(1);
        let r = conn.execute("COMMIT", &[]);
        assert!(
            matches!(
                r,
                Err(Error::UploadFailed(_) | Error::Timeout(_) | Error::Fenced(_))
            ),
            "{r:?}"
        );
        assert!(conn.is_autocommit());
        let n = conn.query("SELECT count(*) FROM t", &[]).unwrap();
        assert_eq!(n[0][0].as_int(), Some(0));
    }
}
