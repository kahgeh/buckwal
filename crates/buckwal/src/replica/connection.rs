//! [`ReadConnection`]: a replica's connection for apps. It is a [`Connection`] of a read-only
//! database (turso_core's `is_readonly` check on the prepared program plus the SQL guard, as
//! the read-only open uses), so INSERT/UPDATE/DELETE, DDL, PRAGMA writes, ATTACH and
//! checkpoints are refused. It also refuses `PRAGMA buckwal_batching`: a replica commits
//! nothing.
use std::sync::{Arc, Mutex};

use crate::conn::{Connection, Description, StmtOutput};
use crate::db::Database;
use crate::error::{Error, Result};
use crate::guard;
use crate::Value;

const BATCHING: &str = "replica: PRAGMA buckwal_batching";

/// The `Database` of the replica's current copy, and a count of how many copies there have
/// been. `Replica::connect` and every [`ReadConnection`] read it.
pub(crate) struct Current {
    inner: Mutex<(u64, Database)>,
}

impl Current {
    pub(crate) fn new(db: Database) -> Current {
        Current {
            inner: Mutex::new((0, db)),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, (u64, Database)> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub(crate) fn get(&self) -> (u64, Database) {
        let g = self.lock();
        (g.0, g.1.clone())
    }

    pub(crate) fn epoch(&self) -> u64 {
        self.lock().0
    }

    /// Switch to the next copy; returns the previous `Database`.
    pub(crate) fn swap(&self, db: Database) -> Database {
        let mut g = self.lock();
        g.0 += 1;
        std::mem::replace(&mut g.1, db)
    }
}

struct Bound {
    epoch: u64,
    conn: Arc<Connection>,
}

/// One read-only connection to a [`crate::Replica`]; open one per thread. Dropping it closes it.
///
/// After a re-restore the replica has a new copy. A statement (or `transaction`) that starts
/// with no transaction open first rebinds the connection to the new copy, so it never serves
/// frozen data. A connection inside a transaction keeps its old snapshot until the
/// transaction ends (the old engine serves it from its own files); it gets an error only if
/// the old engine can no longer serve it.
pub struct ReadConnection {
    current: Arc<Current>,
    bound: Mutex<Bound>,
}

fn refuse_batching(sql: &str) -> Result<()> {
    if guard::mentions_batching(sql) {
        return Err(Error::Unsupported(BATCHING));
    }
    Ok(())
}

impl ReadConnection {
    pub(crate) fn new(current: Arc<Current>) -> Result<ReadConnection> {
        let (epoch, db) = current.get();
        let conn = Arc::new(db.connect()?);
        Ok(ReadConnection {
            current,
            bound: Mutex::new(Bound { epoch, conn }),
        })
    }

    /// The connection to run on: rebound to the current copy if it changed and no
    /// transaction is open.
    fn conn(&self) -> Result<Arc<Connection>> {
        let mut b = self.bound.lock().unwrap_or_else(|e| e.into_inner());
        if self.current.epoch() != b.epoch && b.conn.is_autocommit() {
            let (epoch, db) = self.current.get();
            b.conn = Arc::new(db.connect()?);
            b.epoch = epoch;
        }
        Ok(b.conn.clone())
    }

    /// Run one statement and collect its rows. A statement that writes is `Unsupported`.
    pub fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Vec<Value>>> {
        refuse_batching(sql)?;
        self.conn()?.query(sql, params)
    }

    /// Run one statement; as [`Connection::run`].
    pub fn run(&self, sql: &str, args: &[Value], named: &[(String, Value)]) -> Result<StmtOutput> {
        refuse_batching(sql)?;
        self.conn()?.run(sql, args, named)
    }

    /// Describe one statement without running it; as [`Connection::describe`].
    pub fn describe(&self, sql: &str) -> Result<Description> {
        refuse_batching(sql)?;
        self.conn()?.describe(sql)
    }

    /// No transaction is open.
    pub fn is_autocommit(&self) -> bool {
        let b = self.bound.lock().unwrap_or_else(|e| e.into_inner());
        b.conn.is_autocommit()
    }

    /// `BEGIN`, then `f`, then `COMMIT`: a read transaction, so `f` sees one snapshot of the
    /// replica. It holds the local WAL against a checkpoint while it runs.
    pub fn transaction<T>(&self, f: impl FnOnce(&ReadTransaction<'_>) -> Result<T>) -> Result<T> {
        let conn = self.conn()?;
        conn.transaction(|_| f(&ReadTransaction { conn: self }))
    }

    pub fn close(self) -> Result<()> {
        let b = self.bound.into_inner().unwrap_or_else(|e| e.into_inner());
        match Arc::try_unwrap(b.conn) {
            Ok(c) => c.close(),
            Err(_) => Ok(()),
        }
    }
}

/// The inside of [`ReadConnection::transaction`].
pub struct ReadTransaction<'a> {
    conn: &'a ReadConnection,
}

impl ReadTransaction<'_> {
    pub fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Vec<Value>>> {
        self.conn.query(sql, params)
    }
}
