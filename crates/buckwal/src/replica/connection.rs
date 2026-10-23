//! [`ReadConnection`]: a replica's connection for apps. It is a [`Connection`] of a read-only
//! database (turso_core's `is_readonly` check on the prepared program plus the SQL guard, as
//! the read-only open uses), so INSERT/UPDATE/DELETE, DDL, PRAGMA writes, ATTACH and
//! checkpoints are refused. It also refuses `PRAGMA buckwal_batching`: a replica commits
//! nothing.
use crate::conn::{Connection, Description, StmtOutput};
use crate::error::{Error, Result};
use crate::guard;
use crate::Value;

const BATCHING: &str = "replica: PRAGMA buckwal_batching";

/// One read-only connection to a [`crate::Replica`]; open one per thread. Dropping it closes it.
pub struct ReadConnection {
    inner: Connection,
}

fn refuse_batching(sql: &str) -> Result<()> {
    if guard::mentions_batching(sql) {
        return Err(Error::Unsupported(BATCHING));
    }
    Ok(())
}

impl ReadConnection {
    pub(crate) fn new(inner: Connection) -> ReadConnection {
        ReadConnection { inner }
    }

    /// Run one statement and collect its rows. A statement that writes is `Unsupported`.
    pub fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Vec<Value>>> {
        refuse_batching(sql)?;
        self.inner.query(sql, params)
    }

    /// Run one statement; as [`Connection::run`].
    pub fn run(&self, sql: &str, args: &[Value], named: &[(String, Value)]) -> Result<StmtOutput> {
        refuse_batching(sql)?;
        self.inner.run(sql, args, named)
    }

    /// Describe one statement without running it; as [`Connection::describe`].
    pub fn describe(&self, sql: &str) -> Result<Description> {
        refuse_batching(sql)?;
        self.inner.describe(sql)
    }

    /// No transaction is open.
    pub fn is_autocommit(&self) -> bool {
        self.inner.is_autocommit()
    }

    /// `BEGIN`, then `f`, then `COMMIT`: a read transaction, so `f` sees one snapshot of the
    /// replica. It holds the local WAL against a checkpoint while it runs.
    pub fn transaction<T>(&self, f: impl FnOnce(&ReadTransaction<'_>) -> Result<T>) -> Result<T> {
        self.inner
            .transaction(|_| f(&ReadTransaction { conn: self }))
    }

    pub fn close(self) -> Result<()> {
        self.inner.close()
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
