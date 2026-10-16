//! turso_durable: make a tursodb database durable in an S3 bucket.

pub(crate) mod bucket;
pub mod config;
pub(crate) mod engine;
pub mod error;
pub(crate) mod format;
pub(crate) mod io;
pub(crate) mod restore;
pub mod store;
pub(crate) mod trim;
pub(crate) mod uploader;

pub use error::{Error, Result};

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use turso_core::{Database, MemoryIO, Numeric, OpenOptions, SqliteDialect, Value};

    #[test]
    fn turso_core_links() {
        let io = Arc::new(MemoryIO::new());
        let opts = OpenOptions::new(Arc::new(SqliteDialect));
        let db = Database::open(io, ":memory:", opts).expect("open");
        let conn = db.connect().expect("connect");
        let mut stmt = conn.query("SELECT 1").expect("query").expect("statement");
        let rows = stmt.run_collect_rows().expect("rows");
        assert_eq!(rows, vec![vec![Value::Numeric(Numeric::Integer(1))]]);
    }
}
