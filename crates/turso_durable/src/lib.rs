//! turso_durable: make a tursodb database durable in an S3 bucket.

pub mod batching;
pub(crate) mod bucket;
pub(crate) mod clock;
pub mod config;
pub(crate) mod conn;
pub(crate) mod db;
pub(crate) mod encryption;
pub(crate) mod engine;
pub mod error;
pub(crate) mod files;
pub(crate) mod format;
pub(crate) mod gc;
pub(crate) mod guard;
pub(crate) mod io;
pub(crate) mod maintenance;
pub(crate) mod reconcile;
pub(crate) mod restore;
pub mod stats;
pub mod store;
pub(crate) mod takeover;
pub mod telemetry;
pub(crate) mod trim;
pub(crate) mod uploader;

pub use batching::Batching;
pub use config::Config;
/// Test and diagnostics hooks (`Config::on_event`), not part of the supported API.
#[doc(hidden)]
pub use config::{Event, EventHook};
pub use conn::{CheckpointMode, Column, Connection, StmtOutput, Transaction};
pub use db::{is_claimed, open, Database};
#[doc(hidden)]
pub use db::{open_read_only, open_read_only_with_store, open_with_store};
pub use error::{Error, Result};
#[doc(hidden)]
pub use gc::gc_with_store;
pub use gc::{gc, GcOutcome, GcReport};
#[doc(hidden)]
pub use restore::restore_with_store;
pub use restore::{restore, RestoreReport};
pub use stats::UploadStats;
#[doc(hidden)]
pub use takeover::takeover_with_store;
pub use takeover::{takeover, TakeoverReport};
pub use turso_core::Value;

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

    #[test]
    fn encryption_feature_is_compiled_in() {
        use turso_core::{CipherMode, EncryptionContext, EncryptionKey};
        let key = EncryptionKey::new_256([7; 32]);
        let ctx = EncryptionContext::new(CipherMode::Aegis256, &key, 4096).unwrap();
        let mut page = vec![0u8; 4096];
        page[..6].copy_from_slice(b"marker"); // reserved tail stays zero
        let enc = ctx.encrypt_page(&page, 2).expect("encryption compiled in");
        assert!(!enc.windows(6).any(|w| w == b"marker"));
        assert_eq!(ctx.decrypt_page(&enc, 2).unwrap(), page);
    }
}
