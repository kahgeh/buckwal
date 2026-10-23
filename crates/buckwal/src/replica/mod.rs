//! Read replicas: restore, then follow the bucket's log with a follower thread.

pub(crate) mod apply;
pub(crate) mod connection;
pub(crate) mod fence;
pub(crate) mod image;
pub(crate) mod open;
pub(crate) mod position;
pub(crate) mod stats;
pub(crate) mod sync;
pub(crate) mod waker;

pub use connection::{ReadConnection, ReadTransaction};
pub use open::{open_replica, open_replica_with_store, Replica};
pub use stats::ReplicaStats;
pub use waker::ReplicaWaker;
