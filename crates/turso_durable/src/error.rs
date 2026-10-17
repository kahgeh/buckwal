// Derived from tursodatabase/turso spike/objstore (MIT), commit 417bd81c0.
//! The crate-wide error type.
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    Fenced(String),
    Timeout(String),
    NotFound(String),
    Unsupported(&'static str),
    UploadFailed(String),
    Locked(String),
    AlreadyOpen(String),
    Corrupt(String),
    Bucket(String),
    Engine(String),
    Io(String),
    Config(String),
    Encryption(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Fenced(m) => write!(f, "fenced by a newer writer: {m}"),
            Error::Timeout(m) => write!(f, "timed out: {m}"),
            Error::NotFound(m) => write!(f, "not found: {m}"),
            Error::Unsupported(feat) => write!(f, "{feat} is not supported yet by turso_durable"),
            Error::UploadFailed(m) => write!(f, "upload failed: {m}"),
            Error::Locked(m) => write!(f, "locked: {m}"),
            Error::AlreadyOpen(m) => write!(f, "already open: {m}"),
            Error::Corrupt(m) => write!(f, "corrupt: {m}"),
            Error::Bucket(m) => write!(f, "bucket error: {m}"),
            Error::Engine(m) => write!(f, "engine error: {m}"),
            Error::Io(m) => write!(f, "io error: {m}"),
            Error::Config(m) => write!(f, "config error: {m}"),
            Error::Encryption(m) => write!(f, "encryption error: {m}"),
        }
    }
}

impl std::error::Error for Error {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsupported_display() {
        assert_eq!(
            Error::Unsupported("vacuum").to_string(),
            "vacuum is not supported yet by turso_durable"
        );
    }
}
