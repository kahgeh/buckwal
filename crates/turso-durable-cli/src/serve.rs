//! `serve`: the Hrana server of `turso-durable-server` over a database opened as the writer.
use crate::{open_writer, say};
use std::path::{Path, PathBuf};
use turso_durable::Batching;
use turso_durable_server::{bind, os_signals, ServeConfig};

/// Why `serve` ended without a clean stop.
#[derive(Debug, PartialEq, Eq)]
pub enum Failure {
    /// The settings are wrong (exit 2).
    Usage(String),
    /// A refused directory, an open error or an unclean stop (exit 1).
    Failed(String),
}

impl Failure {
    pub fn exit_code(&self) -> u8 {
        match self {
            Failure::Usage(_) => 2,
            Failure::Failed(_) => 1,
        }
    }

    pub fn message(&self) -> &str {
        match self {
            Failure::Usage(m) | Failure::Failed(m) => m,
        }
    }
}

pub fn run(
    dir: &Path,
    listen: Option<&str>,
    claim: bool,
    batching: Option<Batching>,
) -> Result<(), Failure> {
    let cfg = ServeConfig::from_env(listen).map_err(Failure::Usage)?;
    // Before any TLS or token file is read.
    cfg.check_bind().map_err(Failure::Usage)?;
    // Before binding, so a refused directory never touches the bucket or the port. The
    // message is the one `sql` gives.
    if !claim {
        crate::claim_check(dir).map_err(Failure::Failed)?;
    }
    let bound = bind(cfg).map_err(Failure::Failed)?;
    say(&format!(
        "serve listening addr={} tls={} auth={}",
        bound.local_addr(),
        bound.tls(),
        bound.auth()
    ));
    let dir: PathBuf = dir.to_path_buf();
    let report = bound
        .serve(
            move || open_writer(&dir, claim, batching),
            |db| say(&format!("serve ready generation={}", db.generation())),
            async { os_signals().await },
        )
        .map_err(Failure::Failed)?;
    say(&format!(
        "serve stopped clean={} in_flight_failed={} streams_closed={}",
        report.clean, report.in_flight_failed, report.streams_closed
    ));
    if report.clean {
        return Ok(());
    }
    Err(Failure::Failed(
        "shutdown was not clean: a request or the final upload did not finish in time".to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serve_refuses_non_loopback_without_tls_and_token() {
        let t = tempfile::tempdir().unwrap();
        let e = run(t.path(), Some("0.0.0.0:0"), false, None).unwrap_err();
        assert_eq!(e.exit_code(), 2, "{e:?}");
        for var in [
            "DURABLE_SERVE_TLS_CERT",
            "DURABLE_SERVE_TLS_KEY",
            "DURABLE_SERVE_TOKEN_FILE",
        ] {
            assert!(e.message().contains(var), "{}", e.message());
        }
    }

    #[test]
    fn serve_refuses_an_unclaimed_directory_without_claim() {
        let t = tempfile::tempdir().unwrap();
        for dir in [t.path().to_path_buf(), t.path().join("missing")] {
            let e = run(&dir, Some("127.0.0.1:0"), false, None).unwrap_err();
            assert_eq!(e.exit_code(), 1, "{e:?}");
            let sql_e = crate::sql(&dir, &["SELECT 1".to_string()], false, None).unwrap_err();
            assert_eq!(e.message(), sql_e, "same message as sql");
        }
        assert!(!t.path().join("missing").exists(), "created the directory");
    }
}
