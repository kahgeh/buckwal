//! `serve`: the Hrana server of `turso-durable-server` over a database opened as the writer.
use crate::{open_writer, say};
use std::path::{Path, PathBuf};
use turso_durable::Batching;
use turso_durable_server::{bind, os_signals, ServeConfig, ShutdownReport};

/// Why `serve` ended without a clean stop.
#[derive(Debug, PartialEq, Eq)]
pub enum Failure {
    /// The settings are wrong (exit 2).
    Usage(String),
    /// A refused directory, an open error, an unclean stop or a fenced server (exit 1).
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
    run_with(cfg, dir, claim, batching)
}

/// Everything after the settings are read.
fn run_with(
    cfg: ServeConfig,
    dir: &Path,
    claim: bool,
    batching: Option<Batching>,
) -> Result<(), Failure> {
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
            os_signals(),
        )
        .map_err(Failure::Failed)?;
    let (line, result) = stopped(&report);
    say(&line);
    result
}

/// The stop line, and the exit: 0 only when the stop was clean and no takeover fenced the
/// server, else 1.
fn stopped(report: &ShutdownReport) -> (String, Result<(), Failure>) {
    let line = format!(
        "serve stopped clean={} fenced={} in_flight_failed={} streams_closed={}",
        report.clean, report.fenced, report.in_flight_failed, report.streams_closed
    );
    let why = match (report.fenced, report.clean) {
        (false, true) => return (line, Ok(())),
        (true, _) => "this server was fenced by a takeover: its writes after it were refused",
        (false, false) => {
            "shutdown was not clean: a request or the final upload did not finish in time"
        }
    };
    (line, Err(Failure::Failed(why.to_string())))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A config from no environment at all, so the tests do not depend on the shell.
    fn cfg(listen: &str) -> ServeConfig {
        ServeConfig::from_lookup(Some(listen), |_| None).unwrap()
    }

    #[test]
    fn serve_refuses_non_loopback_without_tls_and_token() {
        let t = tempfile::tempdir().unwrap();
        let e = run_with(cfg("0.0.0.0:0"), t.path(), false, None).unwrap_err();
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
            let e = run_with(cfg("127.0.0.1:0"), &dir, false, None).unwrap_err();
            assert_eq!(e.exit_code(), 1, "{e:?}");
            let sql_e = crate::sql(&dir, &["SELECT 1".to_string()], false, None).unwrap_err();
            assert_eq!(e.message(), sql_e, "same message as sql");
        }
        assert!(!t.path().join("missing").exists(), "created the directory");
    }

    fn report(clean: bool, fenced: bool) -> ShutdownReport {
        ShutdownReport {
            clean,
            fenced,
            in_flight_failed: 0,
            streams_closed: 2,
        }
    }

    #[test]
    fn stop_line_names_the_fence_and_only_a_clean_unfenced_stop_exits_0() {
        let (line, r) = stopped(&report(true, false));
        assert_eq!(
            line,
            "serve stopped clean=true fenced=false in_flight_failed=0 streams_closed=2"
        );
        assert_eq!(r, Ok(()));
        for (clean, fenced) in [(true, true), (false, true), (false, false)] {
            let (line, r) = stopped(&report(clean, fenced));
            assert!(
                line.starts_with(&format!("serve stopped clean={clean} fenced={fenced} ")),
                "{line}"
            );
            let e = r.unwrap_err();
            assert_eq!(e.exit_code(), 1, "clean={clean} fenced={fenced}: {e:?}");
            assert_eq!(e.message().contains("fenced"), fenced, "{}", e.message());
        }
    }
}
