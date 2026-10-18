//! `turso-durable`: restore, verify, query and exercise a durable database.
mod args;
mod bench;
mod scan;
mod serve;
mod soak;

use args::Command;
use std::collections::BTreeSet;
use std::io::Write;
use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;
use turso_durable::store::fault::FaultStore;
use turso_durable::store::s3::{S3Config, S3Store};
use turso_durable::store::ObjectStore;
use turso_durable::telemetry::{self, TelemetryConfig, TelemetryGuard};
use turso_durable::{Batching, Config, Database, UploadStats, Value};

type CliResult<T> = Result<T, String>;

pub fn err<E: std::fmt::Display>(e: E) -> String {
    e.to_string()
}

/// Print one line to stdout under the stdout lock and flush it, so lines never interleave.
pub fn say(line: &str) {
    let mut out = std::io::stdout().lock();
    let _ = writeln!(out, "{line}");
    let _ = out.flush();
}

/// Print `lines`, each after `prefix`, under one stdout lock and flush them together, so
/// no other line lands between them.
pub fn say_all(prefix: &str, lines: &[String]) {
    let mut out = std::io::stdout().lock();
    for line in lines {
        let _ = writeln!(out, "{prefix}{line}");
    }
    let _ = out.flush();
}

/// Milliseconds with 3 decimals.
pub fn ms(d: Duration) -> String {
    format!("{:.3}", d.as_secs_f64() * 1000.0)
}

/// The upload stats as `key=value` lines, in a fixed order.
pub fn stats_lines(s: &UploadStats) -> Vec<String> {
    vec![
        format!("commits_acked={}", s.commits_acked),
        format!("commits_uploaded={}", s.commits_uploaded),
        format!("log_puts={}", s.log_puts),
        format!("log_put_bytes={}", s.log_put_bytes),
        format!("commits_per_log_put={:.3}", s.commits_per_log_put),
        format!("bytes_per_log_put={:.1}", s.bytes_per_log_put),
        format!("ack_wait_p50_ms={}", ms(s.ack_wait_p50)),
        format!("ack_wait_p95_ms={}", ms(s.ack_wait_p95)),
        format!("ack_wait_p99_ms={}", ms(s.ack_wait_p99)),
    ]
}

pub fn config(dir: &Path) -> CliResult<Config> {
    Config::from_env(dir).map_err(err)
}

/// The S3 store `config` names, optionally delaying page PUTs.
pub fn s3_store(config: &Config) -> CliResult<Arc<dyn ObjectStore>> {
    let s3 = S3Store::new(S3Config {
        endpoint: config.endpoint.clone(),
        region: config.region.clone(),
        bucket: config.bucket.clone(),
        access_key: config.access_key.clone(),
        secret_key: config.secret_key.clone(),
        path_style: config.path_style,
        request_timeout: config.request_timeout,
        ca_bundle: config.ca_bundle.clone(),
    })
    .map_err(err)?;
    Ok(Arc::new(s3))
}

/// The S3 store `config` names, optionally delaying page PUTs.
pub fn store(config: &Config, page_put_delay_ms: Option<u64>) -> CliResult<Arc<dyn ObjectStore>> {
    let s3 = s3_store(config)?;
    let Some(ms) = page_put_delay_ms else {
        return Ok(s3);
    };
    let fault = FaultStore::new(s3);
    fault.only_keys_containing("/pages/");
    fault.delay_puts(std::time::Duration::from_millis(ms));
    Ok(Arc::new(fault))
}

pub fn render(v: &Value) -> String {
    if matches!(v, Value::Null) {
        "NULL".into()
    } else if let Some(b) = v.to_blob() {
        let hex: String = b.iter().map(|x| format!("{x:02x}")).collect();
        format!("x'{hex}'")
    } else if let Some(t) = v.to_text() {
        t.to_string()
    } else if let Some(i) = v.as_int() {
        i.to_string()
    } else {
        v.as_float().to_string()
    }
}

fn restore(dir: &Path) -> CliResult<()> {
    let r = turso_durable::restore(&config(dir)?, dir).map_err(err)?;
    say(&format!(
        "restored generation={} log_objects={} gets={} lists={} bytes={} ms={}",
        r.generation,
        r.log_objects,
        r.gets,
        r.lists,
        r.bytes,
        r.elapsed.as_millis()
    ));
    Ok(())
}

/// Restore into the empty directory `dir` and take the database over there; a writer still
/// running elsewhere is fenced for good. `sql --dir` then continues it.
fn takeover(dir: &Path) -> CliResult<()> {
    let r = turso_durable::takeover(config(dir)?).map_err(err)?;
    say(&format!(
        "took over generation={} epoch={} fence_seq={} writer_id={} quiet_ms={} ms={}",
        r.generation,
        r.epoch,
        r.fence_seq,
        r.writer_id,
        r.quiet.as_millis(),
        r.elapsed.as_millis()
    ));
    Ok(())
}

/// GC: delete what an old enough head no longer reaches. It needs no directory.
fn gc() -> CliResult<()> {
    let r = turso_durable::gc(&config(Path::new("."))?).map_err(err)?;
    let opt = |v: Option<u128>| v.map_or("-".to_string(), |v| v.to_string());
    say(&format!(
        "gc generation={} trimmed_against={} age_ms={} deleted={} outcome={} ms={}",
        r.generation,
        opt(r.trimmed_against.map(u128::from)),
        opt(r.age.map(|a| a.as_millis())),
        r.deleted,
        r.outcome.as_str(),
        r.elapsed.as_millis()
    ));
    Ok(())
}

pub fn is_missing_table(msg: &str) -> bool {
    msg.to_ascii_lowercase().contains("no such table")
}

/// Compare the acked ids with the ids in the database; returns the `verify ok` line.
pub fn check_ids(acked: &[i64], ids: &BTreeSet<i64>) -> CliResult<String> {
    let acked_set: BTreeSet<i64> = acked.iter().copied().collect();
    let duplicates = acked.len() - acked_set.len();
    if duplicates > 0 {
        return Err(format!(
            "duplicate_acks={duplicates}: an id was acked twice"
        ));
    }
    let missing: Vec<_> = acked_set.difference(ids).take(10).collect();
    if !missing.is_empty() {
        return Err(format!(
            "acked ids missing after restore: {missing:?} (first 10)"
        ));
    }
    Ok(format!(
        "verify ok acked={} present={} unacked_present={} duplicate_acks=0",
        acked_set.len(),
        acked_set.len(),
        ids.len() - acked_set.len()
    ))
}

fn verify(dir: &Path, acked: Option<&Path>) -> CliResult<()> {
    let acked = match acked {
        Some(p) => args::parse_acked(
            &std::fs::read_to_string(p).map_err(|e| format!("{}: {e}", p.display()))?,
        ),
        None => Vec::new(),
    };
    let db = turso_durable::open_read_only(config(dir)?).map_err(err)?;
    let conn = db.connect().map_err(err)?;
    let rows = conn.query("PRAGMA integrity_check", &[]).map_err(err)?;
    let verdict = rows
        .first()
        .and_then(|r| r.first())
        .map(render)
        .unwrap_or_default();
    if verdict != "ok" {
        return Err(format!("integrity_check: {verdict}"));
    }
    let ids = match conn.query("SELECT id FROM soak", &[]) {
        Ok(rows) => rows
            .iter()
            .filter_map(|r| r.first().and_then(|v| v.as_int()))
            .collect(),
        // soak was killed before its CREATE TABLE became durable: no rows at all.
        Err(e) if is_missing_table(&e.to_string()) => BTreeSet::new(),
        Err(e) => return Err(err(e)),
    };
    say(&check_ids(&acked, &ids)?);
    conn.close().map_err(err)?;
    db.close().map_err(err)
}

/// Refuse a directory the writer does not own (see [`open_writer`]); no bucket call.
pub(crate) fn claim_check(dir: &Path) -> CliResult<()> {
    if turso_durable::is_claimed(dir).map_err(err)? {
        return Ok(());
    }
    Err(format!(
        "{} holds no database this writer owns: opening it would restore or create the \
         database and take it over from any writer still running, which is then fenced for \
         good; use `verify --dir` to read it, or pass --claim to take it over",
        dir.display()
    ))
}

/// Open `dir` as the writer, with `batching` set on it when given. Without `claim`, only a
/// directory the writer already owns: opening any other (empty, or restored and never
/// opened) restores, claims or creates the database, and fences a writer still running
/// elsewhere.
pub(crate) fn open_writer(
    dir: &Path,
    claim: bool,
    batching: Option<Batching>,
) -> CliResult<Database> {
    if !claim {
        claim_check(dir)?;
    }
    let db = turso_durable::open(config(dir)?).map_err(err)?;
    if let Some(b) = batching {
        db.set_batching(b).map_err(err)?;
    }
    Ok(db)
}

/// Run `statements` with `dir` opened as the writer ([`open_writer`]), printing rows.
pub(crate) fn sql(
    dir: &Path,
    statements: &[String],
    claim: bool,
    batching: Option<Batching>,
) -> CliResult<()> {
    let db = open_writer(dir, claim, batching)?;
    let conn = db.connect().map_err(err)?;
    for s in statements {
        for row in conn.query(s, &[]).map_err(err)? {
            say(&row.iter().map(render).collect::<Vec<_>>().join("\t"));
        }
    }
    conn.close().map_err(err)?;
    db.close().map_err(err)
}

/// Run each of `statements` as its own commit with `dir` opened as the writer
/// ([`open_writer`]), then print the upload stats.
fn stats(
    dir: &Path,
    statements: &[String],
    claim: bool,
    batching: Option<Batching>,
) -> CliResult<()> {
    let db = open_writer(dir, claim, batching)?;
    let conn = db.connect().map_err(err)?;
    for s in statements {
        conn.execute(s, &[]).map_err(err)?;
    }
    say_all("", &stats_lines(&db.upload_stats()));
    conn.close().map_err(err)?;
    db.close().map_err(err)
}

/// Export spans and logs as `DURABLE_OTEL*` say; on any failure, warn and run without.
fn telemetry_init() -> Option<TelemetryGuard> {
    match TelemetryConfig::from_env().and_then(telemetry::init) {
        Ok(g) => Some(g),
        Err(e) => {
            eprintln!("warning: telemetry off: {e}");
            None
        }
    }
}

fn main() -> ExitCode {
    // Kept until main returns: dropping it flushes the export.
    let _telemetry = telemetry_init();
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let cmd = match args::parse(&argv) {
        Ok(c) => c,
        Err(m) => {
            eprintln!("error: {m}\n{}", args::USAGE);
            return ExitCode::from(2);
        }
    };
    if let Command::Serve {
        dir,
        listen,
        claim,
        batching,
    } = &cmd
    {
        return match serve::run(dir, listen.as_deref(), *claim, *batching) {
            Ok(()) => ExitCode::SUCCESS,
            Err(f) => {
                eprintln!("error: {}", f.message());
                ExitCode::from(f.exit_code())
            }
        };
    }
    let result = match &cmd {
        Command::Restore { dir } => restore(dir),
        Command::Takeover { dir } => takeover(dir),
        Command::Gc => gc(),
        Command::Verify { dir, acked } => verify(dir, acked.as_deref()),
        Command::Sql {
            dir,
            statements,
            claim,
            batching,
        } => sql(dir, statements, *claim, *batching),
        Command::Stats {
            dir,
            sql,
            claim,
            batching,
        } => stats(dir, sql, *claim, *batching),
        Command::Soak(a) => soak::run(a),
        Command::Bench(a) => bench::run(a),
        Command::Scan(a) => scan::run(a),
        Command::Serve { .. } => unreachable!("handled above"),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(m) => {
            eprintln!("error: {m}");
            ExitCode::from(1)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn set(v: &[i64]) -> BTreeSet<i64> {
        v.iter().copied().collect()
    }

    #[test]
    fn check_ids_cases() {
        assert_eq!(
            check_ids(&[1, 2], &set(&[1, 2, 3])).unwrap(),
            "verify ok acked=2 present=2 unacked_present=1 duplicate_acks=0"
        );
        // A missing table means no ids: fine when nothing was acked...
        assert!(check_ids(&[], &set(&[])).is_ok());
        // ...and a failure naming the ids when something was.
        let e = check_ids(&[4, 5], &set(&[])).unwrap_err();
        assert!(e.contains("[4, 5]"), "{e}");
        let e = check_ids(&[1, 1], &set(&[1])).unwrap_err();
        assert!(e.contains("duplicate_acks=1"), "{e}");
    }

    /// Without --claim, `sql` refuses a directory a writer open would restore, claim or
    /// create, before it reads any setting or touches the bucket.
    #[test]
    fn sql_refuses_an_unclaimed_directory_without_claim() {
        let t = tempfile::tempdir().unwrap();
        for dir in [t.path().to_path_buf(), t.path().join("missing")] {
            let e = sql(&dir, &["SELECT 1".to_string()], false, None).unwrap_err();
            let e2 = stats(&dir, &["SELECT 1".to_string()], false, None).unwrap_err();
            assert_eq!(e, e2, "stats follows the claim rules of sql");
            assert!(
                e.contains("--claim") && e.contains("take") && e.contains("verify"),
                "{e}"
            );
            assert!(!e.contains('\n'), "one line: {e}");
        }
        assert!(!t.path().join("missing").exists(), "created the directory");
    }

    #[test]
    fn stats_lines_name_every_key_with_its_decimals() {
        let s = turso_durable::UploadStats {
            commits_acked: 31,
            commits_uploaded: 30,
            log_puts: 12,
            log_put_bytes: 4096,
            commits_per_log_put: 2.5,
            bytes_per_log_put: 341.33333,
            ack_wait_p50: Duration::from_micros(1500),
            ack_wait_p95: Duration::from_millis(20),
            ack_wait_p99: Duration::from_nanos(20_123_456),
        };
        assert_eq!(
            stats_lines(&s),
            [
                "commits_acked=31",
                "commits_uploaded=30",
                "log_puts=12",
                "log_put_bytes=4096",
                "commits_per_log_put=2.500",
                "bytes_per_log_put=341.3",
                "ack_wait_p50_ms=1.500",
                "ack_wait_p95_ms=20.000",
                "ack_wait_p99_ms=20.123",
            ]
        );
    }

    #[test]
    fn missing_table_is_recognised() {
        assert!(is_missing_table("engine error: no such table: soak"));
        assert!(!is_missing_table("engine error: disk I/O"));
    }

    #[test]
    fn render_values() {
        assert_eq!(render(&Value::Null), "NULL");
        assert_eq!(render(&Value::from_i64(-7)), "-7");
        assert_eq!(render(&Value::from_text("hi")), "hi");
        assert_eq!(
            render(&Value::from_slice(&[0xab, 0x01]).unwrap()),
            "x'ab01'"
        );
        assert_eq!(render(&Value::from_f64(1.5)), "1.5");
    }
}
