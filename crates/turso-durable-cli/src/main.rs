//! `turso-durable`: restore, verify, query and exercise a durable database.
mod args;
mod bench;
mod soak;

use args::Command;
use std::io::Write;
use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;
use turso_durable::store::fault::FaultStore;
use turso_durable::store::s3::{S3Config, S3Store};
use turso_durable::store::ObjectStore;
use turso_durable::{Config, Value};

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

pub fn config(dir: &Path) -> CliResult<Config> {
    Config::from_env(dir).map_err(err)
}

/// The S3 store `config` names, optionally delaying page PUTs.
pub fn store(config: &Config, page_put_delay_ms: Option<u64>) -> CliResult<Arc<dyn ObjectStore>> {
    let s3 = S3Store::new(S3Config {
        endpoint: config.endpoint.clone(),
        region: config.region.clone(),
        bucket: config.bucket.clone(),
        access_key: config.access_key.clone(),
        secret_key: config.secret_key.clone(),
        path_style: config.path_style,
        request_timeout: config.request_timeout,
    })
    .map_err(err)?;
    let s3: Arc<dyn ObjectStore> = Arc::new(s3);
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

fn verify(dir: &Path, acked: Option<&Path>) -> CliResult<()> {
    let acked = match acked {
        Some(p) => args::parse_acked(
            &std::fs::read_to_string(p).map_err(|e| format!("{}: {e}", p.display()))?,
        )?,
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
    let ids: std::collections::BTreeSet<i64> = conn
        .query("SELECT id FROM soak", &[])
        .map_err(err)?
        .iter()
        .filter_map(|r| r.first().and_then(|v| v.as_int()))
        .collect();
    let acked_set: std::collections::BTreeSet<i64> = acked.iter().copied().collect();
    let missing: Vec<_> = acked_set.difference(&ids).take(10).collect();
    if !missing.is_empty() {
        return Err(format!(
            "acked ids missing after restore: {missing:?} (first 10)"
        ));
    }
    let present = acked_set.len();
    say(&format!(
        "verify ok acked={} present={} unacked_present={}",
        acked_set.len(),
        present,
        ids.len() - present
    ));
    conn.close().map_err(err)?;
    db.close().map_err(err)
}

fn sql(dir: &Path, statements: &[String]) -> CliResult<()> {
    let db = turso_durable::open(config(dir)?).map_err(err)?;
    let conn = db.connect().map_err(err)?;
    for s in statements {
        for row in conn.query(s, &[]).map_err(err)? {
            say(&row.iter().map(render).collect::<Vec<_>>().join("\t"));
        }
    }
    conn.close().map_err(err)?;
    db.close().map_err(err)
}

fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let cmd = match args::parse(&argv) {
        Ok(c) => c,
        Err(m) => {
            eprintln!("error: {m}\n{}", args::USAGE);
            return ExitCode::from(2);
        }
    };
    let result = match &cmd {
        Command::Restore { dir } => restore(dir),
        Command::Verify { dir, acked } => verify(dir, acked.as_deref()),
        Command::Sql { dir, statements } => sql(dir, statements),
        Command::Soak(a) => soak::run(a),
        Command::Bench { dir, commits } => bench::run(dir, *commits),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(m) => {
            eprintln!("error: {m}");
            ExitCode::from(1)
        }
    }
}
