//! `buckwal`: restore, verify, query and exercise a durable database.
mod args;
mod bench;
mod follow;
mod notify_setup;
mod packer;
mod scan;
mod serve;
mod soak;

use args::Command;
use buckwal::store::fault::FaultStore;
use buckwal::store::s3::{S3Config, S3Store};
use buckwal::store::ObjectStore;
use buckwal::telemetry::{self, TelemetryConfig, TelemetryGuard};
use buckwal::{Batching, Config, Database, JournalMode, UploadStats, Value};
use std::collections::BTreeSet;
use std::io::Write;
use std::path::Path;
use std::process::ExitCode;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

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
    // Background commits do not wait for their ack, so the ack-wait quantiles are empty (a
    // real wait is never zero) when only they were acked.
    let wait = |d: Duration| {
        if s.ack_wait_p99.is_zero() {
            "none".to_string()
        } else {
            ms(d)
        }
    };
    vec![
        format!("commits_acked={}", s.commits_acked),
        format!("commits_uploaded={}", s.commits_uploaded),
        format!("log_puts={}", s.log_puts),
        format!("log_put_bytes={}", s.log_put_bytes),
        format!("commits_per_log_put={:.3}", s.commits_per_log_put),
        format!("bytes_per_log_put={:.1}", s.bytes_per_log_put),
        format!("ack_wait_p50_ms={}", wait(s.ack_wait_p50)),
        format!("ack_wait_p95_ms={}", wait(s.ack_wait_p95)),
        format!("ack_wait_p99_ms={}", wait(s.ack_wait_p99)),
        format!("commits_behind={}", s.commits_behind),
        format!("oldest_behind_ms={}", ms(s.oldest_behind)),
    ]
}

/// `--journal-mode`, set once by `main` before any database opens; it wins over
/// `BUCKWAL_JOURNAL_MODE`.
static JOURNAL_MODE: OnceLock<JournalMode> = OnceLock::new();

pub fn config(dir: &Path) -> CliResult<Config> {
    let mut cfg = Config::from_env(dir).map_err(err)?;
    if let Some(m) = JOURNAL_MODE.get() {
        cfg.journal_mode = *m;
    }
    Ok(cfg)
}

/// The journal mode a head's JSON records: a v2 head names `"journal_mode":"mvcc"`, a v1
/// head is WAL. The library's `Head::encode` pins this exact compact form (golden test in
/// crates/buckwal/src/format/tests.rs); its decoder is crate-internal.
fn head_journal_mode(head: &[u8]) -> JournalMode {
    let text = String::from_utf8_lossy(head);
    if text.contains("\"journal_mode\":\"mvcc\"") {
        JournalMode::Mvcc
    } else {
        JournalMode::Wal
    }
}

/// Set `cfg.journal_mode` from the bucket's head. Without a head nothing changes (a new
/// database: the configured mode, wal by default). Returns the head's bytes, when there are
/// any.
fn follow_head(cfg: &mut Config) -> CliResult<Option<Vec<u8>>> {
    let store = s3_store(cfg)?;
    let key = format!("{}/head", cfg.prefix);
    let policy = buckwal::store::RetryPolicy::default();
    let head = buckwal::store::with_retry(&policy, &format!("get {key}"), |_| store.get(&key))
        .map_err(err)?;
    Ok(head.map(|head| {
        cfg.journal_mode = head_journal_mode(&head.body);
        head.body
    }))
}

/// The config for a command that only reads the bucket (`restore`, `verify`): the bucket's
/// head decides the journal mode, whatever `BUCKWAL_JOURNAL_MODE` says.
fn reader_config(dir: &Path) -> CliResult<Config> {
    reader_config_with_head(dir).map(|(cfg, _)| cfg)
}

/// [`reader_config`], and the head's bytes.
fn reader_config_with_head(dir: &Path) -> CliResult<(Config, Option<Vec<u8>>)> {
    let mut cfg = config(dir)?;
    let head = follow_head(&mut cfg)?;
    Ok((cfg, head))
}

/// `head format=<n>` and, for a format 4 or 5 head, ` database_id=<id>`: the head's JSON read as
/// text (a head is plaintext in every format; the library's decoder is crate-internal). Format
/// 3 is an encrypted bucket made before format 4: it has no database id, and an object copied
/// in from another database of the same key file verifies (README, Encryption).
fn head_summary(head: &[u8]) -> String {
    let text = String::from_utf8_lossy(head);
    let field = |name: &str, quoted: bool| -> Option<String> {
        let at = text.find(&format!("\"{name}\":"))? + name.len() + 3;
        let rest = &text[at..];
        let rest = if quoted {
            rest.strip_prefix('"')?
        } else {
            rest
        };
        let end = rest.find(|c: char| {
            if quoted {
                c == '"'
            } else {
                !c.is_ascii_digit()
            }
        })?;
        Some(rest[..end].to_string())
    };
    let format = field("format", false).unwrap_or_else(|| "?".into());
    let mut out = format!("head format={format}");
    if let Some(id) = field("database_id", true) {
        out.push_str(&format!(" database_id={id}"));
    }
    out
}

/// Whether the environment (through `is_set`) names a journal mode.
fn journal_mode_in_env(is_set: impl Fn(&str) -> bool) -> bool {
    is_set("BUCKWAL_JOURNAL_MODE")
}

/// The config for a command that opens the database as the writer (`sql`, `stats`, `serve`,
/// `takeover`, `soak`, `bench`): an explicit `--journal-mode` or `BUCKWAL_JOURNAL_MODE` is
/// kept (and the library refuses a mismatch with the bucket); with neither, an existing
/// bucket's head decides and a missing one means a new WAL database.
pub fn writer_config(dir: &Path) -> CliResult<Config> {
    let mut cfg = config(dir)?;
    let explicit =
        JOURNAL_MODE.get().is_some() || journal_mode_in_env(|k| std::env::var_os(k).is_some());
    if !explicit {
        follow_head(&mut cfg)?;
    }
    Ok(cfg)
}

/// The S3 store `config` names.
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

/// The S3 store `config` names, optionally delaying each PUT of the head by
/// `head_put_delay_ms` (soak `--head-put-delay-ms`).
pub fn store(config: &Config, head_put_delay_ms: Option<u64>) -> CliResult<Arc<dyn ObjectStore>> {
    let s3 = s3_store(config)?;
    let Some(ms) = head_put_delay_ms else {
        return Ok(s3);
    };
    let fault = FaultStore::new(s3);
    fault.only_keys_containing(&head_key(&config.prefix));
    fault.delay_puts(std::time::Duration::from_millis(ms));
    Ok(Arc::new(fault))
}

/// The key of the head under `prefix`.
fn head_key(prefix: &str) -> String {
    match prefix.trim_matches('/') {
        "" => "head".into(),
        p => format!("{p}/head"),
    }
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
    let r = buckwal::restore(&reader_config(dir)?, dir).map_err(err)?;
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
    let r = buckwal::takeover(writer_config(dir)?).map_err(err)?;
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

/// Convert the bucket's database of format 1 to 4 to format 5 in place; `dir` gets the restored
/// copy, which `sql --dir` then continues as the writer.
fn upgrade(dir: &Path) -> CliResult<()> {
    let r = buckwal::upgrade(&reader_config(dir)?, dir).map_err(err)?;
    say(&format!(
        "upgraded from_format={} from_generation={} generation={} epoch={} fence_seq={} \
         database_id={} pack_key={} bytes={} replaced_pointer={} ms={}",
        r.from_format,
        r.from_generation,
        r.generation,
        r.epoch,
        r.fence_seq,
        r.database_id,
        r.pack_key,
        r.bytes,
        r.replaced_pointer,
        r.elapsed.as_millis()
    ));
    Ok(())
}

/// Vacuum the writer's own database in `dir` in place (`buckwal::vacuum`), and print what it did.
fn vacuum(dir: &Path) -> CliResult<()> {
    let r = buckwal::vacuum(&writer_config(dir)?).map_err(err)?;
    say(&vacuum_line(&r));
    Ok(())
}

/// The line `vacuum` prints for its report.
fn vacuum_line(r: &buckwal::VacuumReport) -> String {
    format!(
        "vacuumed page_size={} pages_before={} free_pages_before={} free_page_share={:.3} \
         pages_after={} free_pages_after={} bytes_before={} bytes_after={} ms={}",
        r.page_size,
        r.pages_before,
        r.free_pages_before,
        r.free_page_share_before(),
        r.pages_after,
        r.free_pages_after,
        r.file_bytes_before,
        r.file_bytes_after,
        r.elapsed.as_millis()
    )
}

/// GC: delete what the database no longer needs (old enough heads, packs, log and uploads). It
/// needs no directory.
fn gc() -> CliResult<()> {
    let r = buckwal::gc(&config(Path::new("."))?).map_err(err)?;
    let opt = |v: Option<u128>| v.map_or("-".to_string(), |v| v.to_string());
    say(&format!(
        "gc generation={} trimmed_against={} age_ms={} deleted={} packs_deleted={} \
         orphan_packs_deleted={} uploads_aborted={} outcome={} ms={}",
        r.generation,
        opt(r.trimmed_against.map(u128::from)),
        opt(r.age.map(|a| a.as_millis())),
        r.deleted,
        r.packs_deleted,
        r.orphan_packs_deleted,
        r.uploads_aborted,
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
    let (cfg, head) = reader_config_with_head(dir)?;
    let store = s3_store(&cfg)?;
    say_all("", &verify_with(cfg, store, head, &acked)?);
    Ok(())
}

/// `verify` against `store`: its lines.
fn verify_with(
    cfg: Config,
    store: Arc<dyn ObjectStore>,
    head: Option<Vec<u8>>,
    acked: &[i64],
) -> CliResult<Vec<String>> {
    let mut lines = Vec::new();
    // The open comes first, so a restore into an empty `dir` starts at once: it must end
    // before the writer switches the generation.
    let db = buckwal::open_read_only_with_store(cfg.clone(), store.clone()).map_err(err)?;
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
    lines.push(check_ids(acked, &ids)?);
    if let Some(head) = head {
        lines.push(head_summary(&head));
    }
    conn.close().map_err(err)?;
    db.close().map_err(err)?;
    // Then the bucket: a copy that was restored before shows nothing of a changed pack block.
    let checked = buckwal::check_bucket_with_store(&cfg, store)
        .map_err(|e| format!("verify: bucket: {e}"))?;
    if let Some(key) = &checked.pack_key {
        lines.push(format!(
            "verify pack key={key} pack_generation={} size={} blocks={} log_generations={} \
             log_objects={}",
            checked.pack_generation,
            checked.pack_size,
            checked.blocks,
            checked.log_generations,
            checked.log_objects
        ));
    }
    Ok(lines)
}

/// Make the pack directory of `pack` and `packer`: `dir`, when `BUCKWAL_PACK_DIR` names no
/// other (the library makes that one).
pub(crate) fn make_pack_dir(cfg: &Config, dir: &Path) -> CliResult<()> {
    if cfg.pack_dir.is_none() {
        std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    }
    Ok(())
}

/// `pack`: one pack run now, with `dir` as the pack directory. A `failed` run is an error
/// (exit 1); `packed`, `nothing_to_do` and `superseded` are not.
fn pack(dir: &Path) -> CliResult<()> {
    let cfg = config(dir)?;
    make_pack_dir(&cfg, dir)?;
    let r = buckwal::pack(&cfg).map_err(err)?;
    say(&pack_line(&r));
    pack_exit(&r.outcome).map_err(|e| match &r.error {
        Some(why) => format!("{e}: {why}"),
        None => e,
    })
}

/// The exit of `pack`: an error for a `failed` run, which names its step and key, else none.
fn pack_exit(outcome: &buckwal::PackOutcome) -> CliResult<()> {
    match outcome {
        buckwal::PackOutcome::Failed { step, key } => Err(match key {
            Some(k) => format!("the pack run failed at {step} ({k})"),
            None => format!("the pack run failed at {step}"),
        }),
        _ => Ok(()),
    }
}

/// The line `pack` prints for its report: each value one word (`-` for none, and spaces in a
/// step become `_`). The error is not in it: `pack` gives it on stderr.
fn pack_line(r: &buckwal::PackReport) -> String {
    let (step, key) = match &r.outcome {
        buckwal::PackOutcome::Failed { step, key } => (
            step.replace(' ', "_"),
            key.clone().unwrap_or_else(|| "-".into()),
        ),
        _ => ("-".into(), "-".into()),
    };
    let (source, reason) = r.source.map_or(("-", "-"), |s| (s.as_str(), s.reason()));
    format!(
        "pack outcome={} pack_generation={} pack_key={} size={} bytes_down={} bytes_up={} \
         source={source} reason={reason} free_page_share={:.3} failed_step={step} \
         failed_key={key} ms={}",
        r.outcome.as_str(),
        r.pack_generation,
        r.pack_key.as_deref().unwrap_or("-"),
        r.pack_size,
        r.bytes_down,
        r.bytes_up,
        r.free_page_share,
        r.elapsed.as_millis()
    )
}

/// Refuse a directory the writer does not own (see [`open_writer`]); no bucket call.
pub(crate) fn claim_check(dir: &Path) -> CliResult<()> {
    if buckwal::is_claimed(dir).map_err(err)? {
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
    open_writer_with(dir, claim, batching, |_| {})
}

/// [`open_writer`], with `adjust` applied to the writer's config before the open (`serve
/// --pack`).
pub(crate) fn open_writer_with(
    dir: &Path,
    claim: bool,
    batching: Option<Batching>,
    adjust: impl FnOnce(&mut Config),
) -> CliResult<Database> {
    if !claim {
        claim_check(dir)?;
    }
    let mut cfg = writer_config(dir)?;
    adjust(&mut cfg);
    let db = buckwal::open(cfg).map_err(err)?;
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
    say(&format!("durable_commit={}", db.durable_commit()));
    conn.close().map_err(err)?;
    db.close().map_err(err)
}

/// Export spans and logs as `BUCKWAL_OTEL*` say; on any failure, warn and run without.
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
    let cmd = match args::parse_with_mode(&argv) {
        Ok((c, mode)) => {
            if let Some(m) = mode {
                let _ = JOURNAL_MODE.set(m);
            }
            c
        }
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
        pack,
    } = &cmd
    {
        return match serve::run(dir, listen.as_deref(), *claim, *batching, *pack) {
            Ok(()) => ExitCode::SUCCESS,
            Err(f) => {
                eprintln!("error: {}", f.message());
                ExitCode::from(f.exit_code())
            }
        };
    }
    let handled = match &cmd {
        Command::Follow { dir, poll_ms } => Some(follow::run(dir, *poll_ms)),
        Command::Packer { dir } => Some(packer::run(dir)),
        Command::NotifySetup {
            bucket,
            replicas,
            region,
            topic,
        } => {
            // A settings error: usage, exit 2.
            let r = notify_setup::run(bucket, *replicas, region.as_deref(), topic.as_deref());
            Some(r.map_err(serve::Failure::Usage))
        }
        _ => None,
    };
    if let Some(result) = handled {
        return match result {
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
        Command::Upgrade { dir } => upgrade(dir),
        Command::Vacuum { dir } => vacuum(dir),
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
        Command::Pack { dir } => pack(dir),
        Command::Serve { .. }
        | Command::Follow { .. }
        | Command::Packer { .. }
        | Command::NotifySetup { .. } => {
            unreachable!("handled above")
        }
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

    /// `soak --head-put-delay-ms` delays only the head object: its key under the prefix, with
    /// the slashes `Config::keys` drops.
    #[test]
    fn head_key_is_the_head_under_the_prefix() {
        assert_eq!(head_key(""), "head");
        assert_eq!(head_key("sys/t-1"), "sys/t-1/head");
        assert_eq!(head_key("/sys/t-1/"), "sys/t-1/head");
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
        let s = buckwal::UploadStats {
            commits_acked: 31,
            commits_uploaded: 30,
            log_puts: 12,
            log_put_bytes: 4096,
            commits_per_log_put: 2.5,
            bytes_per_log_put: 341.33333,
            ack_wait_p50: Duration::from_micros(1500),
            ack_wait_p95: Duration::from_millis(20),
            ack_wait_p99: Duration::from_nanos(20_123_456),
            commits_behind: 4,
            oldest_behind: Duration::from_micros(2500),
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
                "commits_behind=4",
                "oldest_behind_ms=2.500",
            ]
        );
        // Only background commits acked: no ack was waited for.
        let s = buckwal::UploadStats {
            commits_acked: 5,
            ..buckwal::UploadStats::default()
        };
        let lines = stats_lines(&s);
        assert!(
            lines.contains(&"ack_wait_p50_ms=none".to_string()),
            "{lines:?}"
        );
        assert!(
            lines.contains(&"ack_wait_p99_ms=none".to_string()),
            "{lines:?}"
        );
    }

    #[test]
    fn writer_config_reads_buckwal_journal_mode() {
        assert!(journal_mode_in_env(|k| k == "BUCKWAL_JOURNAL_MODE"));
        assert!(!journal_mode_in_env(|_| false));
    }

    #[test]
    fn head_summary_names_the_format_and_the_database_id() {
        let v4 = br#"{"format":4,"generation":2,"manifest":"manifest/0000000001","writer_id":"w","epoch":1,"journal_mode":"wal","database_id":"0123456789abcdef0123456789abcdef","key_check":"aa","integrity":"bb"}"#;
        assert_eq!(
            head_summary(v4),
            "head format=4 database_id=0123456789abcdef0123456789abcdef"
        );
        let v3 = br#"{"format":3,"generation":2,"manifest":null,"writer_id":"w","epoch":1,"journal_mode":"wal","key_check":"aa","integrity":"bb"}"#;
        assert_eq!(head_summary(v3), "head format=3");
        let v1 = br#"{"format":1,"generation":1,"manifest":null,"writer_id":"w","epoch":1}"#;
        assert_eq!(head_summary(v1), "head format=1");
        assert_eq!(head_summary(b"not json"), "head format=?");
    }

    #[test]
    fn head_journal_mode_reads_the_head() {
        let v2 = br#"{"format":2,"journal_mode":"mvcc","generation":2}"#;
        let v1 = br#"{"format":1,"generation":2}"#;
        assert_eq!(head_journal_mode(v2), JournalMode::Mvcc);
        assert_eq!(head_journal_mode(v1), JournalMode::Wal);
        assert_eq!(head_journal_mode(b"not json"), JournalMode::Wal);
    }

    /// `pack` exits 0 on `packed`, `nothing_to_do` and `superseded`, and 1 on `failed`, with
    /// the failed step and key in the message.
    #[test]
    fn pack_command_exit_codes() {
        use buckwal::PackOutcome;
        for ok in [
            PackOutcome::Packed,
            PackOutcome::NothingToDo,
            PackOutcome::Superseded,
        ] {
            assert_eq!(pack_exit(&ok), Ok(()), "{ok:?}");
        }
        let e = pack_exit(&PackOutcome::Failed {
            step: "disk".into(),
            key: None,
        })
        .unwrap_err();
        assert!(e.contains("failed") && e.contains("disk"), "{e}");
        let e = pack_exit(&PackOutcome::Failed {
            step: "verify size".into(),
            key: Some("t/pack/0000000003/r1".into()),
        })
        .unwrap_err();
        assert!(
            e.contains("verify size") && e.contains("t/pack/0000000003/r1"),
            "{e}"
        );
    }

    fn report(outcome: buckwal::PackOutcome) -> buckwal::PackReport {
        buckwal::PackReport {
            outcome,
            pack_generation: 3,
            pack_key: Some("t/pack/0000000003/r1".into()),
            bytes_down: 4096,
            bytes_up: 8192,
            step_times: vec![("read", Duration::from_millis(5))],
            free_page_share: 0.25,
            pack_size: 8192,
            error: None,
            source: Some(buckwal::PackSource::Downloaded(
                buckwal::DownloadReason::Moved,
            )),
            elapsed: Duration::from_millis(1234),
        }
    }

    #[test]
    fn pack_line_names_the_report() {
        assert_eq!(
            pack_line(&report(buckwal::PackOutcome::Packed)),
            "pack outcome=packed pack_generation=3 pack_key=t/pack/0000000003/r1 size=8192 \
             bytes_down=4096 bytes_up=8192 source=downloaded reason=moved free_page_share=0.250 \
             failed_step=- failed_key=- ms=1234"
        );
        let mut r = report(buckwal::PackOutcome::Failed {
            step: "verify size".into(),
            key: Some("t/pack/0000000003/r1".into()),
        });
        r.source = Some(buckwal::PackSource::Kept);
        r.pack_key = None;
        r.error = Some("the HEAD size is 1 byte more".into());
        let line = pack_line(&r);
        assert!(
            line.starts_with("pack outcome=failed pack_generation=3 pack_key=- "),
            "{line}"
        );
        assert!(line.contains(" source=kept reason=none "), "{line}");
        assert!(
            line.contains(" failed_step=verify_size failed_key=t/pack/0000000003/r1 "),
            "{line}"
        );
        assert!(
            !line.contains("HEAD size"),
            "the error is not in the line: {line}"
        );
        r.source = None;
        assert!(pack_line(&r).contains(" source=- reason=- "));
    }

    fn mem_config(dir: &Path) -> Config {
        let mut c = Config::new(dir, "http://127.0.0.1:1", "bucket", "ak", "sk");
        c.prefix = "t".to_string();
        c.retry_deadline = Duration::from_secs(2);
        c
    }

    /// A writer commits ids 1 to 3 in `soak`, ends its generation, commits 4, ends it again
    /// and commits 5; a pack run between the two switches packs the first generation.
    fn soak_bucket(mem: &Arc<buckwal::store::MemStore>, root: &Path) {
        use buckwal::CheckpointMode;
        let store: Arc<dyn ObjectStore> = mem.clone();
        let db = buckwal::open_with_store(mem_config(&root.join("w")), store.clone()).unwrap();
        let c = db.connect().unwrap();
        c.execute("CREATE TABLE soak(id INTEGER PRIMARY KEY)", &[])
            .unwrap();
        for id in 1..=3 {
            c.execute("INSERT INTO soak VALUES (?)", &[Value::from_i64(id)])
                .unwrap();
        }
        c.checkpoint(CheckpointMode::Truncate).unwrap();
        let r = buckwal::pack_with_store(&mem_config(&root.join("p")), store.clone()).unwrap();
        assert_eq!(r.outcome, buckwal::PackOutcome::Packed, "{r:?}");
        c.execute("INSERT INTO soak VALUES (4)", &[]).unwrap();
        c.checkpoint(CheckpointMode::Truncate).unwrap();
        c.execute("INSERT INTO soak VALUES (5)", &[]).unwrap();
        c.close().unwrap();
        db.close().unwrap();
    }

    fn head_of(mem: &buckwal::store::MemStore) -> Option<Vec<u8>> {
        mem.get("t/head").unwrap().map(|o| o.body)
    }

    /// `verify` reads the pointer and checks every block of the pack against its codes, and
    /// the chain of generations and the log from the pack on: a flipped byte in the pack, and
    /// a generation's log that is gone, are reported. A correct bucket verifies, and its lines
    /// begin with the `verify ok` line, as before.
    #[test]
    fn verify_checks_pack_blocks() {
        let mem = Arc::new(buckwal::store::MemStore::new());
        let t = tempfile::tempdir().unwrap();
        soak_bucket(&mem, t.path());
        let store: Arc<dyn ObjectStore> = mem.clone();
        let lines = verify_with(
            mem_config(&t.path().join("r1")),
            store.clone(),
            head_of(&mem),
            &[1, 2, 3, 4, 5],
        )
        .unwrap();
        assert_eq!(
            lines[0],
            "verify ok acked=5 present=5 unacked_present=0 duplicate_acks=0"
        );
        assert!(
            lines[1].starts_with("head format=5 database_id="),
            "{lines:?}"
        );
        let pack_key = mem
            .list("t/pack/")
            .unwrap()
            .into_iter()
            .find(|k| !k.ends_with(".codes"))
            .unwrap();
        let rel = pack_key.strip_prefix("t/").unwrap();
        assert!(
            lines[2].starts_with(&format!("verify pack key={rel} pack_generation=2 ")),
            "{lines:?}"
        );
        assert!(lines[2].contains(" blocks=1 "), "{lines:?}");
        // Every log object from the pack generation on, and their generations.
        let log: Vec<String> = mem
            .list("t/log/")
            .unwrap()
            .into_iter()
            .filter(|k| k.as_str() >= "t/log/0000000002/")
            .collect();
        let generations: BTreeSet<&str> = log.iter().map(|k| &k[..17]).collect();
        assert!(generations.len() >= 2, "{log:?}");
        assert!(
            lines[2].ends_with(&format!(
                " log_generations={} log_objects={}",
                generations.len(),
                log.len()
            )),
            "{lines:?} {log:?}"
        );

        // One flipped byte in the pack: the local restore in r1 is not read, the bucket is.
        let good = mem.get(&pack_key).unwrap().unwrap().body;
        let mut bad = good.clone();
        bad[good.len() - 1] ^= 0x01;
        mem.put(&pack_key, &bad).unwrap();
        let e = verify_with(
            mem_config(&t.path().join("r1")),
            store.clone(),
            head_of(&mem),
            &[],
        )
        .unwrap_err();
        assert!(e.starts_with("verify: bucket: "), "{e}");
        assert!(
            e.contains("checksum mismatch") && e.contains(&pack_key),
            "{e}"
        );
        mem.put(&pack_key, &good).unwrap();

        // The log of the first generation after the pack is gone: the chain is broken.
        let log: Vec<String> = mem.list("t/log/0000000002/").unwrap();
        assert!(!log.is_empty());
        for k in &log {
            mem.delete(k).unwrap();
        }
        let e =
            verify_with(mem_config(&t.path().join("r1")), store, head_of(&mem), &[]).unwrap_err();
        assert!(
            e.starts_with("verify: bucket: ") && e.contains("log_incomplete"),
            "{e}"
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
