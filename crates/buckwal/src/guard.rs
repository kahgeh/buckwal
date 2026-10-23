//! The SQL guard (D5, D7): statements the engine must never see, refused before `prepare`.
//! - `PRAGMA journal_mode` set to `mvcc`/`experimental_mvcc` in a WAL database: the engine
//!   writes the MVCC header to page 1 before it opens the MVCC log, so refusing the log
//!   afterwards would leave the header changed (`MVCC: switching an existing WAL database`).
//! - `PRAGMA journal_mode` set to `wal` in an MVCC database (`MVCC: switching back to WAL`).
//! - `PRAGMA buckwal_batching` set to a Window in an MVCC database (`MVCC: Window batching`).
//! - `PRAGMA journal_mode` set to anything else: only the WAL and the MVCC log are shipped.
//! - `PRAGMA synchronous` below FULL: a commit that does not fsync is not uploaded either.
//! - `ATTACH`: writes to an attached database are not shipped.
//! - `PRAGMA hexkey` or `PRAGMA cipher` set to a value: the engine would swap the
//!   connection's key or cipher (translate/pragma.rs:644-655); the key comes from
//!   `BUCKWAL_ENCRYPTION_KEY_FILE` only (D16).
//! - `PRAGMA buckwal_batching` the engine would be handed: [`crate::Connection`] runs it
//!   itself ([`batching_pragma`]), and the engine would take it for an unknown pragma and
//!   silently do nothing. So it is refused among other statements (not even the first of
//!   several reaches the engine; the split path, [`check_sql_split`], catches each in place
//!   instead), on a schema other than `main`, and under `EXPLAIN`. A value it could never
//!   accept is refused here too, so a batch holding one runs none of its statements.
//!
//! Reading a pragma (no value) is fine. A statement that does not parse is left to the
//! engine, which reports it (and never runs it or anything after it).
use crate::batching::Batching;
use crate::config::JournalMode;
use crate::error::{Error, Result};
use turso_parser::ast::{Cmd, Expr, Literal, PragmaBody, QualifiedName, Stmt, UnaryOperator};
use turso_parser::parser::Parser;

/// Refuse a statement that would leave WAL mode, lower `synchronous` or attach a database,
/// and a `PRAGMA buckwal_batching` that is not the only statement in `sql`.
pub(crate) fn check_sql(sql: &str, mode: JournalMode) -> Result<()> {
    check(sql, false, mode)
}

/// [`check_sql`] for a caller that runs `sql` one statement at a time, catching each
/// `PRAGMA buckwal_batching` in place ([`first_batching_pragma`]): it may be one of several.
pub(crate) fn check_sql_split(sql: &str, mode: JournalMode) -> Result<()> {
    check(sql, true, mode)
}

fn check(sql: &str, split: bool, mode: JournalMode) -> Result<()> {
    let lower = sql.to_ascii_lowercase();
    if !lower.contains("pragma") && !lower.contains("attach") {
        return Ok(());
    }
    // Statements parsed; a buckwal_batching PRAGMA among them; text after them that does
    // not parse (the engine reports it, but only after preparing what comes before).
    let (mut cmds, mut batching, mut unparsed) = (0, false, false);
    for cmd in Parser::new(sql.as_bytes()) {
        let Ok(cmd) = cmd else {
            unparsed = true;
            break;
        };
        cmds += 1;
        let explain = !matches!(cmd, Cmd::Stmt(_));
        let stmt = match cmd {
            Cmd::Stmt(s) | Cmd::Explain(s) => s,
            Cmd::ExplainQueryPlan { stmt, .. } => stmt,
        };
        if let Stmt::Pragma { name, body } = &stmt {
            if name.name.as_str().eq_ignore_ascii_case("buckwal_batching") {
                check_batching(name, body.as_ref(), explain, mode)?;
                batching = true;
                continue;
            }
        }
        match stmt {
            Stmt::Attach { .. } => return Err(Error::Unsupported("ATTACH")),
            Stmt::Pragma {
                name,
                body: Some(PragmaBody::Equals(value) | PragmaBody::Call(value)),
            } => check_pragma(name.name.as_str(), &value, mode)?,
            _ => {}
        }
    }
    if batching && !split && (cmds > 1 || unparsed) {
        return Err(Error::Config(
            "PRAGMA buckwal_batching must be its own statement".into(),
        ));
    }
    Ok(())
}

/// A `PRAGMA buckwal_batching` the connection can run: on `main`, not explained, and with a
/// valid value if it has one. (Outside a transaction is only known when it runs.)
fn check_batching(
    name: &QualifiedName,
    body: Option<&PragmaBody>,
    explain: bool,
    mode: JournalMode,
) -> Result<()> {
    if !is_batching(name) {
        return Err(Error::Config(
            "PRAGMA buckwal_batching applies to main only".into(),
        ));
    }
    if explain {
        return Err(Error::Config(
            "EXPLAIN of PRAGMA buckwal_batching is not supported".into(),
        ));
    }
    if let BatchingPragma::Set(b) = setting(body) {
        let b = b?;
        b.validate()?;
        b.check_mode(mode)?;
    }
    Ok(())
}

fn check_pragma(name: &str, value: &Expr, mode: JournalMode) -> Result<()> {
    if name.eq_ignore_ascii_case("hexkey") || name.eq_ignore_ascii_case("cipher") {
        return Err(Error::Unsupported(
            "encryption PRAGMAs (use BUCKWAL_ENCRYPTION_KEY_FILE)",
        ));
    }
    let value = pragma_value(value).map(|v| v.to_ascii_lowercase());
    if name.eq_ignore_ascii_case("journal_mode") {
        return match (value.as_deref(), mode) {
            (Some("wal"), JournalMode::Wal) => Ok(()),
            (Some("wal"), JournalMode::Mvcc) => {
                Err(Error::Unsupported("MVCC: switching back to WAL"))
            }
            (Some("mvcc" | "experimental_mvcc"), JournalMode::Mvcc) => Ok(()),
            (Some("mvcc" | "experimental_mvcc"), JournalMode::Wal) => Err(Error::Unsupported(
                "MVCC: switching an existing WAL database",
            )),
            _ => Err(Error::Unsupported("journal_mode other than WAL")),
        };
    }
    if name.eq_ignore_ascii_case("synchronous") {
        // Anything else the engine reads as FULL (translate/pragma.rs:662-676); the
        // post-prepare sync mode check backs this up.
        if let Some("off" | "normal" | "0" | "1") = value.as_deref() {
            return Err(Error::Unsupported("synchronous below FULL"));
        }
    }
    Ok(())
}

/// `PRAGMA [main.]buckwal_batching`, which [`crate::Connection`] runs itself.
#[derive(Debug, PartialEq)]
pub(crate) enum BatchingPragma {
    /// No value: read the setting.
    Read,
    /// `= value` or `(value)`, parsed with [`Batching`]'s `FromStr`.
    Set(Result<Batching>),
}

/// `sql` mentions `buckwal_batching` anywhere (case-insensitive); every spelling of the
/// PRAGMA does.
pub(crate) fn mentions_batching(sql: &str) -> bool {
    const NAME: &[u8] = b"buckwal_batching";
    sql.as_bytes()
        .windows(NAME.len())
        .any(|w| w.eq_ignore_ascii_case(NAME))
}

/// `sql` is exactly one statement, `PRAGMA [main.]buckwal_batching` (any case).
pub(crate) fn batching_pragma(sql: &str) -> Option<BatchingPragma> {
    if !mentions_batching(sql) {
        return None;
    }
    let mut parser = Parser::new(sql.as_bytes());
    let pragma = classify(parser.next()?.ok()?)?;
    parser.next().is_none().then_some(pragma)
}

/// The first statement of `sql`, when it is `PRAGMA [main.]buckwal_batching`, and the offset
/// in `sql` just past it (and its `;`), where the next statement starts.
pub(crate) fn first_batching_pragma(sql: &str) -> Option<(BatchingPragma, usize)> {
    if !mentions_batching(sql) {
        return None;
    }
    let mut parser = Parser::new(sql.as_bytes());
    let pragma = classify(parser.next()?.ok()?)?;
    Some((pragma, parser.offset()))
}

/// The pragma `name` is `[main.]buckwal_batching`.
fn is_batching(name: &QualifiedName) -> bool {
    name.name.as_str().eq_ignore_ascii_case("buckwal_batching")
        && name
            .db_name
            .as_ref()
            .is_none_or(|db| db.as_str().eq_ignore_ascii_case("main"))
}

fn classify(cmd: Cmd) -> Option<BatchingPragma> {
    let Cmd::Stmt(Stmt::Pragma { name, body }) = cmd else {
        return None;
    };
    is_batching(&name).then(|| setting(body.as_ref()))
}

/// What a `PRAGMA buckwal_batching` with `body` asks for.
fn setting(body: Option<&PragmaBody>) -> BatchingPragma {
    let Some(PragmaBody::Equals(value) | PragmaBody::Call(value)) = body else {
        return BatchingPragma::Read;
    };
    BatchingPragma::Set(match pragma_value(value) {
        Some(text) => text.parse(),
        None => Err(Error::Config(
            "PRAGMA buckwal_batching takes off or 'window <max_wait> <max_commits> <max_bytes>'"
                .into(),
        )),
    })
}

/// A pragma value as text: a bare identifier, a quoted string or a (signed) number.
fn pragma_value(e: &Expr) -> Option<String> {
    match e {
        Expr::Name(n) | Expr::Id(n) => Some(n.as_str().to_string()),
        Expr::Literal(Literal::Keyword(s) | Literal::String(s) | Literal::Numeric(s)) => {
            Some(unquote(s))
        }
        Expr::Unary(UnaryOperator::Negative, inner) => pragma_value(inner).map(|v| format!("-{v}")),
        Expr::Unary(UnaryOperator::Positive, inner) => pragma_value(inner),
        _ => None,
    }
}

/// Strip one level of SQL quoting: '…', "…", `…` or […].
fn unquote(s: &str) -> String {
    let b = s.as_bytes();
    if b.len() >= 2 {
        let (first, last) = (b[0], b[b.len() - 1]);
        let inner = &s[1..s.len() - 1];
        match (first, last) {
            (b'\'', b'\'') => return inner.replace("''", "'"),
            (b'"', b'"') => return inner.replace("\"\"", "\""),
            (b'`', b'`') => return inner.replace("``", "`"),
            (b'[', b']') => return inner.to_string(),
            _ => {}
        }
    }
    s.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::JournalMode;
    use crate::error::Error;

    // The WAL-mode checks below keep their call shape.
    fn check_sql(sql: &str) -> Result<()> {
        super::check_sql(sql, JournalMode::Wal)
    }

    fn check_sql_split(sql: &str) -> Result<()> {
        super::check_sql_split(sql, JournalMode::Wal)
    }

    fn refused(sql: &str) -> &'static str {
        match check_sql(sql) {
            Err(Error::Unsupported(what)) => what,
            r => panic!("{sql}: {r:?}"),
        }
    }

    fn refused_in_mvcc(sql: &str) -> &'static str {
        match super::check_sql(sql, JournalMode::Mvcc) {
            Err(Error::Unsupported(what)) => what,
            r => panic!("{sql}: {r:?}"),
        }
    }

    const SWITCH_TO_MVCC: &str = "MVCC: switching an existing WAL database";
    const SWITCH_BACK: &str = "MVCC: switching back to WAL";

    #[test]
    fn guard_refuses_journal_mode_wal_in_mvcc() {
        for sql in [
            "PRAGMA journal_mode=wal",
            "PRAGMA journal_mode='WAL'",
            "pragma JOURNAL_MODE = wal",
            "PRAGMA main.journal_mode = 'wal'",
            "PRAGMA journal_mode(\"wal\")",
            "SELECT 1; PRAGMA journal_mode=wal;",
        ] {
            assert_eq!(refused_in_mvcc(sql), SWITCH_BACK, "{sql}");
            let r = super::check_sql_split(sql, JournalMode::Mvcc);
            assert_eq!(r, Err(Error::Unsupported(SWITCH_BACK)), "split: {sql}");
        }
        // Reading the mode, setting it to what it is, and other modes' own refusal.
        for sql in [
            "PRAGMA journal_mode",
            "PRAGMA journal_mode=mvcc",
            "PRAGMA journal_mode='experimental_mvcc'",
            "SELECT 'pragma journal_mode=wal'",
        ] {
            assert_eq!(super::check_sql(sql, JournalMode::Mvcc), Ok(()), "{sql}");
        }
        assert_eq!(
            refused_in_mvcc("PRAGMA journal_mode=delete"),
            "journal_mode other than WAL"
        );
    }

    #[test]
    fn guard_still_refuses_mvcc_pragma_in_wal_mode() {
        for sql in [
            "PRAGMA journal_mode='mvcc'",
            "pragma JOURNAL_MODE = mvcc",
            "PRAGMA journal_mode = 'EXPERIMENTAL_MVCC'",
            "PRAGMA main.journal_mode = 'mvcc'",
            "PRAGMA journal_mode(\"mvcc\")",
            "SELECT 1; PRAGMA journal_mode=mvcc;",
        ] {
            assert_eq!(refused(sql), SWITCH_TO_MVCC, "{sql}");
        }
    }

    #[test]
    fn buckwal_batching_window_is_refused_in_mvcc() {
        for sql in [
            "PRAGMA buckwal_batching = 'window 50ms 64 1MiB'",
            "PRAGMA main.buckwal_batching('window 1s 2 3')",
        ] {
            assert_eq!(refused_in_mvcc(sql), "MVCC: Window batching", "{sql}");
            assert_eq!(super::check_sql(sql, JournalMode::Wal), Ok(()), "{sql}");
            let r = super::check_sql_split(sql, JournalMode::Mvcc);
            assert_eq!(r, Err(Error::Unsupported("MVCC: Window batching")), "{sql}");
        }
        for sql in [
            "PRAGMA buckwal_batching = off",
            "PRAGMA buckwal_batching",
            "PRAGMA buckwal_batching = 'off'",
        ] {
            assert_eq!(super::check_sql(sql, JournalMode::Mvcc), Ok(()), "{sql}");
        }
    }

    #[test]
    fn guard_refuses_mvcc_other_journal_modes_low_synchronous_and_attach() {
        for sql in [
            "PRAGMA journal_mode='mvcc'",
            "pragma JOURNAL_MODE = mvcc",
            "PRAGMA journal_mode = 'EXPERIMENTAL_MVCC'",
            "PRAGMA main.journal_mode = 'mvcc'",
            "PRAGMA journal_mode(\"mvcc\")",
            "SELECT 1; PRAGMA journal_mode=mvcc;",
        ] {
            assert_eq!(refused(sql), SWITCH_TO_MVCC, "{sql}");
        }
        for sql in [
            "PRAGMA journal_mode=delete",
            "PRAGMA journal_mode=TRUNCATE",
            "PRAGMA journal_mode='persist'",
            "PRAGMA journal_mode=memory",
            "PRAGMA journal_mode=off",
        ] {
            assert_eq!(refused(sql), "journal_mode other than WAL", "{sql}");
        }
        for sql in [
            "PRAGMA synchronous=OFF",
            "PRAGMA synchronous = normal",
            "PRAGMA synchronous=0",
            "PRAGMA main.synchronous(1)",
            "PRAGMA synchronous='off'",
        ] {
            assert_eq!(refused(sql), "synchronous below FULL", "{sql}");
        }
        for sql in ["ATTACH 'x.db' AS x", "attach database 'x.db' as x"] {
            assert_eq!(refused(sql), "ATTACH", "{sql}");
        }
    }

    #[test]
    fn batching_pragma_is_a_lone_buckwal_batching_pragma() {
        use BatchingPragma::{Read, Set};
        let w = Batching::Window {
            max_wait: std::time::Duration::from_millis(50),
            max_commits: 64,
            max_bytes: 1 << 20,
        };
        assert_eq!(
            batching_pragma("PRAGMA buckwal_batching = 'window 50ms 64 1MiB'"),
            Some(Set(Ok(w)))
        );
        assert_eq!(
            batching_pragma("pragma BUCKWAL_BATCHING=off"),
            Some(Set(Ok(Batching::Off)))
        );
        assert_eq!(batching_pragma("PRAGMA buckwal_batching"), Some(Read));
        let r = batching_pragma("PRAGMA buckwal_batching = 'nope'");
        assert!(matches!(r, Some(Set(Err(Error::Config(_))))), "{r:?}");
        assert_eq!(batching_pragma("SELECT 'pragma buckwal_batching'"), None);
        // Other spellings of the same statement.
        assert_eq!(
            batching_pragma("PRAGMA main.buckwal_batching('window 50ms 64 1MiB');"),
            Some(Set(Ok(w)))
        );
        assert_eq!(batching_pragma(" PRAGMA buckwal_batching ; "), Some(Read));
        let r = batching_pragma("PRAGMA buckwal_batching = NULL");
        assert!(matches!(r, Some(Set(Err(Error::Config(_))))), "{r:?}");
        // Not one statement, another pragma, or another schema: not ours to run (`check_sql`
        // refuses the buckwal_batching ones).
        for sql in [
            // Does not parse: the engine reports it.
            "PRAGMA buckwal_batching = 1 + 2",
            "PRAGMA buckwal_batching; SELECT 1",
            "SELECT 1; PRAGMA buckwal_batching",
            "PRAGMA journal_mode",
            "PRAGMA temp.buckwal_batching = off",
            "EXPLAIN PRAGMA buckwal_batching",
        ] {
            assert_eq!(batching_pragma(sql), None, "{sql}");
        }
    }

    #[test]
    fn guard_refuses_buckwal_batching_among_several_statements() {
        for sql in [
            "SELECT 1; PRAGMA buckwal_batching = off",
            "PRAGMA buckwal_batching; SELECT 1",
            "BEGIN; pragma MAIN.BUCKWAL_BATCHING('window 1ms 1 1'); COMMIT",
            // A parse error after it: the engine would still see the PRAGMA first.
            "PRAGMA buckwal_batching = off; SELEC",
        ] {
            assert_eq!(
                check_sql(sql),
                Err(Error::Config(
                    "PRAGMA buckwal_batching must be its own statement".into()
                )),
                "{sql}"
            );
            if !sql.ends_with("SELEC") {
                assert_eq!(check_sql_split(sql), Ok(()), "split: {sql}");
            }
        }
        for sql in [
            "PRAGMA buckwal_batching",
            "PRAGMA buckwal_batching = off;",
            "SELECT 'buckwal_batching'; SELECT 1",
        ] {
            assert_eq!(check_sql(sql), Ok(()), "{sql}");
        }
        // Another schema, or EXPLAIN: the engine would take it for an unknown pragma and
        // silently do nothing, so it is refused, as one statement or among several.
        for (sql, why) in [
            (
                "PRAGMA temp.buckwal_batching = off",
                "PRAGMA buckwal_batching applies to main only",
            ),
            (
                "PRAGMA aux.BUCKWAL_BATCHING",
                "PRAGMA buckwal_batching applies to main only",
            ),
            (
                "EXPLAIN PRAGMA buckwal_batching",
                "EXPLAIN of PRAGMA buckwal_batching is not supported",
            ),
            (
                "EXPLAIN QUERY PLAN PRAGMA main.buckwal_batching = off",
                "EXPLAIN of PRAGMA buckwal_batching is not supported",
            ),
        ] {
            let want = Err(Error::Config(why.into()));
            assert_eq!(check_sql(sql), want, "{sql}");
            assert_eq!(check_sql_split(&format!("SELECT 1; {sql}")), want, "{sql}");
        }
        // A value known to be bad is refused up front, before anything in the batch runs.
        for sql in [
            "INSERT INTO t VALUES (1); PRAGMA buckwal_batching = 'nope'",
            "SELECT 1; PRAGMA buckwal_batching = 'window 1ms 0 1'",
            "PRAGMA buckwal_batching = NULL",
        ] {
            let r = check_sql_split(sql);
            assert!(matches!(r, Err(Error::Config(_))), "{sql}: {r:?}");
        }
        // The split path still applies every other rule.
        assert_eq!(
            check_sql_split("PRAGMA buckwal_batching = off; PRAGMA synchronous = off"),
            Err(Error::Unsupported("synchronous below FULL"))
        );
    }

    /// Setting the key or the cipher would swap the connection's (translate/pragma.rs:644-655);
    /// the key comes from BUCKWAL_ENCRYPTION_KEY_FILE only. Reading them is fine.
    #[test]
    fn encryption_pragmas_are_refused() {
        let what = "encryption PRAGMAs (use BUCKWAL_ENCRYPTION_KEY_FILE)";
        for sql in [
            &format!("PRAGMA hexkey='{}'", "00".repeat(32)) as &str,
            "PRAGMA HEXKEY = \"0011\"",
            "PRAGMA main.hexkey('00')",
            "PRAGMA cipher = 'aes256gcm'",
            "pragma Cipher(aegis256)",
            "SELECT 1; PRAGMA cipher = 'aes128gcm'",
        ] {
            assert_eq!(refused(sql), what, "{sql}");
        }
        for sql in ["PRAGMA cipher", "PRAGMA hexkey", "PRAGMA main.cipher"] {
            assert_eq!(check_sql(sql), Ok(()), "{sql}");
        }
    }

    #[test]
    fn guard_allows_reads_wal_full_and_ordinary_sql() {
        for sql in [
            "SELECT 1",
            "PRAGMA journal_mode",
            "PRAGMA journal_mode=wal",
            "PRAGMA journal_mode='WAL'",
            "PRAGMA synchronous",
            "PRAGMA synchronous=FULL",
            "PRAGMA synchronous=2",
            "PRAGMA synchronous=extra",
            "SELECT 'pragma journal_mode=mvcc'",
            "CREATE TABLE attachments(id INTEGER PRIMARY KEY)",
            "PRAGMA integrity_check",
            // A parse error is left to the engine to report.
            "PRAGMA journal_mode = (",
        ] {
            assert_eq!(check_sql(sql), Ok(()), "{sql}");
        }
    }
}
