//! The SQL guard (D5, D7): statements the engine must never see, refused before `prepare`.
//! - `PRAGMA journal_mode` set to `mvcc`/`experimental_mvcc`: the engine writes the MVCC
//!   header to page 1 before it opens the MVCC log, so refusing the log afterwards would leave
//!   the header changed.
//! - `PRAGMA journal_mode` set to anything but `wal`: only the WAL is shipped.
//! - `PRAGMA synchronous` below FULL: a commit that does not fsync is not uploaded either.
//! - `ATTACH`: writes to an attached database are not shipped.
//!
//! Reading a pragma (no value) is fine. A statement that does not parse is left to the
//! engine, which reports it (and never runs it or anything after it).
use crate::error::{Error, Result};
use turso_parser::ast::{Cmd, Expr, Literal, PragmaBody, Stmt, UnaryOperator};
use turso_parser::parser::Parser;

/// Refuse a statement that would leave WAL mode, lower `synchronous` or attach a database.
pub(crate) fn check_sql(sql: &str) -> Result<()> {
    let lower = sql.to_ascii_lowercase();
    if !lower.contains("pragma") && !lower.contains("attach") {
        return Ok(());
    }
    for cmd in Parser::new(sql.as_bytes()) {
        let Ok(cmd) = cmd else {
            return Ok(());
        };
        let stmt = match cmd {
            Cmd::Stmt(s) | Cmd::Explain(s) => s,
            Cmd::ExplainQueryPlan { stmt, .. } => stmt,
        };
        match stmt {
            Stmt::Attach { .. } => return Err(Error::Unsupported("ATTACH")),
            Stmt::Pragma {
                name,
                body: Some(PragmaBody::Equals(value) | PragmaBody::Call(value)),
            } => check_pragma(name.name.as_str(), &value)?,
            _ => {}
        }
    }
    Ok(())
}

fn check_pragma(name: &str, value: &Expr) -> Result<()> {
    let value = pragma_value(value).map(|v| v.to_ascii_lowercase());
    if name.eq_ignore_ascii_case("journal_mode") {
        return match value.as_deref() {
            Some("wal") => Ok(()),
            Some("mvcc" | "experimental_mvcc") => Err(Error::Unsupported("MVCC")),
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
    use crate::error::Error;

    fn refused(sql: &str) -> &'static str {
        match check_sql(sql) {
            Err(Error::Unsupported(what)) => what,
            r => panic!("{sql}: {r:?}"),
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
            assert_eq!(refused(sql), "MVCC", "{sql}");
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
