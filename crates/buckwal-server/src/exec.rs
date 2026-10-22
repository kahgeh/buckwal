//! Runs SQL over HTTP v3 stream requests (JSON) on a `buckwal::Connection`.
//!
//! SQL text, arguments and tokens are never logged here.
use crate::proto::{
    describe_result, lib_error, stmt_result, to_value, Batch, BatchCond, BatchResult, ProtoError,
    Stmt, StmtResult, StreamRequest, StreamResponse, StreamResult,
};
use buckwal::{Batching, Connection, Database, Error, Value};
use serde::Deserialize;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use turso_parser::ast::{
    Cmd, Expr, InsertBody, Literal, PragmaBody, QualifiedName, Stmt as SqlStmt, UnaryOperator,
    Upsert, UpsertDo,
};
use turso_parser::parser::Parser;

/// The endpoint a request came in on: `/v2/pipeline` or `/v3/...`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Version {
    V2,
    V3,
}

/// One stream: its connection, its stored SQL and whether it is over.
pub struct StreamState {
    pub conn: Connection,
    pub sql: HashMap<i32, String>,
    pub closed: bool,
}

/// Most stored SQL texts per stream.
pub const MAX_STORED_SQL: usize = 1024;

/// Set once any stream sees the database fenced; every request after it is `FENCED`.
#[derive(Clone, Default, Debug)]
pub struct Fence(Arc<AtomicBool>);

impl Fence {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn is_set(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
    pub fn set(&self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// A new stream on `db` with the given busy timeout.
pub fn open_stream(db: &Database, busy: Duration) -> buckwal::Result<StreamState> {
    let conn = db.connect()?;
    conn.execute(&format!("PRAGMA busy_timeout = {}", busy.as_millis()), &[])?;
    Ok(StreamState {
        conn,
        sql: HashMap::new(),
        closed: false,
    })
}

fn fenced_error() -> ProtoError {
    ProtoError::new("FENCED", "this writer has been fenced by a takeover")
}

/// Record a library error: a fence ends the stream and trips the shared flag.
fn note(st: &mut StreamState, fence: &Fence, e: &Error) -> ProtoError {
    if matches!(e, Error::Fenced(_)) {
        fence.set();
        st.closed = true;
    }
    lib_error(e)
}

/// Run one request. `req` is parsed here so a bad request fails only itself.
pub fn execute_request(
    st: &mut StreamState,
    v: Version,
    req: serde_json::Value,
    fence: &Fence,
) -> StreamResult {
    if fence.is_set() {
        st.closed = true;
        return StreamResult::Error {
            error: fenced_error(),
        };
    }
    if st.closed {
        return StreamResult::Error {
            error: ProtoError::new("STREAM_CLOSED", "the stream is closed"),
        };
    }
    let kind = req.get("type").and_then(|t| t.as_str()).map(str::to_owned);
    let req = match serde_json::from_value::<StreamRequest>(req) {
        Ok(r) => r,
        Err(e) => {
            const KNOWN: [&str; 8] = [
                "close",
                "execute",
                "batch",
                "sequence",
                "describe",
                "store_sql",
                "close_sql",
                "get_autocommit",
            ];
            let error = match kind.as_deref() {
                Some(t) if !KNOWN.contains(&t) => {
                    ProtoError::new("UNSUPPORTED", format!("unsupported request type {t:?}"))
                }
                _ => ProtoError::new("PROTO_ERROR", format!("invalid request: {e}")),
            };
            return StreamResult::Error { error };
        }
    };
    match handle(st, v, req, fence) {
        Ok(response) => StreamResult::Ok { response },
        Err(error) => StreamResult::Error { error },
    }
}

fn handle(
    st: &mut StreamState,
    v: Version,
    req: StreamRequest,
    fence: &Fence,
) -> Result<StreamResponse, ProtoError> {
    match req {
        StreamRequest::Close => {
            st.closed = true;
            Ok(StreamResponse::Close)
        }
        StreamRequest::Execute { stmt } => Ok(StreamResponse::Execute {
            result: run_stmt(st, &stmt, fence)?,
        }),
        StreamRequest::Batch { batch } => Ok(StreamResponse::Batch {
            result: run_batch(st, v, &batch, fence)?,
        }),
        StreamRequest::Sequence { sql, sql_id } => {
            let sql = resolve_sql(st, sql, sql_id)?;
            st.conn
                .execute_batch(&sql)
                .map_err(|e| note(st, fence, &e))?;
            Ok(StreamResponse::Sequence)
        }
        // Never through the batching intercept: a describe of the PRAGMA changes nothing.
        StreamRequest::Describe { sql, sql_id } => {
            let sql = resolve_sql(st, sql, sql_id)?;
            let d = st.conn.describe(&sql).map_err(|e| note(st, fence, &e))?;
            Ok(StreamResponse::Describe {
                result: describe_result(d),
            })
        }
        StreamRequest::StoreSql { sql_id, sql } => {
            if !st.sql.contains_key(&sql_id) && st.sql.len() >= MAX_STORED_SQL {
                return Err(ProtoError::new(
                    "SQL_INPUT_ERROR",
                    format!("at most {MAX_STORED_SQL} SQL texts can be stored per stream"),
                ));
            }
            st.sql.insert(sql_id, sql);
            Ok(StreamResponse::StoreSql)
        }
        StreamRequest::CloseSql { sql_id } => {
            st.sql.remove(&sql_id);
            Ok(StreamResponse::CloseSql)
        }
        StreamRequest::GetAutocommit => {
            if v == Version::V2 {
                return Err(ProtoError::new("UNSUPPORTED", "get_autocommit needs /v3"));
            }
            Ok(StreamResponse::GetAutocommit {
                is_autocommit: st.conn.is_autocommit(),
            })
        }
    }
}

/// `sql` xor `sql_id`.
fn resolve_sql(
    st: &StreamState,
    sql: Option<String>,
    sql_id: Option<i32>,
) -> Result<String, ProtoError> {
    match (sql, sql_id) {
        (Some(s), None) => Ok(s),
        (None, Some(id)) => st.sql.get(&id).cloned().ok_or_else(|| {
            ProtoError::new("SQL_INPUT_ERROR", format!("no stored SQL with id {id}"))
        }),
        _ => Err(ProtoError::new(
            "SQL_INPUT_ERROR",
            "exactly one of sql and sql_id must be given",
        )),
    }
}

fn run_stmt(st: &mut StreamState, stmt: &Stmt, fence: &Fence) -> Result<StmtResult, ProtoError> {
    if fence.is_set() {
        st.closed = true;
        return Err(fenced_error());
    }
    if !stmt.args.is_empty() && !stmt.named_args.is_empty() {
        return Err(ProtoError::new(
            "SQL_INPUT_ERROR",
            "a statement cannot have both args and named_args",
        ));
    }
    let sql = resolve_sql(st, stmt.sql.clone(), stmt.sql_id)?;
    let want_rows = stmt.want_rows.unwrap_or(true);

    // With arguments the statement goes to the library, which refuses it.
    if st.conn.is_autocommit() && stmt.args.is_empty() && stmt.named_args.is_empty() {
        if let Some(parsed) = batching_set(&sql) {
            let start = Instant::now();
            // `set_batching` validates.
            let applied = parsed.and_then(|b| st.conn.set_batching(Some(b)));
            applied.map_err(|e| note(st, fence, &e))?;
            return Ok(StmtResult {
                cols: Vec::new(),
                rows: Vec::new(),
                affected_row_count: 0,
                last_insert_rowid: None,
                rows_read: 0,
                rows_written: 0,
                query_duration_ms: start.elapsed().as_secs_f64() * 1000.0,
            });
        }
    }

    let args = stmt
        .args
        .iter()
        .map(to_value)
        .collect::<Result<Vec<Value>, _>>()?;
    // Names match with the prefix stripped (PROTOCOL.md 8.1), so `:x`, `@x`, `$x` and `x` all
    // bind whichever of `:x`, `@x` and `$x` the statement has; `run` tries a bare name with each.
    let named = stmt
        .named_args
        .iter()
        .map(|a| {
            let name = a.name.strip_prefix([':', '@', '$']).unwrap_or(&a.name);
            Ok((name.to_owned(), to_value(&a.value)?))
        })
        .collect::<Result<Vec<(String, Value)>, ProtoError>>()?;
    let insert = insert_kind(&sql);
    // An upsert's changed row may have been updated, which leaves the rowid alone, so it
    // inserted only if the connection's last rowid moved. Every output carries that rowid.
    let before = match insert {
        Some(Insert::Upsert) => Some(
            st.conn
                .run("SELECT 1", &[], &[])
                .map_err(|e| note(st, fence, &e))?
                .last_insert_rowid,
        ),
        _ => None,
    };
    // After the upsert's extra call, so the duration is the statement's own.
    let start = Instant::now();
    let out = st
        .conn
        .run(&sql, &args, &named)
        .map_err(|e| note(st, fence, &e))?;
    let inserted = out.changes > 0
        && match insert {
            None => false,
            Some(Insert::Plain) => true,
            Some(Insert::Upsert) => before != Some(out.last_insert_rowid),
        };
    Ok(stmt_result(out, want_rows, inserted, start.elapsed()))
}

/// An `INSERT` or `REPLACE` statement, by how its changed rows count as inserted.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Insert {
    /// Every row it changed it inserted.
    Plain,
    /// It has an `ON CONFLICT ... DO UPDATE`: a changed row may be an update.
    Upsert,
}

/// The kind of `INSERT` (or `REPLACE`) `sql`'s statement is; `None` for any other statement.
fn insert_kind(sql: &str) -> Option<Insert> {
    if !mentions(sql, b"insert") && !mentions(sql, b"replace") {
        return None;
    }
    let Cmd::Stmt(SqlStmt::Insert { body, .. }) = Parser::new(sql.as_bytes()).next()?.ok()? else {
        return None;
    };
    let updates = match &body {
        InsertBody::Select(_, Some(upsert)) => upsert_updates(upsert),
        _ => false,
    };
    Some(if updates {
        Insert::Upsert
    } else {
        Insert::Plain
    })
}

/// Whether any clause of an `ON CONFLICT` chain is a `DO UPDATE`.
fn upsert_updates(u: &Upsert) -> bool {
    matches!(u.do_clause, UpsertDo::Set { .. }) || u.next.as_deref().is_some_and(upsert_updates)
}

/// Whether `sql` holds `word` (lowercase ASCII) in any case.
fn mentions(sql: &str, word: &[u8]) -> bool {
    sql.as_bytes()
        .windows(word.len())
        .any(|w| w.eq_ignore_ascii_case(word))
}

/// Run a batch. Conditions are checked up front, so a bad one fails the whole batch. Both
/// endpoints refuse a forward condition before this (`check_pipeline`, `check_steps`), so
/// here that check is a backstop.
pub fn run_batch(
    st: &mut StreamState,
    v: Version,
    batch: &Batch,
    fence: &Fence,
) -> Result<BatchResult, ProtoError> {
    for (i, step) in batch.steps.iter().enumerate() {
        if let Some(c) = &step.condition {
            check_cond(c, i, v)?;
        }
    }
    let n = batch.steps.len();
    let mut results: Vec<Option<StmtResult>> = vec![None; n];
    let mut errors: Vec<Option<ProtoError>> = vec![None; n];
    // Some(true) ok, Some(false) error, None skipped.
    let mut outcome: Vec<Option<bool>> = vec![None; n];
    for (i, step) in batch.steps.iter().enumerate() {
        let run = match &step.condition {
            None => true,
            Some(c) => eval_cond(c, &outcome, st),
        };
        if !run {
            continue;
        }
        match run_stmt(st, &step.stmt, fence) {
            Ok(r) => {
                results[i] = Some(r);
                outcome[i] = Some(true);
            }
            Err(e) => {
                errors[i] = Some(e);
                outcome[i] = Some(false);
            }
        }
    }
    Ok(BatchResult {
        step_results: results,
        step_errors: errors,
    })
}

/// The protocol errors that fail a whole pipeline before anything in it runs (PROTOCOL.md
/// 5.3): a `close` that is not the last request, or a batch condition on its own or a later
/// step. A condition that does not parse is left to its request, which then fails alone.
pub fn check_pipeline(requests: &[serde_json::Value]) -> Result<(), ProtoError> {
    let last = requests.len().saturating_sub(1);
    for (i, r) in requests.iter().enumerate() {
        match r.get("type").and_then(|t| t.as_str()) {
            Some("close") if i < last => {
                return Err(ProtoError::new(
                    "PROTO_ERROR",
                    format!("request {i} is a close, which must be the last request"),
                ));
            }
            Some("batch") => {
                let steps = r.pointer("/batch/steps").and_then(|s| s.as_array());
                for (at, step) in steps.into_iter().flatten().enumerate() {
                    let cond = step.get("condition").map(BatchCond::deserialize);
                    if let Some(Ok(c)) = cond {
                        check_cond(&c, at, Version::V3)?;
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// A cursor batch's conditions refer only to earlier steps (PROTOCOL.md 6.2.1).
pub fn check_steps(batch: &Batch) -> Result<(), ProtoError> {
    for (at, step) in batch.steps.iter().enumerate() {
        if let Some(c) = &step.condition {
            check_cond(c, at, Version::V3)?;
        }
    }
    Ok(())
}

fn check_cond(c: &BatchCond, at: usize, v: Version) -> Result<(), ProtoError> {
    match c {
        BatchCond::Ok { step } | BatchCond::Error { step } => {
            if (*step as usize) < at {
                Ok(())
            } else {
                Err(ProtoError::new(
                    "PROTO_ERROR",
                    format!(
                        "step {at} has a condition on step {step}, which is not an earlier step"
                    ),
                ))
            }
        }
        BatchCond::Not { cond } => check_cond(cond, at, v),
        BatchCond::And { conds } | BatchCond::Or { conds } => {
            conds.iter().try_for_each(|c| check_cond(c, at, v))
        }
        BatchCond::IsAutocommit => {
            if v == Version::V2 {
                Err(ProtoError::new(
                    "UNSUPPORTED",
                    "the is_autocommit condition needs /v3",
                ))
            } else {
                Ok(())
            }
        }
    }
}

fn eval_cond(c: &BatchCond, outcome: &[Option<bool>], st: &StreamState) -> bool {
    match c {
        BatchCond::Ok { step } => outcome[*step as usize] == Some(true),
        // The negation of `ok`: a skipped step counts too (PROTOCOL.md 6.2.1).
        BatchCond::Error { step } => outcome[*step as usize] != Some(true),
        BatchCond::Not { cond } => !eval_cond(cond, outcome, st),
        BatchCond::And { conds } => conds.iter().all(|c| eval_cond(c, outcome, st)),
        BatchCond::Or { conds } => conds.iter().any(|c| eval_cond(c, outcome, st)),
        BatchCond::IsAutocommit => st.conn.is_autocommit(),
    }
}

/// `sql` is exactly one `PRAGMA [main.]durable_batching = value` or `(value)`: the batching
/// it asks for (or the error for a value that is not text). `None` for anything else, which
/// then goes to the library (including a read of the setting).
fn batching_set(sql: &str) -> Option<buckwal::Result<Batching>> {
    if !mentions(sql, b"durable_batching") {
        return None;
    }
    let mut parser = Parser::new(sql.as_bytes());
    let Cmd::Stmt(SqlStmt::Pragma { name, body }) = parser.next()?.ok()? else {
        return None;
    };
    if !is_batching(&name) || parser.next().is_some() {
        return None;
    }
    let Some(PragmaBody::Equals(value) | PragmaBody::Call(value)) = body.as_ref() else {
        return None;
    };
    Some(match pragma_value(value) {
        Some(text) => Batching::from_str(&text),
        None => Err(Error::Config(
            "PRAGMA durable_batching takes off or 'window <max_wait> <max_commits> <max_bytes>'"
                .into(),
        )),
    })
}

fn is_batching(name: &QualifiedName) -> bool {
    name.name.as_str().eq_ignore_ascii_case("durable_batching")
        && name
            .db_name
            .as_ref()
            .is_none_or(|db| db.as_str().eq_ignore_ascii_case("main"))
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

/// Strip one level of SQL quoting: '...', "...", `...` or [...].
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
    use buckwal::store::{FaultStore, MemStore};
    use buckwal::{open_with_store, takeover_with_store, Config};
    use serde_json::{json, Value as J};
    use std::path::Path;
    use tempfile::TempDir;

    fn config(dir: &Path) -> Config {
        let mut c = Config::new(dir, "http://127.0.0.1:1", "b", "k", "s");
        c.retry_deadline = Duration::from_secs(2);
        c.request_timeout = Duration::from_millis(500);
        c.lease = Duration::from_secs(5);
        c
    }

    struct Fx {
        _dir: TempDir,
        db: Database,
        st: StreamState,
        fence: Fence,
    }

    fn fx() -> Fx {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(MemStore::new());
        let db = open_with_store(config(&dir.path().join("db")), store).unwrap();
        let st = open_stream(&db, Duration::from_millis(100)).unwrap();
        Fx {
            _dir: dir,
            db,
            st,
            fence: Fence::new(),
        }
    }

    impl Fx {
        fn req(&mut self, v: Version, r: J) -> StreamResult {
            execute_request(&mut self.st, v, r, &self.fence)
        }
        fn ok(&mut self, r: J) -> J {
            match self.req(Version::V3, r) {
                StreamResult::Ok { response } => serde_json::to_value(response).unwrap(),
                StreamResult::Error { error } => panic!("{error:?}"),
            }
        }
        fn err(&mut self, v: Version, r: J) -> String {
            match self.req(v, r) {
                StreamResult::Error { error } => error.code.unwrap(),
                StreamResult::Ok { response } => panic!("{response:?}"),
            }
        }
        fn sql(&mut self, sql: &str) -> J {
            self.ok(json!({"type":"execute","stmt":{"sql":sql}}))["result"].clone()
        }
    }

    fn setting(f: &mut Fx) -> String {
        let r = f.sql("PRAGMA durable_batching");
        r["rows"][0][0]["value"].as_str().unwrap().to_string()
    }

    #[test]
    fn execute_insert_then_select_with_columns() {
        let mut f = fx();
        f.sql("CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT)");
        let r = f.sql("INSERT INTO t(name) VALUES ('a')");
        assert_eq!(r["affected_row_count"], 1);
        assert_eq!(r["last_insert_rowid"], "1");
        let r = f.sql("SELECT id, name FROM t");
        assert_eq!(r["cols"][0]["name"], "id");
        assert_eq!(r["cols"][1]["name"], "name");
        assert_eq!(r["rows"][0][0], json!({"type":"integer","value":"1"}));
        assert_eq!(r["rows"][0][1], json!({"type":"text","value":"a"}));
    }

    #[test]
    fn non_finite_results_are_null_floats() {
        let mut f = fx();
        let r = f.sql("SELECT 1e999, -1e999");
        let nf = json!({"type":"float","value":null});
        assert_eq!(r["rows"][0], json!([nf, nf]));
        // A write whose RETURNING gives inf is answered (it was committed).
        f.sql("CREATE TABLE r(x REAL)");
        let r = f.sql("INSERT INTO r VALUES (1e999) RETURNING x");
        assert_eq!(r["rows"][0][0], nf);
        assert_eq!(f.sql("SELECT count(*) FROM r")["rows"][0][0]["value"], "1");
        // Input is unchanged: a null float value is still not accepted.
        let arg = json!({"type":"float","value":null});
        let req = json!({"type":"execute","stmt":{"sql":"SELECT ?","args":[arg]}});
        assert_eq!(f.err(Version::V3, req), "PROTO_ERROR");
    }

    /// `last_insert_rowid` is set only when the statement inserted a row (PROTOCOL.md 8.4),
    /// and `rows_written` is the changed-row count.
    #[test]
    fn last_insert_rowid_only_when_the_statement_inserted() {
        let mut f = fx();
        let mut check = |sql: &str, rowid: J, changed: u64| {
            let r = f.sql(sql);
            assert_eq!(r["last_insert_rowid"], rowid, "{sql}: {r}");
            assert_eq!(r["affected_row_count"], changed, "{sql}: {r}");
            assert_eq!(r["rows_written"], changed, "{sql}: {r}");
            assert_eq!(r["rows_read"], 0, "{sql}: {r}");
        };
        let null = J::Null;
        check(
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT UNIQUE)",
            null.clone(),
            0,
        );
        check("CREATE TABLE log(x)", null.clone(), 0);
        check(
            "CREATE TRIGGER tr AFTER UPDATE ON t BEGIN INSERT INTO log VALUES (new.id); END",
            null.clone(),
            0,
        );
        check("INSERT INTO t VALUES (5, 'a')", json!("5"), 1);
        check("SELECT * FROM t", null.clone(), 0);
        check("DELETE FROM t WHERE id = 5", null.clone(), 1);
        // The same rowid again after the DELETE: still an insert.
        check("INSERT INTO t VALUES (5, 'a')", json!("5"), 1);
        check("INSERT OR IGNORE INTO t VALUES (5, 'z')", null.clone(), 0);
        check("INSERT INTO t(v) VALUES ('p'), ('q')", json!("7"), 2);
        // The trigger's insert into `log` is not this statement's.
        check("UPDATE t SET v = v || 'x' WHERE id = 5", null.clone(), 1);
        check("REPLACE INTO t VALUES (6, 'r')", json!("6"), 1);
        check("INSERT OR REPLACE INTO t VALUES (6, 's')", json!("6"), 1);
        check(
            "WITH c AS (SELECT 20 AS i) INSERT INTO t SELECT i, 'w' FROM c",
            json!("20"),
            1,
        );
        check("EXPLAIN INSERT INTO t VALUES (21, 'e')", null.clone(), 0);
        // An upsert that only updated inserted nothing; one that inserted did.
        check(
            "INSERT INTO t VALUES (30, 'q') ON CONFLICT(v) DO UPDATE SET v = 'q2'",
            null.clone(),
            1,
        );
        check(
            "INSERT INTO t VALUES (31, 'new') ON CONFLICT(v) DO UPDATE SET v = 'n2'",
            json!("31"),
            1,
        );
        check(
            "INSERT INTO t VALUES (32, 'new') ON CONFLICT(v) DO NOTHING",
            null.clone(),
            0,
        );
        check("PRAGMA durable_batching = off", null.clone(), 0);
        let r = f.ok(json!({"type":"execute","stmt":{
            "sql":"INSERT INTO t VALUES (?, 'arg') RETURNING id",
            "args":[{"type":"integer","value":"40"}]}}));
        assert_eq!(r["result"]["last_insert_rowid"], "40");
        assert_eq!(r["result"]["rows"][0][0]["value"], "40");
    }

    #[test]
    fn named_args_reach_run() {
        let mut f = fx();
        let r = f.ok(json!({"type":"execute","stmt":{"sql":"SELECT :a",
            "named_args":[{"name":"a","value":{"type":"integer","value":"1"}}]}}));
        assert_eq!(
            r["result"]["rows"][0][0],
            json!({"type":"integer","value":"1"})
        );
    }

    /// The name is matched with its prefix stripped (PROTOCOL.md 8.1): `x`, `:x`, `@x` and
    /// `$x` each bind `:x`, `@x` or `$x`.
    #[test]
    fn named_args_bind_whatever_their_prefix() {
        let mut f = fx();
        for param in [":x", "@x", "$x"] {
            for name in ["x", ":x", "@x", "$x"] {
                let r = f.ok(
                    json!({"type":"execute","stmt":{"sql":format!("SELECT {param}"),
                    "named_args":[{"name":name,"value":{"type":"integer","value":"7"}}]}}),
                );
                assert_eq!(r["result"]["rows"][0][0]["value"], "7", "{param} {name:?}");
            }
        }
        let r = f.ok(json!({"type":"execute","stmt":{"sql":"SELECT $a, :b",
            "named_args":[{"name":":a","value":{"type":"integer","value":"1"}},
                          {"name":"@b","value":{"type":"integer","value":"2"}}]}}));
        assert_eq!(r["result"]["rows"][0][0]["value"], "1");
        assert_eq!(r["result"]["rows"][0][1]["value"], "2");
        // A name the statement does not have is still an error.
        let unknown = json!({"type":"execute","stmt":{"sql":"SELECT :x",
            "named_args":[{"name":"$y","value":{"type":"integer","value":"7"}}]}});
        assert_eq!(f.err(Version::V3, unknown), "SQL_INPUT_ERROR");
        // `?` is not a prefix to strip: `?2` binds the second parameter.
        let r = f.ok(json!({"type":"execute","stmt":{"sql":"SELECT ?1, ?2",
            "named_args":[{"name":"?2","value":{"type":"integer","value":"2"}},
                          {"name":"?1","value":{"type":"integer","value":"1"}}]}}));
        assert_eq!(r["result"]["rows"][0][0]["value"], "1");
        assert_eq!(r["result"]["rows"][0][1]["value"], "2");
        // `:x` and `$x` are two parameters, but both names strip to `x`.
        let one = json!({"type":"integer","value":"1"});
        for names in [json!(["x"]), json!([":x", "$x"])] {
            let named: Vec<J> = names
                .as_array()
                .unwrap()
                .iter()
                .map(|n| json!({"name":n,"value":one}))
                .collect();
            let both = json!({"type":"execute","stmt":{"sql":"SELECT :x, $x","named_args":named}});
            assert_eq!(f.err(Version::V3, both), "SQL_INPUT_ERROR", "{names}");
        }
    }

    #[test]
    fn mixed_args_and_named_args_are_sql_input_error() {
        let mut f = fx();
        let one = json!({"type":"integer","value":"1"});
        let named = json!([{"name":"a","value":one}]);
        let mixed = json!({"sql":"SELECT ?, :a","args":[one],"named_args":named});
        assert_eq!(
            f.err(Version::V3, json!({"type":"execute","stmt":mixed})),
            "SQL_INPUT_ERROR"
        );
        let r = f.ok(json!({"type":"batch","batch":{"steps":[{"stmt":mixed}]}}));
        assert_eq!(r["result"]["step_errors"][0]["code"], "SQL_INPUT_ERROR");
        // An empty array is the same as leaving the field out.
        for stmt in [
            json!({"sql":"SELECT :a","args":[],"named_args":named}),
            json!({"sql":"SELECT ?","args":[one],"named_args":[]}),
        ] {
            let r = f.ok(json!({"type":"execute","stmt":stmt}));
            assert_eq!(r["result"]["rows"][0][0]["value"], "1", "{stmt}");
        }
    }

    #[test]
    fn stored_sql_by_id_and_unknown_id() {
        let mut f = fx();
        f.ok(json!({"type":"store_sql","sql_id":7,"sql":"SELECT 41 + 1"}));
        let r = f.ok(json!({"type":"execute","stmt":{"sql_id":7}}));
        assert_eq!(r["result"]["rows"][0][0]["value"], "42");
        // A stored id is replaced.
        f.ok(json!({"type":"store_sql","sql_id":7,"sql":"SELECT 1"}));
        let r = f.ok(json!({"type":"execute","stmt":{"sql_id":7}}));
        assert_eq!(r["result"]["rows"][0][0]["value"], "1");
        f.ok(json!({"type":"close_sql","sql_id":7}));
        assert_eq!(
            f.err(Version::V3, json!({"type":"execute","stmt":{"sql_id":7}})),
            "SQL_INPUT_ERROR"
        );
        // Closing an unknown id is fine.
        f.ok(json!({"type":"close_sql","sql_id":99}));
        // sql and sql_id together, or neither.
        assert_eq!(
            f.err(
                Version::V3,
                json!({"type":"execute","stmt":{"sql":"SELECT 1","sql_id":1}})
            ),
            "SQL_INPUT_ERROR"
        );
        assert_eq!(
            f.err(Version::V3, json!({"type":"execute","stmt":{}})),
            "SQL_INPUT_ERROR"
        );
    }

    #[test]
    fn store_sql_cap() {
        let mut f = fx();
        for id in 0..MAX_STORED_SQL as i32 {
            f.ok(json!({"type":"store_sql","sql_id":id,"sql":"SELECT 1"}));
        }
        assert_eq!(
            f.err(
                Version::V3,
                json!({"type":"store_sql","sql_id":5000,"sql":"SELECT 1"})
            ),
            "SQL_INPUT_ERROR"
        );
        // Replacing an existing id is still allowed at the cap.
        f.ok(json!({"type":"store_sql","sql_id":3,"sql":"SELECT 2"}));
    }

    fn step(sql: &str, cond: Option<J>) -> J {
        match cond {
            Some(c) => json!({"condition":c,"stmt":{"sql":sql,"want_rows":false}}),
            None => json!({"stmt":{"sql":sql,"want_rows":false}}),
        }
    }

    #[test]
    fn batch_conditions_follow_the_transaction_pattern() {
        let mut f = fx();
        f.sql("CREATE TABLE t(id INTEGER UNIQUE)");
        let ok = |s: u32| json!({"type":"ok","step":s});
        let r = f.ok(json!({"type":"batch","batch":{"steps":[
            step("BEGIN", None),
            step("INSERT INTO t VALUES (1)", Some(ok(0))),
            step("INSERT INTO t VALUES (1)", Some(ok(1))),
            step("INSERT INTO t VALUES (2)", Some(ok(2))),
            step("COMMIT", Some(ok(3))),
            step("ROLLBACK", Some(json!({"type":"not","cond":ok(4)}))),
        ]}}));
        let res = &r["result"];
        assert_eq!(res["step_errors"][2]["code"], "SQLITE_CONSTRAINT");
        assert!(res["step_results"][2].is_null());
        for i in [3, 4] {
            assert!(res["step_results"][i].is_null(), "{i}");
            assert!(res["step_errors"][i].is_null(), "{i}");
        }
        assert!(!res["step_results"][5].is_null(), "ROLLBACK ran");
        assert_eq!(
            f.ok(json!({"type":"get_autocommit"}))["is_autocommit"],
            true
        );
        let r = f.sql("SELECT count(*) FROM t");
        assert_eq!(r["rows"][0][0]["value"], "0");
    }

    /// `error` is the negation of `ok`: true for a failed and for a skipped step.
    #[test]
    fn batch_error_condition_is_not_ok() {
        let mut f = fx();
        let ok = |s: u32| json!({"type":"ok","step":s});
        let error = |s: u32| json!({"type":"error","step":s});
        let r = f.ok(json!({"type":"batch","batch":{"steps":[
            step("SELECT 1", None),
            step("SELECT * FROM nope", None),
            step("SELECT 2", Some(ok(0))),
            step("SELECT 3", Some(ok(1))),
            step("SELECT 4", Some(error(1))),
            step("SELECT 5", Some(json!({"type":"not","cond":error(3)}))),
            step("SELECT 6", Some(error(3))),
            step("SELECT 7", Some(error(0))),
        ]}}));
        let res = &r["result"];
        let ran: Vec<bool> = (0..8).map(|i| !res["step_results"][i].is_null()).collect();
        assert_eq!(
            ran,
            [true, false, true, false, true, false, true, false],
            "{res}"
        );
        assert!(!res["step_errors"][1].is_null());
    }

    #[test]
    fn batch_rejects_conditions_on_later_steps() {
        let mut f = fx();
        let code = f.err(
            Version::V3,
            json!({"type":"batch","batch":{"steps":[
                step("SELECT 1", Some(json!({"type":"ok","step":1}))),
                step("SELECT 2", None)]}}),
        );
        assert_eq!(code, "PROTO_ERROR");
    }

    #[test]
    fn check_pipeline_close_position() {
        let close = json!({"type":"close"});
        let sel = json!({"type":"execute","stmt":{"sql":"SELECT 1"}});
        for ok in [
            vec![],
            vec![close.clone()],
            vec![sel.clone(), close.clone()],
        ] {
            assert_eq!(check_pipeline(&ok), Ok(()), "{ok:?}");
        }
        for bad in [
            vec![close.clone(), sel.clone()],
            vec![sel.clone(), close.clone(), sel.clone()],
            vec![close.clone(), close.clone()],
        ] {
            let e = check_pipeline(&bad).unwrap_err();
            assert_eq!(e.code.as_deref(), Some("PROTO_ERROR"), "{bad:?}");
            assert!(e.message.contains("close"), "{e:?}");
        }
    }

    #[test]
    fn check_pipeline_forward_conditions_at_any_depth() {
        let b = |cond: J| {
            vec![json!({"type":"batch","batch":{"steps":[
                step("SELECT 1", None), step("SELECT 2", Some(cond))]}})]
        };
        let ok0 = json!({"type":"ok","step":0});
        for good in [
            ok0.clone(),
            json!({"type":"not","cond":{"type":"error","step":0}}),
            json!({"type":"and","conds":[ok0.clone(),{"type":"is_autocommit"}]}),
            // Malformed: left to the request, which fails alone.
            json!({"type":"bogus"}),
            json!({"type":"ok","step":"x"}),
        ] {
            assert_eq!(check_pipeline(&b(good.clone())), Ok(()), "{good}");
        }
        for bad in [
            json!({"type":"ok","step":1}),
            json!({"type":"error","step":2}),
            json!({"type":"or","conds":[ok0.clone(),
                {"type":"not","cond":{"type":"and","conds":[{"type":"error","step":1}]}}]}),
        ] {
            let e = check_pipeline(&b(bad.clone())).unwrap_err();
            assert_eq!(e.code.as_deref(), Some("PROTO_ERROR"), "{bad}");
        }
        // The cursor's batch, already parsed.
        let batch: Batch = serde_json::from_value(json!({"steps":[
            step("SELECT 1", Some(json!({"type":"not","cond":{"type":"ok","step":0}})))]}))
        .unwrap();
        assert_eq!(
            check_steps(&batch).unwrap_err().code.as_deref(),
            Some("PROTO_ERROR")
        );
    }

    #[test]
    fn batch_is_autocommit_condition_v3_only() {
        let mut f = fx();
        let b = json!({"type":"batch","batch":{"steps":[
            step("SELECT 1", Some(json!({"type":"is_autocommit"})))]}});
        let r = f.req(Version::V3, b.clone());
        match r {
            StreamResult::Ok {
                response: StreamResponse::Batch { result },
            } => assert!(result.step_results[0].is_some()),
            other => panic!("{other:?}"),
        }
        assert_eq!(f.err(Version::V2, b), "UNSUPPORTED");
    }

    #[test]
    fn get_autocommit_v3_and_v2() {
        let mut f = fx();
        assert_eq!(
            f.ok(json!({"type":"get_autocommit"}))["is_autocommit"],
            true
        );
        f.sql("BEGIN");
        assert_eq!(
            f.ok(json!({"type":"get_autocommit"}))["is_autocommit"],
            false
        );
        assert_eq!(
            f.err(Version::V2, json!({"type":"get_autocommit"})),
            "UNSUPPORTED"
        );
    }

    #[test]
    fn sequence_runs_all_statements() {
        let mut f = fx();
        f.ok(json!({"type":"sequence","sql":"CREATE TABLE t(a); INSERT INTO t VALUES (1); INSERT INTO t VALUES (2);"}));
        assert_eq!(f.sql("SELECT count(*) FROM t")["rows"][0][0]["value"], "2");
        f.ok(json!({"type":"store_sql","sql_id":1,"sql":"INSERT INTO t VALUES (3)"}));
        f.ok(json!({"type":"sequence","sql_id":1}));
        assert_eq!(f.sql("SELECT count(*) FROM t")["rows"][0][0]["value"], "3");
    }

    #[test]
    fn describe_result_shape() {
        let mut f = fx();
        f.sql("CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT)");
        let r = f.ok(
            json!({"type":"describe","sql":"SELECT id, name, 1 FROM t WHERE id = :id AND ? > ?3"}),
        );
        assert_eq!(
            r,
            json!({"type":"describe","result":{
                "params":[{"name":"id"},{"name":null},{"name":null}],
                "cols":[
                    {"name":"id","decltype":"INTEGER"},
                    {"name":"name","decltype":"TEXT"},
                    {"name":"1","decltype":null}],
                "is_explain":false,
                "is_readonly":true}})
        );
        let r = f.ok(json!({"type":"describe","sql":"EXPLAIN DELETE FROM t"}));
        assert_eq!(r["result"]["is_explain"], true);
        assert_eq!(r["result"]["is_readonly"], true);
        let r = f.ok(json!({"type":"describe","sql":"DELETE FROM t"}));
        assert_eq!(
            r["result"],
            json!({"params":[],"cols":[],"is_explain":false,"is_readonly":false})
        );
        // On /v2 too.
        let StreamResult::Ok { .. } =
            f.req(Version::V2, json!({"type":"describe","sql":"SELECT 1"}))
        else {
            panic!("describe on v2")
        };
    }

    #[test]
    fn describe_by_sql_id_and_bad_sql_choice() {
        let mut f = fx();
        f.ok(json!({"type":"store_sql","sql_id":3,"sql":"SELECT ?"}));
        let r = f.ok(json!({"type":"describe","sql_id":3}));
        assert_eq!(r["result"]["params"], json!([{"name":null}]));
        for bad in [
            json!({"type":"describe","sql_id":4}),
            json!({"type":"describe","sql":"SELECT 1","sql_id":3}),
            json!({"type":"describe"}),
        ] {
            assert_eq!(f.err(Version::V3, bad.clone()), "SQL_INPUT_ERROR", "{bad}");
        }
    }

    #[test]
    fn describe_of_lower_synchronous_is_an_error_and_sync_stays_full() {
        let mut f = fx();
        let r = f.req(
            Version::V3,
            json!({"type":"describe","sql":"PRAGMA synchronous=OFF"}),
        );
        let StreamResult::Error { error } = r else {
            panic!("{r:?}")
        };
        assert_eq!(error.code.as_deref(), Some("UNSUPPORTED"));
        assert!(error.message.contains("synchronous"), "{error:?}");
        let r = f.sql("PRAGMA synchronous");
        assert_eq!(r["rows"][0][0], json!({"type":"integer","value":"2"}));
    }

    #[test]
    fn describe_of_batching_pragma_is_fixed_and_not_intercepted() {
        let mut f = fx();
        f.sql("PRAGMA durable_batching = 'window 20ms 64 1MiB'");
        for sql in ["PRAGMA durable_batching = 'off'", "PRAGMA durable_batching"] {
            let r = f.ok(json!({"type":"describe","sql":sql}));
            assert_eq!(
                r["result"],
                json!({"params":[],"cols":[{"name":"durable_batching","decltype":null}],
                    "is_explain":false,"is_readonly":false}),
                "{sql}"
            );
            assert_eq!(setting(&mut f), "window 20ms 64 1MiB", "{sql}");
        }
    }

    #[test]
    fn describe_on_a_fenced_server_is_fenced() {
        let mem = Arc::new(MemStore::new());
        let dir_a = tempfile::tempdir().unwrap();
        let db = open_with_store(config(&dir_a.path().join("db")), mem.clone()).unwrap();
        let mut st = open_stream(&db, Duration::from_millis(100)).unwrap();
        let fence = Fence::new();
        st.conn.execute("CREATE TABLE t(id INTEGER)", &[]).unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        takeover_with_store(config(&dir_b.path().join("db")), mem).unwrap();
        let req = |st: &mut StreamState, r: J| execute_request(st, Version::V3, r, &fence);
        let insert = json!({"type":"execute","stmt":{"sql":"INSERT INTO t VALUES (1)"}});
        let StreamResult::Error { error } = req(&mut st, insert) else {
            panic!("the insert is fenced")
        };
        assert_eq!(error.code.as_deref(), Some("FENCED"));
        let mut fresh = open_stream(&db, Duration::from_millis(100)).unwrap();
        let describe = json!({"type":"describe","sql":"SELECT id FROM t"});
        match req(&mut fresh, describe) {
            StreamResult::Error { error } => assert_eq!(error.code.as_deref(), Some("FENCED")),
            other => panic!("{other:?}"),
        }
        assert!(fresh.closed);
    }

    #[test]
    fn unknown_request_type_is_unsupported_and_malformed_is_proto_error() {
        let mut f = fx();
        assert_eq!(
            f.err(Version::V3, json!({"type":"open_cursor"})),
            "UNSUPPORTED"
        );
        for bad in [json!({}), json!({"type":5}), json!({"type":"execute"})] {
            assert_eq!(f.err(Version::V3, bad.clone()), "PROTO_ERROR", "{bad}");
        }
    }

    #[test]
    fn batching_pragma_variants_are_not_intercepted() {
        let mut f = fx();
        for sql in [
            "PRAGMA durable_batching = 'off'; SELECT 1",
            "EXPLAIN PRAGMA durable_batching = 'off'",
            "PRAGMA temp.durable_batching = 'off'",
        ] {
            f.sql("PRAGMA durable_batching = 'window 20ms 64 1MiB'");
            f.err(Version::V3, json!({"type":"execute","stmt":{"sql":sql}}));
            assert_eq!(setting(&mut f), "window 20ms 64 1MiB", "{sql}");
        }
    }

    #[test]
    fn fenced_inside_a_batch_sets_the_fence_and_later_steps_answer_fenced() {
        let mem = Arc::new(MemStore::new());
        let dir_a = tempfile::tempdir().unwrap();
        let db = open_with_store(config(&dir_a.path().join("db")), mem.clone()).unwrap();
        let mut st = open_stream(&db, Duration::from_millis(100)).unwrap();
        let fence = Fence::new();
        st.conn.execute("CREATE TABLE t(id INTEGER)", &[]).unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        takeover_with_store(config(&dir_b.path().join("db")), mem).unwrap();
        let r = execute_request(
            &mut st,
            Version::V3,
            json!({"type":"batch","batch":{"steps":[
                step("INSERT INTO t VALUES (1)", None),
                step("SELECT 1", None),
                step("SELECT 2", None)]}}),
            &fence,
        );
        let StreamResult::Ok {
            response: StreamResponse::Batch { result },
        } = r
        else {
            panic!("{r:?}")
        };
        assert!(fence.is_set());
        assert!(st.closed);
        for i in 0..3 {
            assert!(result.step_results[i].is_none(), "{i}");
            let code = result.step_errors[i].as_ref().unwrap().code.as_deref();
            assert_eq!(code, Some("FENCED"), "{i}");
        }
    }

    #[test]
    fn batch_condition_on_the_current_step_is_proto_error() {
        let mut f = fx();
        let code = f.err(
            Version::V3,
            json!({"type":"batch","batch":{"steps":[
                step("SELECT 1", None),
                step("SELECT 2", Some(json!({"type":"ok","step":1})))]}}),
        );
        assert_eq!(code, "PROTO_ERROR");
    }

    #[test]
    fn request_after_close_is_stream_closed() {
        let mut f = fx();
        f.ok(json!({"type":"close"}));
        assert!(f.st.closed);
        assert_eq!(
            f.err(
                Version::V3,
                json!({"type":"execute","stmt":{"sql":"SELECT 1"}})
            ),
            "STREAM_CLOSED"
        );
    }

    #[test]
    fn per_stream_batching_pragma_in_autocommit() {
        let mut f = fx();
        assert_eq!(setting(&mut f), "off");
        let r = f.sql("PRAGMA durable_batching = 'window 20ms 64 1MiB'");
        assert_eq!(r["rows"], json!([]));
        assert_eq!(setting(&mut f), "window 20ms 64 1MiB");
        let mut other = open_stream(&f.db, Duration::from_millis(100)).unwrap();
        let fence = Fence::new();
        let read = execute_request(
            &mut other,
            Version::V3,
            json!({"type":"execute","stmt":{"sql":"PRAGMA durable_batching"}}),
            &fence,
        );
        match read {
            StreamResult::Ok {
                response: StreamResponse::Execute { result },
            } => assert_eq!(
                result.rows[0][0],
                crate::proto::HValue::Text {
                    value: "off".into()
                }
            ),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn batching_pragma_inside_transaction_goes_to_library() {
        let mut f = fx();
        f.sql("PRAGMA durable_batching = 'window 20ms 64 1MiB'");
        f.sql("BEGIN");
        f.sql("PRAGMA durable_batching = off");
        assert_eq!(setting(&mut f), "off");
        f.sql("COMMIT");
        assert_eq!(setting(&mut f), "window 20ms 64 1MiB");
    }

    #[test]
    fn batching_pragma_with_args_is_not_intercepted() {
        let mut f = fx();
        f.sql("PRAGMA durable_batching = 'window 20ms 64 1MiB'");
        let arg = json!({"type":"integer","value":"1"});
        for stmt in [
            json!({"sql":"PRAGMA durable_batching = 'off'","args":[arg]}),
            json!({"sql":"PRAGMA durable_batching = 'off'","named_args":[{"name":"a","value":arg}]}),
        ] {
            match f.req(Version::V3, json!({"type":"execute","stmt":stmt})) {
                StreamResult::Error { .. } => {}
                other => panic!("{stmt}: {other:?}"),
            }
            assert_eq!(setting(&mut f), "window 20ms 64 1MiB", "{stmt}");
        }
    }

    #[test]
    fn bad_batching_value_is_sql_input_error() {
        let mut f = fx();
        for sql in [
            "PRAGMA durable_batching = 'bogus'",
            "PRAGMA durable_batching = 'window 0ms 0 0'",
        ] {
            assert_eq!(
                f.err(Version::V3, json!({"type":"execute","stmt":{"sql":sql}})),
                "SQL_INPUT_ERROR",
                "{sql}"
            );
        }
        assert_eq!(setting(&mut f), "off");
    }

    #[test]
    fn fenced_library_error_sets_the_fence_and_later_requests_skip_the_library() {
        let mem = Arc::new(MemStore::new());
        let fault = Arc::new(FaultStore::new(mem.clone()));
        let dir_a = tempfile::tempdir().unwrap();
        let db = open_with_store(config(&dir_a.path().join("db")), fault).unwrap();
        let mut st = open_stream(&db, Duration::from_millis(100)).unwrap();
        let fence = Fence::new();
        let run = |st: &mut StreamState, sql: &str| {
            execute_request(
                st,
                Version::V3,
                json!({"type":"execute","stmt":{"sql":sql}}),
                &fence,
            )
        };
        assert!(matches!(
            run(&mut st, "CREATE TABLE t(id INTEGER)"),
            StreamResult::Ok { .. }
        ));
        let dir_b = tempfile::tempdir().unwrap();
        takeover_with_store(config(&dir_b.path().join("db")), mem.clone()).unwrap();
        match run(&mut st, "INSERT INTO t VALUES (1)") {
            StreamResult::Error { error } => assert_eq!(error.code.as_deref(), Some("FENCED")),
            other => panic!("{other:?}"),
        }
        assert!(fence.is_set());
        assert!(st.closed);
        match run(&mut st, "SELECT 1") {
            StreamResult::Error { error } => assert_eq!(error.code.as_deref(), Some("FENCED")),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn upload_failure_maps_to_upload_failed_or_timeout() {
        let mem = Arc::new(MemStore::new());
        let fault = Arc::new(FaultStore::new(mem));
        let dir = tempfile::tempdir().unwrap();
        let db = open_with_store(config(&dir.path().join("db")), fault.clone()).unwrap();
        let mut st = open_stream(&db, Duration::from_millis(100)).unwrap();
        let fence = Fence::new();
        let mut run = |sql: &str| {
            execute_request(
                &mut st,
                Version::V3,
                json!({"type":"execute","stmt":{"sql":sql}}),
                &fence,
            )
        };
        assert!(matches!(
            run("CREATE TABLE t(id INTEGER)"),
            StreamResult::Ok { .. }
        ));
        fault.only_keys_containing("log/");
        fault.fail_next_puts_permanently(1000);
        match run("INSERT INTO t VALUES (1)") {
            StreamResult::Error { error } => assert!(
                matches!(error.code.as_deref(), Some("UPLOAD_FAILED" | "TIMEOUT")),
                "{error:?}"
            ),
            other => panic!("{other:?}"),
        }
    }
}
