//! SQL over HTTP v3 JSON types, conversion to and from library values, and error codes.

use base64::alphabet;
use base64::engine::general_purpose::{GeneralPurpose, GeneralPurposeConfig, STANDARD_NO_PAD};
use base64::engine::DecodePaddingMode;
use base64::Engine as _;
use serde::{Deserialize, Serialize, Serializer};
use std::time::Duration;

const LENIENT: GeneralPurpose = GeneralPurpose::new(
    &alphabet::STANDARD,
    GeneralPurposeConfig::new().with_decode_padding_mode(DecodePaddingMode::Indifferent),
);

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct PipelineReq {
    #[serde(default)]
    pub baton: Option<String>,
    /// Each request is parsed alone (`StreamRequest`), so one bad request fails only itself.
    pub requests: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PipelineResp {
    pub baton: Option<String>,
    pub base_url: Option<String>,
    pub results: Vec<StreamResult>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamRequest {
    Close,
    Execute {
        stmt: Stmt,
    },
    Batch {
        batch: Batch,
    },
    Sequence {
        #[serde(default)]
        sql: Option<String>,
        #[serde(default)]
        sql_id: Option<i32>,
    },
    Describe {
        #[serde(default)]
        sql: Option<String>,
        #[serde(default)]
        sql_id: Option<i32>,
    },
    StoreSql {
        sql_id: i32,
        sql: String,
    },
    CloseSql {
        sql_id: i32,
    },
    GetAutocommit,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamResult {
    Ok { response: StreamResponse },
    Error { error: ProtoError },
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamResponse {
    Close,
    Execute { result: StmtResult },
    Batch { result: BatchResult },
    Sequence,
    Describe { result: DescribeResult },
    StoreSql,
    CloseSql,
    GetAutocommit { is_autocommit: bool },
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Stmt {
    #[serde(default)]
    pub sql: Option<String>,
    #[serde(default)]
    pub sql_id: Option<i32>,
    #[serde(default)]
    pub args: Vec<HValue>,
    #[serde(default)]
    pub named_args: Vec<NamedArg>,
    #[serde(default)]
    pub want_rows: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct NamedArg {
    pub name: String,
    pub value: HValue,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StmtResult {
    pub cols: Vec<Col>,
    pub rows: Vec<Vec<HValue>>,
    pub affected_row_count: u64,
    /// A decimal string here (PROTOCOL.md 8.4); the cursor's `step_end` carries the number.
    #[serde(serialize_with = "decimal_or_null")]
    pub last_insert_rowid: Option<i64>,
    pub rows_read: u64,
    pub rows_written: u64,
    pub query_duration_ms: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Col {
    pub name: Option<String>,
    pub decltype: Option<String>,
}

/// A `describe` result (PROTOCOL.md 6.4).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DescribeResult {
    pub params: Vec<DescribeParam>,
    pub cols: Vec<Col>,
    pub is_explain: bool,
    pub is_readonly: bool,
}

/// One bind parameter: `name` is `null` for a positional one.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DescribeParam {
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Batch {
    pub steps: Vec<BatchStep>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct BatchStep {
    #[serde(default)]
    pub condition: Option<BatchCond>,
    pub stmt: Stmt,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BatchCond {
    Ok { step: u32 },
    Error { step: u32 },
    Not { cond: Box<BatchCond> },
    And { conds: Vec<BatchCond> },
    Or { conds: Vec<BatchCond> },
    IsAutocommit,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct BatchResult {
    pub step_results: Vec<Option<StmtResult>>,
    pub step_errors: Vec<Option<ProtoError>>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct CursorReq {
    #[serde(default)]
    pub baton: Option<String>,
    pub batch: Batch,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CursorRespBody {
    pub baton: Option<String>,
    pub base_url: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CursorEntry {
    StepBegin {
        step: u32,
        cols: Vec<Col>,
    },
    StepEnd {
        affected_row_count: u64,
        last_insert_rowid: Option<i64>,
    },
    StepError {
        step: u32,
        error: ProtoError,
    },
    Row {
        row: Vec<HValue>,
    },
    Error {
        error: ProtoError,
    },
    /// The last line of a completed cursor (PROTOCOL.md 7.2.6); the value is always `null`.
    ReplicationIndex {
        replication_index: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProtoError {
    pub message: String,
    pub code: Option<String>,
}

impl ProtoError {
    pub fn new(code: &str, message: impl Into<String>) -> Self {
        ProtoError {
            message: message.into(),
            code: Some(code.to_string()),
        }
    }
}

/// A protocol value (PROTOCOL.md 8.2).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HValue {
    Null,
    Integer {
        value: String,
    },
    Float {
        #[serde(serialize_with = "finite_or_null")]
        value: f64,
    },
    Text {
        value: String,
    },
    Blob {
        base64: String,
    },
}

/// JSON has no NaN or infinity: a non-finite float is written as `null` (PROTOCOL.md 8.2).
fn finite_or_null<S: Serializer>(f: &f64, s: S) -> Result<S::Ok, S::Error> {
    if f.is_finite() {
        s.serialize_f64(*f)
    } else {
        s.serialize_none()
    }
}

fn decimal_or_null<S: Serializer>(id: &Option<i64>, s: S) -> Result<S::Ok, S::Error> {
    match id {
        Some(id) => s.collect_str(id),
        None => s.serialize_none(),
    }
}

pub fn to_value(v: &HValue) -> Result<buckwal::Value, ProtoError> {
    use buckwal::Value;
    Ok(match v {
        HValue::Null => Value::Null,
        HValue::Integer { value } => {
            let digits = value.strip_prefix('-').unwrap_or(value);
            if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
                return Err(ProtoError::new(
                    "PROTO_ERROR",
                    format!("invalid integer {value:?}"),
                ));
            }
            Value::from_i64(value.parse::<i64>().map_err(|e| {
                ProtoError::new("PROTO_ERROR", format!("invalid integer {value:?}: {e}"))
            })?)
        }
        HValue::Float { value } => {
            if value.is_nan() {
                return Err(ProtoError::new("PROTO_ERROR", "float is NaN"));
            }
            Value::from_f64(*value)
        }
        HValue::Text { value } => Value::from_text(value.clone()),
        HValue::Blob { base64 } => {
            let bytes = LENIENT
                .decode(base64)
                .map_err(|e| ProtoError::new("PROTO_ERROR", format!("invalid base64: {e}")))?;
            Value::from_slice(&bytes)
                .map_err(|e| ProtoError::new("INTERNAL_ERROR", format!("blob allocation: {e}")))?
        }
    })
}

/// A library value as a protocol value: a blob as unpadded base64, a non-finite float kept (it
/// is written as `null`).
pub fn from_value(v: &buckwal::Value) -> HValue {
    use buckwal::Value;
    if matches!(v, Value::Null) {
        return HValue::Null;
    }
    if let Some(i) = v.as_int() {
        return HValue::Integer {
            value: i.to_string(),
        };
    }
    if let Some(t) = v.to_text() {
        return HValue::Text {
            value: t.to_string(),
        };
    }
    if let Some(b) = v.to_blob() {
        return HValue::Blob {
            base64: STANDARD_NO_PAD.encode(b),
        };
    }
    HValue::Float {
        value: v.as_float(),
    }
}

/// A statement's result (PROTOCOL.md 8.4). `inserted`: the statement inserted a row, so
/// `last_insert_rowid` is set; `rows_written` is the changed-row count, `rows_read` always 0.
pub fn stmt_result(
    out: buckwal::StmtOutput,
    want_rows: bool,
    inserted: bool,
    elapsed: Duration,
) -> StmtResult {
    let rows = if want_rows {
        out.rows
            .iter()
            .map(|r| r.iter().map(from_value).collect())
            .collect()
    } else {
        Vec::new()
    };
    StmtResult {
        cols: out
            .columns
            .into_iter()
            .map(|c| Col {
                name: Some(c.name),
                decltype: c.decltype,
            })
            .collect(),
        rows,
        affected_row_count: out.changes,
        last_insert_rowid: inserted.then_some(out.last_insert_rowid),
        rows_read: 0,
        rows_written: out.changes,
        query_duration_ms: elapsed.as_secs_f64() * 1000.0,
    }
}

/// A library description as a `describe` result (PROTOCOL.md 6.4).
pub fn describe_result(d: buckwal::Description) -> DescribeResult {
    DescribeResult {
        params: d
            .params
            .into_iter()
            .map(|name| DescribeParam { name })
            .collect(),
        cols: d
            .columns
            .into_iter()
            .map(|c| Col {
                name: Some(c.name),
                decltype: c.decltype,
            })
            .collect(),
        is_explain: d.is_explain,
        is_readonly: d.is_readonly,
    }
}

/// The whole engine messages that are `SQLITE_BUSY`, matched exactly (a table named `busy_jobs`
/// must not be): turso_core 0.8.3's `Busy` and `BusySnapshot` (error.rs:78, 92; both code 5,
/// error.rs:173) and the library's own sleep-then-busy message (conn.rs:363).
const BUSY_MESSAGES: [&str; 3] = [
    "Database is busy",
    "Database snapshot is stale. You must rollback and retry the whole transaction.",
    "database is busy (locked)",
];

pub fn error_code(e: &buckwal::Error) -> &'static str {
    use buckwal::Error as E;
    match e {
        E::Fenced(_) => "FENCED",
        E::Timeout(_) => "TIMEOUT",
        E::UploadFailed(_) => "UPLOAD_FAILED",
        E::Unsupported(_) => "UNSUPPORTED",
        E::Engine(m) => {
            if m.to_ascii_lowercase().contains("constraint failed") {
                "SQLITE_CONSTRAINT"
            } else if BUSY_MESSAGES.contains(&m.as_str()) {
                "SQLITE_BUSY"
            } else {
                "SQLITE_ERROR"
            }
        }
        E::Config(_) => "SQL_INPUT_ERROR",
        E::Bucket(_) => "BUCKET_ERROR",
        E::Io(_) => "IO_ERROR",
        // A stale restored copy was `Corrupt` before it had its own variant: same code.
        E::Corrupt(_) | E::Stale(_) => "CORRUPT",
        E::Encryption(_) => "ENCRYPTION_ERROR",
        E::NotFound(_) | E::Locked(_) | E::AlreadyOpen(_) => "INTERNAL_ERROR",
    }
}

pub fn lib_error(e: &buckwal::Error) -> ProtoError {
    ProtoError::new(error_code(e), e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use buckwal::{Column, Error, StmtOutput, Value};

    // Request shapes copied from libSQL's clients (provenance). SQL over HTTP v3 descends from
    // libSQL's Hrana, and these shapes remain accepted.
    // @libsql/hrana-client 0.9.0 (v2 JSON): execute with args and named_args.
    const HRANA_CLIENT_EXECUTE: &str = r#"{"type":"execute","stmt":{"sql":"SELECT ?, :a","args":[{"type":"integer","value":"42"},{"type":"text","value":"x"}],"named_args":[{"name":"a","value":{"type":"null"}}],"want_rows":true}}"#;

    // @libsql/client 0.15.15 hrana.js: the transaction batch (BEGIN, then steps gated on ok/not/and).
    const LIBSQL_CLIENT_BATCH: &str = r#"{"type":"batch","batch":{"steps":[
        {"stmt":{"sql":"BEGIN","want_rows":false}},
        {"condition":{"type":"ok","step":0},"stmt":{"sql":"INSERT INTO t VALUES (1)","args":[],"want_rows":false}},
        {"condition":{"type":"and","conds":[{"type":"ok","step":1},{"type":"not","cond":{"type":"error","step":0}}]},"stmt":{"sql":"COMMIT","want_rows":false}},
        {"condition":{"type":"or","conds":[{"type":"is_autocommit"}]},"stmt":{"sql":"SELECT 1"}}]}}"#;

    // libsql Rust crate 0.10.0 (v3): batch, with "replication_index": null inside the stmt.
    const LIBSQL_RUST_BATCH: &str = r#"{"type":"batch","batch":{"steps":[{"stmt":{"sql":"SELECT 1","args":[],"named_args":[],"want_rows":true,"replication_index":null}}],"replication_index":null}}"#;
    // libsql Rust crate 0.10.0 (v3): get_autocommit.
    const LIBSQL_RUST_AUTOCOMMIT: &str = r#"{"type":"get_autocommit"}"#;
    // @libsql/hrana-client 0.9.0: sequence by sql_id.
    const HRANA_CLIENT_SEQUENCE: &str = r#"{"type":"sequence","sql_id":3}"#;
    // @libsql/hrana-client 0.9.0: store_sql.
    const HRANA_CLIENT_STORE_SQL: &str = r#"{"type":"store_sql","sql_id":3,"sql":"SELECT 1"}"#;
    // @libsql/hrana-client 0.9.0: close_sql.
    const HRANA_CLIENT_CLOSE_SQL: &str = r#"{"type":"close_sql","sql_id":3}"#;
    // @libsql/hrana-client 0.9.0: close.
    const HRANA_CLIENT_CLOSE: &str = r#"{"type":"close"}"#;

    fn parse(s: &str) -> StreamRequest {
        serde_json::from_str(s).unwrap_or_else(|e| panic!("{s}: {e}"))
    }

    fn sql_stmt(sql: &str, want_rows: Option<bool>) -> Stmt {
        Stmt {
            sql: Some(sql.into()),
            sql_id: None,
            args: vec![],
            named_args: vec![],
            want_rows,
        }
    }

    #[test]
    fn parses_client_requests() {
        match parse(HRANA_CLIENT_EXECUTE) {
            StreamRequest::Execute { stmt } => {
                assert_eq!(stmt.sql.as_deref(), Some("SELECT ?, :a"));
                assert_eq!(
                    stmt.args,
                    vec![
                        HValue::Integer { value: "42".into() },
                        HValue::Text { value: "x".into() }
                    ]
                );
                assert_eq!(
                    stmt.named_args,
                    vec![NamedArg {
                        name: "a".into(),
                        value: HValue::Null
                    }]
                );
                assert_eq!(stmt.want_rows, Some(true));
            }
            other => panic!("{other:?}"),
        }

        match parse(LIBSQL_CLIENT_BATCH) {
            StreamRequest::Batch { batch } => {
                assert_eq!(batch.steps.len(), 4);
                assert_eq!(batch.steps[0].condition, None);
                assert_eq!(batch.steps[0].stmt, sql_stmt("BEGIN", Some(false)));
                assert_eq!(batch.steps[1].condition, Some(BatchCond::Ok { step: 0 }));
                assert_eq!(
                    batch.steps[2].condition,
                    Some(BatchCond::And {
                        conds: vec![
                            BatchCond::Ok { step: 1 },
                            BatchCond::Not {
                                cond: Box::new(BatchCond::Error { step: 0 })
                            }
                        ]
                    })
                );
                assert_eq!(
                    batch.steps[3].condition,
                    Some(BatchCond::Or {
                        conds: vec![BatchCond::IsAutocommit]
                    })
                );
            }
            other => panic!("{other:?}"),
        }

        assert_eq!(
            parse(LIBSQL_RUST_BATCH),
            StreamRequest::Batch {
                batch: Batch {
                    steps: vec![BatchStep {
                        condition: None,
                        stmt: sql_stmt("SELECT 1", Some(true))
                    }]
                }
            }
        );
        assert_eq!(parse(LIBSQL_RUST_AUTOCOMMIT), StreamRequest::GetAutocommit);
        assert_eq!(
            parse(HRANA_CLIENT_SEQUENCE),
            StreamRequest::Sequence {
                sql: None,
                sql_id: Some(3)
            }
        );
        assert_eq!(
            parse(HRANA_CLIENT_STORE_SQL),
            StreamRequest::StoreSql {
                sql_id: 3,
                sql: "SELECT 1".into()
            }
        );
        assert_eq!(
            parse(HRANA_CLIENT_CLOSE_SQL),
            StreamRequest::CloseSql { sql_id: 3 }
        );
        assert_eq!(parse(HRANA_CLIENT_CLOSE), StreamRequest::Close);
    }

    #[test]
    fn pipeline_req_parses_with_extra_fields() {
        let req: PipelineReq = serde_json::from_str(
            r#"{"baton":null,"requests":[{"type":"close"},{"type":"open_cursor"}],"extra":1}"#,
        )
        .unwrap();
        assert_eq!(req.baton, None);
        assert_eq!(req.requests.len(), 2);
    }

    #[test]
    fn unknown_request_type_fails_alone() {
        let req: PipelineReq = serde_json::from_str(
            r#"{"requests":[{"type":"close"},{"type":"open_cursor"},{"type":"get_autocommit"}]}"#,
        )
        .unwrap();
        let parsed: Vec<Result<StreamRequest, _>> = req
            .requests
            .into_iter()
            .map(serde_json::from_value::<StreamRequest>)
            .collect();
        assert_eq!(parsed[0].as_ref().unwrap(), &StreamRequest::Close);
        assert!(parsed[1].is_err());
        assert_eq!(parsed[2].as_ref().unwrap(), &StreamRequest::GetAutocommit);
    }

    fn round_trip(json: &str) -> (Value, HValue) {
        let h: HValue = serde_json::from_str(json).unwrap();
        let v = to_value(&h).unwrap();
        let back = from_value(&v);
        (v, back)
    }

    #[test]
    fn values_round_trip_exactly() {
        for s in ["9223372036854775807", "-9223372036854775808"] {
            let json = format!(r#"{{"type":"integer","value":"{s}"}}"#);
            let (v, back) = round_trip(&json);
            assert_eq!(v.as_int(), Some(s.parse().unwrap()));
            assert_eq!(back, HValue::Integer { value: s.into() });
        }

        let (v, back) = round_trip(r#"{"type":"float","value":0.1}"#);
        assert_eq!(v.as_float().to_bits(), 0.1f64.to_bits());
        match back {
            HValue::Float { value } => assert_eq!(value.to_bits(), 0.1f64.to_bits()),
            other => panic!("{other:?}"),
        }

        let (v, back) = round_trip(r#"{"type":"text","value":"a\u0000é"}"#);
        assert_eq!(v.to_text(), Some("a\u{0}é"));
        assert_eq!(
            back,
            HValue::Text {
                value: "a\u{0}é".into()
            }
        );

        for b in ["AAEC/w==", "AAEC/w"] {
            let json = format!(r#"{{"type":"blob","base64":"{b}"}}"#);
            let (v, back) = round_trip(&json);
            assert_eq!(v.to_blob(), Some(&[0u8, 1, 2, 255][..]));
            // Output is unpadded (PROTOCOL.md 8.2).
            assert_eq!(
                back,
                HValue::Blob {
                    base64: "AAEC/w".into()
                }
            );
        }

        let (v, back) = round_trip(r#"{"type":"null"}"#);
        assert!(matches!(v, Value::Null));
        assert_eq!(back, HValue::Null);
    }

    #[test]
    fn bad_values_are_proto_errors() {
        for json in [
            r#"{"type":"integer","value":"1.5"}"#,
            r#"{"type":"integer","value":"99999999999999999999"}"#,
            r#"{"type":"blob","base64":"!!"}"#,
            r#"{"type":"integer","value":"+5"}"#,
            r#"{"type":"integer","value":" 5"}"#,
            r#"{"type":"integer","value":"5 "}"#,
            r#"{"type":"integer","value":""}"#,
            r#"{"type":"integer","value":"-"}"#,
        ] {
            let h: HValue = serde_json::from_str(json).unwrap();
            let e = to_value(&h).unwrap_err();
            assert_eq!(e.code.as_deref(), Some("PROTO_ERROR"), "{json}");
        }
    }

    #[test]
    fn json_integer_float_becomes_float() {
        let h: HValue = serde_json::from_str(r#"{"type":"float","value":1}"#).unwrap();
        let v = to_value(&h).unwrap();
        assert_eq!(v.as_int(), None);
        assert_eq!(v.as_float().to_bits(), 1.0f64.to_bits());
        assert_eq!(from_value(&v), HValue::Float { value: 1.0 });
    }

    #[test]
    fn batch_result_skipped_steps_are_null() {
        let r = BatchResult {
            step_results: vec![None],
            step_errors: vec![None],
        };
        assert_eq!(
            serde_json::to_value(&r).unwrap(),
            serde_json::json!({"step_results":[null],"step_errors":[null]})
        );
    }

    #[test]
    fn non_finite_float_is_null() {
        for f in [f64::INFINITY, f64::NEG_INFINITY] {
            let h = from_value(&Value::from_f64(f));
            assert!(matches!(h, HValue::Float { value } if value == f), "{h:?}");
        }
        // The engine stores NaN as NULL, so only infinities reach `from_value`.
        for f in [f64::INFINITY, f64::NEG_INFINITY, f64::NAN] {
            let h = HValue::Float { value: f };
            assert_eq!(
                serde_json::to_value(&h).unwrap(),
                serde_json::json!({"type":"float","value":null}),
                "{f}"
            );
            assert_eq!(
                serde_json::to_string(&h).unwrap(),
                r#"{"type":"float","value":null}"#,
                "{f}"
            );
        }
    }

    #[test]
    fn blob_output_is_unpadded_standard_base64() {
        for (bytes, b64) in [
            (&[0xde, 0xad, 0xbe, 0xef][..], "3q2+7w"),
            (&[0xfb, 0xff][..], "+/8"),
            (&[1, 2, 3][..], "AQID"),
            (&[][..], ""),
        ] {
            let v = Value::from_slice(bytes).unwrap();
            assert_eq!(from_value(&v), HValue::Blob { base64: b64.into() });
        }
    }

    fn output() -> StmtOutput {
        StmtOutput {
            columns: vec![Column {
                name: "id".into(),
                decltype: Some("INTEGER".into()),
            }],
            rows: vec![vec![Value::from_i64(1)], vec![Value::from_i64(2)]],
            changes: 3,
            last_insert_rowid: 7,
        }
    }

    #[test]
    fn stmt_result_shape() {
        let r = stmt_result(output(), true, true, Duration::from_millis(2));
        let j = serde_json::to_value(&r).unwrap();
        assert_eq!(j["cols"][0]["name"], "id");
        assert_eq!(j["cols"][0]["decltype"], "INTEGER");
        assert_eq!(r.last_insert_rowid, Some(7));
        assert_eq!(j["last_insert_rowid"], "7");
        assert_eq!(j["affected_row_count"], 3);
        assert_eq!(j["rows_read"], 0);
        assert_eq!(j["rows_written"], 3);
        assert_eq!(j["query_duration_ms"], 2.0);
        assert_eq!(j["rows"][1][0]["value"], "2");

        let r = stmt_result(output(), false, false, Duration::from_millis(2));
        let j = serde_json::to_value(&r).unwrap();
        assert_eq!(j["rows"], serde_json::json!([]));
        assert_eq!(j["cols"][0]["name"], "id");
        assert_eq!(j["last_insert_rowid"], serde_json::Value::Null);
        assert_eq!(j["rows_written"], 3);

        // The decimal string is exact at the edges of i64.
        for id in [i64::MAX, i64::MIN] {
            let out = StmtOutput {
                last_insert_rowid: id,
                ..output()
            };
            let r = stmt_result(out, false, true, Duration::ZERO);
            let j = serde_json::to_value(&r).unwrap();
            assert_eq!(j["last_insert_rowid"], id.to_string());
        }
    }

    #[test]
    fn error_codes() {
        let engine = |m: &str| Error::Engine(m.to_string());
        let cases: Vec<(Error, &str)> = vec![
            (Error::Fenced("x".into()), "FENCED"),
            (Error::Timeout("x".into()), "TIMEOUT"),
            (Error::UploadFailed("x".into()), "UPLOAD_FAILED"),
            (Error::Unsupported("vacuum"), "UNSUPPORTED"),
            (
                engine("UNIQUE constraint failed: t.id"),
                "SQLITE_CONSTRAINT",
            ),
            (engine("Database is busy"), "SQLITE_BUSY"),
            (engine("database is busy (locked)"), "SQLITE_BUSY"),
            (
                engine("Database snapshot is stale. You must rollback and retry the whole transaction."),
                "SQLITE_BUSY",
            ),
            (engine("no such table: t"), "SQLITE_ERROR"),
            (engine("no such table: busy_jobs"), "SQLITE_ERROR"),
            (
                engine("trigger: the snapshot is stale, retry"),
                "SQLITE_ERROR",
            ),
            (
                engine("Runtime error: Database snapshot is stale. You must rollback and retry the whole transaction. (19)"),
                "SQLITE_ERROR",
            ),
            (Error::Config("x".into()), "SQL_INPUT_ERROR"),
            (Error::Bucket("x".into()), "BUCKET_ERROR"),
            (Error::Io("x".into()), "IO_ERROR"),
            (Error::Corrupt("x".into()), "CORRUPT"),
            (Error::Encryption("x".into()), "ENCRYPTION_ERROR"),
            (Error::NotFound("x".into()), "INTERNAL_ERROR"),
            (Error::Locked("x".into()), "INTERNAL_ERROR"),
            (Error::AlreadyOpen("x".into()), "INTERNAL_ERROR"),
        ];
        for (e, code) in cases {
            assert_eq!(error_code(&e), code, "{e}");
            let p = lib_error(&e);
            assert_eq!(p.code.as_deref(), Some(code));
            assert_eq!(p.message, e.to_string());
        }
    }

    #[test]
    fn cursor_entries_serialize() {
        let j = |e: CursorEntry| serde_json::to_value(&e).unwrap();
        assert_eq!(
            j(CursorEntry::StepBegin {
                step: 0,
                cols: vec![Col {
                    name: Some("a".into()),
                    decltype: None
                }]
            }),
            serde_json::json!({"type":"step_begin","step":0,"cols":[{"name":"a","decltype":null}]})
        );
        assert_eq!(
            j(CursorEntry::Row {
                row: vec![HValue::Integer { value: "1".into() }]
            }),
            serde_json::json!({"type":"row","row":[{"type":"integer","value":"1"}]})
        );
        assert_eq!(
            j(CursorEntry::StepEnd {
                affected_row_count: 1,
                last_insert_rowid: Some(5)
            }),
            serde_json::json!({"type":"step_end","affected_row_count":1,"last_insert_rowid":5})
        );
        assert_eq!(
            j(CursorEntry::StepEnd {
                affected_row_count: 0,
                last_insert_rowid: None
            }),
            serde_json::json!({"type":"step_end","affected_row_count":0,"last_insert_rowid":null})
        );
        assert_eq!(
            j(CursorEntry::StepError {
                step: 1,
                error: ProtoError::new("SQLITE_ERROR", "boom")
            }),
            serde_json::json!({"type":"step_error","step":1,"error":{"message":"boom","code":"SQLITE_ERROR"}})
        );
        assert_eq!(
            j(CursorEntry::Error {
                error: ProtoError::new("PROTO_ERROR", "bad")
            }),
            serde_json::json!({"type":"error","error":{"message":"bad","code":"PROTO_ERROR"}})
        );
    }

    #[test]
    fn pipeline_response_shape() {
        let resp = PipelineResp {
            baton: None,
            base_url: None,
            results: vec![
                StreamResult::Ok {
                    response: StreamResponse::GetAutocommit {
                        is_autocommit: true,
                    },
                },
                StreamResult::Error {
                    error: ProtoError::new("STREAM_CLOSED", "closed"),
                },
            ],
        };
        assert_eq!(
            serde_json::to_value(&resp).unwrap(),
            serde_json::json!({"baton":null,"base_url":null,"results":[
                {"type":"ok","response":{"type":"get_autocommit","is_autocommit":true}},
                {"type":"error","error":{"message":"closed","code":"STREAM_CLOSED"}}]})
        );
    }
}
