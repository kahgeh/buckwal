//! Hrana over HTTP against a server on `127.0.0.1:0` over a `MemStore`, through the test
//! client (`turso_durable_hrana_client`), which builds requests in the real clients' shapes.
mod common;

use common::{db_config, restored, start, tls_material, Opts, TOKEN};
use serde_json::{json, Value};
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};
use turso_durable::takeover_with_store;
use turso_durable_hrana_client::{
    batch, close, cond_ok, describe, execute, execute_named, get_autocommit, result_error,
    result_rows, stmt, tx_batch_request, Arg, Client, Proto, Reply,
};
use turso_durable_server::{bind, ServeConfig, ShutdownReport, TlsFiles};

fn ok_pipeline(r: &Reply) {
    assert_eq!(r.status, 200, "{}", r.body);
    for (i, res) in r.body["results"].as_array().unwrap().iter().enumerate() {
        assert_eq!(res["type"], "ok", "result {i}: {}", r.body);
    }
}

fn code(r: &Reply) -> &str {
    r.body["code"]
        .as_str()
        .unwrap_or_else(|| panic!("no code: {}", r.body))
}

fn count(c: &turso_durable_hrana_client::Client, table: &str) -> i64 {
    let mut st = c.stream(Proto::V2);
    let r = st
        .pipeline(vec![
            execute(&format!("SELECT count(*) FROM {table}"), &[]),
            close(),
        ])
        .unwrap();
    ok_pipeline(&r);
    result_rows(&r, 0).unwrap()[0][0].as_i64().unwrap()
}

const CREATE: &str = "CREATE TABLE t(id INTEGER PRIMARY KEY, body TEXT)";

#[test]
fn version_probes() {
    let s = start(Opts::default());
    let c = s.client(None);
    assert_eq!(c.get("/v2").unwrap().status, 200);
    let v3 = c.get("/v3").unwrap();
    assert_eq!(v3.status, 200);
    assert_eq!(v3.body, json!({}));
    assert_eq!(c.get("/v3-protobuf").unwrap().status, 404);
    let nope = c.get("/nope").unwrap();
    assert_eq!(nope.status, 404);
    assert_eq!(code(&nope), "NOT_FOUND");
    // A known path with the wrong method is also 404.
    assert_eq!(c.get("/v2/pipeline").unwrap().status, 404);
    assert_eq!(c.post_raw("/v3", b"{}".to_vec(), None).unwrap().status, 404);
    let h = c.get("/health").unwrap();
    assert_eq!(h.status, 200);
    assert_eq!(h.body, json!({"status":"ready"}));
    let report = s.stop().unwrap();
    assert!(report.clean, "{report:?}");
    assert!(!report.fenced, "{report:?}");
}

#[test]
fn v2_execute_and_stream_continuity() {
    let s = start(Opts::default());
    let c = s.client(None);
    let mut st = c.stream(Proto::V2);
    ok_pipeline(&st.pipeline(vec![execute(CREATE, &[])]).unwrap());
    let b1 = st.baton().expect("open stream has a baton").to_string();
    let r = st
        .pipeline(vec![
            execute(
                "INSERT INTO t VALUES (?, ?)",
                &[Arg::Int(1), Arg::Text("a".into())],
            ),
            execute_named(
                "INSERT INTO t VALUES (:id, :body)",
                &[("id", Arg::Int(2)), ("body", Arg::Text("b".into()))],
            ),
        ])
        .unwrap();
    ok_pipeline(&r);
    let b2 = st.baton().unwrap().to_string();
    assert_ne!(b1, b2, "the baton rotates");
    let r = st
        .pipeline(vec![execute("SELECT id, body FROM t ORDER BY id", &[])])
        .unwrap();
    ok_pipeline(&r);
    let cols: Vec<&str> = r.body["results"][0]["response"]["result"]["cols"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    assert_eq!(cols, ["id", "body"]);
    assert_eq!(
        result_rows(&r, 0).unwrap(),
        vec![vec![json!(1), json!("a")], vec![json!(2), json!("b")]]
    );
    assert_ne!(st.baton().unwrap(), b2);
    let r = st.pipeline(vec![close()]).unwrap();
    ok_pipeline(&r);
    assert_eq!(r.body["baton"], Value::Null, "close ends the stream");
    s.stop().unwrap();
}

/// The libsql Rust crate 0.10.0 sends an `execute` as a one-step batch, then `get_autocommit`.
fn libsql_rust_execute(sql: &str, args: &[Arg]) -> Value {
    let mut st = stmt(sql, args);
    st["named_args"] = json!([]);
    st["replication_index"] = Value::Null;
    let mut b = batch(vec![(None, st)]);
    b["batch"]["replication_index"] = Value::Null;
    b
}

#[test]
fn v3_batch_get_autocommit_and_cursor() {
    let s = start(Opts::default());
    let c = s.client(None);
    let mut st = c.stream(Proto::V3);
    for sql in [CREATE, "INSERT INTO t VALUES (1, 'a'), (2, 'b')"] {
        let r = st
            .pipeline(vec![libsql_rust_execute(sql, &[]), get_autocommit()])
            .unwrap();
        ok_pipeline(&r);
        assert_eq!(
            r.body["results"][0]["response"]["result"]["step_errors"],
            json!([null])
        );
        assert_eq!(
            r.body["results"][1]["response"]["is_autocommit"],
            json!(true),
            "{}",
            r.body
        );
    }
    let before = st.baton().unwrap().to_string();
    let q = libsql_rust_execute("SELECT id, body FROM t ORDER BY id", &[]);
    let (status, entries) = st.cursor(q["batch"].clone()).unwrap();
    assert_eq!(status, 200, "{entries:?}");
    let kinds: Vec<&str> = entries
        .iter()
        .map(|e| e["type"].as_str().unwrap())
        .collect();
    assert_eq!(
        kinds,
        ["step_begin", "row", "row", "step_end", "replication_index"],
        "{entries:?}"
    );
    let cols: Vec<&str> = entries[0]["cols"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    assert_eq!(cols, ["id", "body"]);
    assert_eq!(entries[1]["row"][1], json!({"type":"text","value":"a"}));
    assert_eq!(entries[2]["row"][0], json!({"type":"integer","value":"2"}));
    let after = st.baton().expect("cursor keeps the stream").to_string();
    assert_ne!(before, after);
    s.stop().unwrap();
}

const TERMINATOR: &str = r#"{"type":"replication_index","replication_index":null}"#;

/// The body of a completed cursor ends with the terminator line (PROTOCOL.md 7.2.6), even
/// when a step failed or the batch is empty. A refused cursor has no NDJSON body at all.
#[test]
fn cursor_body_ends_with_the_terminator() {
    let s = start(Opts::default());
    let c = s.client(None);
    let raw = |batch: Value| {
        let body = json!({"baton": null, "batch": batch}).to_string();
        let r = reqwest::blocking::Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .post(format!("{}/v3/cursor", s.url()))
            .body(body)
            .send()
            .unwrap();
        (r.status().as_u16(), r.text().unwrap())
    };
    for steps in [
        json!([{"stmt": stmt("SELECT 1", &[])}]),
        json!([{"stmt": stmt("SELECT * FROM nope", &[])}, {"stmt": stmt("SELECT 2", &[])}]),
        json!([]),
    ] {
        let (status, body) = raw(json!({ "steps": steps }));
        assert_eq!(status, 200, "{body}");
        assert!(body.ends_with('\n'), "{body:?}");
        let last = body.lines().last().unwrap();
        assert_eq!(last, TERMINATOR, "{steps}: {body}");
        assert_eq!(
            body.lines().filter(|l| *l == TERMINATOR).count(),
            1,
            "{body}"
        );
    }
    let mut st = c.stream(Proto::V3);
    st.set_baton(Some("1.bogus".into()));
    let (status, entries) = st
        .cursor(batch(vec![(None, stmt("SELECT 1", &[]))])["batch"].clone())
        .unwrap();
    assert_eq!(status, 400, "{entries:?}");
    assert!(
        !format!("{entries:?}").contains("replication_index"),
        "{entries:?}"
    );
    s.stop().unwrap();
}

/// A condition on its own or a later step, at any depth, fails the whole cursor request
/// with 400 before the baton line (PROTOCOL.md 6.2.1, 5.3): no NDJSON body, nothing runs,
/// and the stream the baton named is closed (its transaction rolled back).
#[test]
fn cursor_forward_condition_is_400_before_the_body() {
    let s = start(Opts::default());
    let c = s.client(None);
    let mut setup = c.stream(Proto::V2);
    ok_pipeline(&setup.pipeline(vec![execute(CREATE, &[]), close()]).unwrap());
    let insert = stmt("INSERT INTO t(body) VALUES ('no')", &[]);
    let nested =
        json!({"type":"and","conds":[cond_ok(0),{"type":"not","cond":{"type":"error","step":2}}]});
    for cond in [cond_ok(1), nested] {
        let mut st = c.stream(Proto::V3);
        let b = batch(vec![
            (None, insert.clone()),
            (Some(cond), stmt("SELECT 1", &[])),
        ]);
        let (status, entries) = st.cursor(b["batch"].clone()).unwrap();
        assert_eq!(status, 400, "{entries:?}");
        assert_eq!(entries[0]["code"], "PROTO_ERROR", "{entries:?}");
        assert!(entries[0].get("baton").is_none(), "{entries:?}");
    }
    let mut st = c.stream(Proto::V3);
    ok_pipeline(
        &st.pipeline(vec![
            execute("BEGIN", &[]),
            execute("INSERT INTO t(body) VALUES ('txn')", &[]),
        ])
        .unwrap(),
    );
    let baton = st.baton().unwrap().to_string();
    let b = batch(vec![(Some(cond_ok(0)), insert.clone())]);
    let (status, _) = st.cursor(b["batch"].clone()).unwrap();
    assert_eq!(status, 400);
    st.set_baton(Some(baton));
    let r = st.pipeline(vec![execute("SELECT 1", &[])]).unwrap();
    assert_eq!(r.status, 400, "{}", r.body);
    assert_eq!(code(&r), "STREAM_EXPIRED", "the stream was closed");
    assert_eq!(
        count(&c, "t"),
        0,
        "nothing ran and the transaction rolled back"
    );
    s.stop().unwrap();
}

/// A `close` that is not the last request fails the whole pipeline with 400 before anything
/// in it runs (PROTOCOL.md 5.3, 6.8). A null baton creates no stream; a baton's stream is
/// closed, its open transaction rolled back.
#[test]
fn close_not_last_fails_the_whole_pipeline() {
    let s = start(Opts::default());
    let c = s.client(None);
    let mut setup = c.stream(Proto::V2);
    ok_pipeline(&setup.pipeline(vec![execute(CREATE, &[]), close()]).unwrap());
    let insert = || execute("INSERT INTO t(body) VALUES ('no')", &[]);
    for proto in [Proto::V2, Proto::V3] {
        for reqs in [
            vec![close(), insert()],
            vec![insert(), close(), execute("SELECT 1", &[])],
            vec![close(), close()],
        ] {
            let r = c.stream(proto).pipeline(reqs).unwrap();
            assert_eq!(r.status, 400, "{proto:?}: {}", r.body);
            assert_eq!(code(&r), "PROTO_ERROR");
            assert!(r.body.get("results").is_none(), "{}", r.body);
        }
    }
    ok_pipeline(&c.stream(Proto::V3).pipeline(vec![close()]).unwrap());
    let mut st = c.stream(Proto::V3);
    ok_pipeline(&st.pipeline(vec![execute("BEGIN", &[]), insert()]).unwrap());
    let baton = st.baton().unwrap().to_string();
    let r = st.pipeline(vec![insert(), close(), insert()]).unwrap();
    assert_eq!(r.status, 400, "{}", r.body);
    st.set_baton(Some(baton));
    let r = st.pipeline(vec![execute("SELECT 1", &[])]).unwrap();
    assert_eq!(
        code(&r),
        "STREAM_EXPIRED",
        "the stream was closed: {}",
        r.body
    );
    assert_eq!(
        count(&c, "t"),
        0,
        "nothing ran and the transaction rolled back"
    );
    let report = s.stop().unwrap();
    assert_eq!(
        report.streams_closed, 0,
        "no stream was left open: {report:?}"
    );
}

/// A batch condition on its own or a later step, at any depth, fails the whole pipeline with
/// 400; the requests before the batch do not run (PROTOCOL.md 6.2.1, 5.3).
#[test]
fn pipeline_forward_condition_fails_the_whole_pipeline() {
    let s = start(Opts::default());
    let c = s.client(None);
    let mut setup = c.stream(Proto::V2);
    ok_pipeline(&setup.pipeline(vec![execute(CREATE, &[]), close()]).unwrap());
    let insert = || execute("INSERT INTO t(body) VALUES ('no')", &[]);
    let deep = json!({"type":"or","conds":[{"type":"not","cond":{"type":"and","conds":[cond_ok(0),{"type":"error","step":3}]}}]});
    for proto in [Proto::V2, Proto::V3] {
        for cond in [cond_ok(1), json!({"type":"error","step":1}), deep.clone()] {
            let b = batch(vec![
                (None, stmt("SELECT 1", &[])),
                (Some(cond.clone()), stmt("SELECT 2", &[])),
            ]);
            let r = c.stream(proto).pipeline(vec![insert(), b]).unwrap();
            assert_eq!(r.status, 400, "{proto:?} {cond}: {}", r.body);
            assert_eq!(code(&r), "PROTO_ERROR");
        }
    }
    assert_eq!(count(&c, "t"), 0, "the insert before the batch did not run");
    s.stop().unwrap();
}

/// The cursor's `error` condition is the negation of `ok`: true for a skipped step too
/// (PROTOCOL.md 6.2.1).
#[test]
fn cursor_error_condition_is_true_for_a_skipped_step() {
    let s = start(Opts::default());
    let c = s.client(None);
    let mut st = c.stream(Proto::V3);
    let error = |n: usize| json!({"type":"error","step":n});
    let b = batch(vec![
        (None, stmt("SELECT * FROM nope", &[])),
        (Some(cond_ok(0)), stmt("SELECT 1", &[])),
        (Some(error(1)), stmt("SELECT 2", &[])),
        (
            Some(json!({"type":"not","cond":error(1)})),
            stmt("SELECT 3", &[]),
        ),
    ]);
    let (status, entries) = st.cursor(b["batch"].clone()).unwrap();
    assert_eq!(status, 200, "{entries:?}");
    let begun: Vec<i64> = entries
        .iter()
        .filter(|e| e["type"] == "step_begin")
        .map(|e| e["step"].as_i64().unwrap())
        .collect();
    assert_eq!(begun, [2], "{entries:?}");
    s.stop().unwrap();
}

/// A non-finite float is `{"type":"float","value":null}` and a blob is unpadded base64
/// (PROTOCOL.md 8.2), in pipeline results and in cursor rows. Padded input is still read.
#[test]
fn non_finite_floats_are_null_and_blobs_unpadded() {
    let s = start(Opts::default());
    let c = s.client(None);
    let mut st = c.stream(Proto::V3);
    let sql = "SELECT 1e999, -1e999, ?, ?";
    let padded = json!({"type":"blob","base64":"3q2+7w=="});
    let unpadded = json!({"type":"blob","base64":"3q2+7w"});
    let mut q = stmt(sql, &[]);
    q["args"] = json!([padded, unpadded]);
    let want = json!([
        {"type":"float","value":null},
        {"type":"float","value":null},
        {"type":"blob","base64":"3q2+7w"},
        {"type":"blob","base64":"3q2+7w"}
    ]);
    let r = st
        .pipeline(vec![json!({"type":"execute","stmt":q.clone()})])
        .unwrap();
    ok_pipeline(&r);
    assert_eq!(r.body["results"][0]["response"]["result"]["rows"][0], want);
    let (status, entries) = st.cursor(json!({"steps":[{"stmt":q}]})).unwrap();
    assert_eq!(status, 200, "{entries:?}");
    assert_eq!(entries[1]["type"], "row", "{entries:?}");
    assert_eq!(entries[1]["row"], want);
    s.stop().unwrap();
}

#[test]
fn libsql_client_transaction_batch_commits_once() {
    let s = start(Opts::default());
    let c = s.client(None);
    let mut st = c.stream(Proto::V3);
    ok_pipeline(&st.pipeline(vec![execute(CREATE, &[])]).unwrap());
    let logs = s.log_keys();
    let inserts: Vec<Value> = (1..=3)
        .map(|i| stmt("INSERT INTO t VALUES (?, 'x')", &[Arg::Int(i)]))
        .collect();
    let r = st
        .pipeline(vec![tx_batch_request(&inserts), get_autocommit()])
        .unwrap();
    ok_pipeline(&r);
    let result = &r.body["results"][0]["response"]["result"];
    // BEGIN, 3 inserts, COMMIT run; ROLLBACK is skipped.
    assert_eq!(
        result["step_errors"],
        json!([null, null, null, null, null, null])
    );
    let ran: Vec<bool> = result["step_results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| !x.is_null())
        .collect();
    assert_eq!(ran, [true, true, true, true, true, false]);
    assert_eq!(
        r.body["results"][1]["response"]["is_autocommit"],
        json!(true)
    );
    assert_eq!(s.log_keys(), logs + 1, "the transaction is one commit");
    assert_eq!(count(&c, "t"), 3);
    s.stop().unwrap();
}

#[test]
fn replayed_baton_is_refused_and_does_not_run() {
    let s = start(Opts::default());
    let c = s.client(None);
    let mut st = c.stream(Proto::V2);
    ok_pipeline(&st.pipeline(vec![execute(CREATE, &[])]).unwrap());
    let insert = || execute("INSERT INTO t(body) VALUES ('once')", &[]);
    let b1 = st.baton().unwrap().to_string();
    ok_pipeline(&st.pipeline(vec![insert()]).unwrap());
    let b2 = st.baton().unwrap().to_string();
    assert_eq!(count(&c, "t"), 1);
    st.set_baton(Some(b1));
    let r = st.pipeline(vec![insert()]).unwrap();
    assert_eq!(r.status, 400, "{}", r.body);
    assert_eq!(code(&r), "STREAM_EXPIRED");
    assert_eq!(count(&c, "t"), 1, "the replayed insert must not run");
    st.set_baton(Some(b2));
    ok_pipeline(&st.pipeline(vec![insert()]).unwrap());
    assert_eq!(count(&c, "t"), 2);
    s.stop().unwrap();
}

/// The body limit is checked as the body is read, so the 413 comes once the server has read
/// one byte past 16 MiB. A client still sending then may see its connection reset instead
/// (the server closes with the rest unread), so this sends exactly one byte over the limit
/// on a raw connection: every byte sent is read, and the 413 always arrives.
#[test]
fn oversized_body_is_413() {
    use std::io::Read;
    let s = start(Opts::default());
    let c = s.client(None);
    let len = (16 << 20) + 1;
    let mut tcp = std::net::TcpStream::connect(s.addr).unwrap();
    tcp.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
    write!(
        tcp,
        "POST /v2/pipeline HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\
         Content-Type: application/json\r\nContent-Length: {len}\r\n\r\n"
    )
    .unwrap();
    tcp.write_all(&vec![b' '; len]).unwrap();
    let mut reply = Vec::new();
    // `Connection: close`: the server closes the connection after the reply.
    tcp.read_to_end(&mut reply).unwrap();
    let reply = String::from_utf8_lossy(&reply);
    assert!(reply.starts_with("HTTP/1.1 413 "), "{reply}");
    assert!(reply.contains(r#""code":"PAYLOAD_TOO_LARGE""#), "{reply}");
    let h = c.get("/health").unwrap();
    assert_eq!(h.status, 200, "the server stays up");
    // A body just under the limit is read and parsed (and is malformed here).
    let r = c
        .post_raw("/v2/pipeline", vec![b' '; (16 << 20) - 1], None)
        .unwrap();
    assert_eq!(r.status, 400, "{}", r.body);
    s.stop().unwrap();
}

#[test]
fn malformed_body_is_400_proto_error() {
    let s = start(Opts::default());
    let c = s.client(None);
    for (path, body) in [
        ("/v2/pipeline", &b"{not json"[..]),
        ("/v3/pipeline", br#"{"requests": 5}"#),
        ("/v3/pipeline", b""),
        ("/v3/cursor", br#"{"baton": null}"#),
    ] {
        let r = c.post_raw(path, body.to_vec(), None).unwrap();
        assert_eq!(r.status, 400, "{path} {body:?}: {}", r.body);
        assert_eq!(code(&r), "PROTO_ERROR", "{path}");
        assert!(r.body["message"].is_string(), "{}", r.body);
    }
    s.stop().unwrap();
}

#[test]
fn unknown_request_type_errors_alone() {
    let s = start(Opts::default());
    let c = s.client(None);
    let mut st = c.stream(Proto::V2);
    let r = st
        .pipeline(vec![
            execute("SELECT 1", &[]),
            json!({"type":"frobnicate"}),
            execute("SELECT 2", &[]),
        ])
        .unwrap();
    assert_eq!(r.status, 200, "{}", r.body);
    assert_eq!(result_rows(&r, 0).unwrap(), vec![vec![json!(1)]]);
    let (_, code) = result_error(&r, 1).expect("request 1 fails");
    assert_eq!(code.as_deref(), Some("UNSUPPORTED"));
    assert_eq!(result_rows(&r, 2).unwrap(), vec![vec![json!(2)]]);
    assert!(st.baton().is_some(), "the stream stays open");
    s.stop().unwrap();
}

/// A driver's prepare, then its query: describe, then execute with as many arguments as the
/// describe reported, on one stream, on both pipeline endpoints.
#[test]
fn describe_then_execute_on_v2_and_v3() {
    let s = start(Opts::default());
    let c = s.client(None);
    let mut st = c.stream(Proto::V3);
    ok_pipeline(
        &st.pipeline(vec![
            execute(CREATE, &[]),
            execute("INSERT INTO t VALUES (7, 'seven')", &[]),
        ])
        .unwrap(),
    );
    for proto in [Proto::V2, Proto::V3] {
        let mut st = c.stream(proto);
        let sql = "SELECT body FROM t WHERE id = ?";
        let r = st.pipeline(vec![describe(sql)]).unwrap();
        ok_pipeline(&r);
        let d = &r.body["results"][0]["response"];
        assert_eq!(d["type"], "describe", "{proto:?}: {}", r.body);
        assert_eq!(
            d["result"],
            json!({"params":[{"name":null}],"cols":[{"name":"body","decltype":"TEXT"}],
                "is_explain":false,"is_readonly":true}),
            "{proto:?}"
        );
        let n = d["result"]["params"].as_array().unwrap().len();
        let args = vec![Arg::Int(7); n];
        let r = st.pipeline(vec![execute(sql, &args), close()]).unwrap();
        ok_pipeline(&r);
        assert_eq!(result_rows(&r, 0).unwrap(), vec![vec![json!("seven")]]);
    }
    s.stop().unwrap();
}

fn pipeline_body() -> Vec<u8> {
    json!({"baton": null, "requests": [execute("SELECT 1", &[]), close()]})
        .to_string()
        .into_bytes()
}

/// POST `body` to `path` with the extra `headers`; the status and the body text.
fn post_with(url: &str, path: &str, body: &Value, headers: &[(&str, &str)]) -> (u16, String) {
    let mut req = reqwest::blocking::Client::builder()
        .no_proxy()
        .build()
        .unwrap()
        .post(format!("{url}{path}"))
        .body(body.to_string());
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    let r = req.send().unwrap();
    (r.status().as_u16(), r.text().unwrap())
}

const KEY_HEADER_VALUE: &str = "a2V5LXZhbHVlLXRoYXQtbXVzdC1uZXZlci1iZS1lY2hvZWQ=";

/// `x-turso-encryption-key` (PROTOCOL.md 3.1) is refused with 400 before any stream is
/// touched: the server encrypts with its own key file. Any case of the name, every endpoint.
#[test]
fn encryption_key_header_is_refused_before_any_stream() {
    let s = start(Opts::default());
    let c = s.client(None);
    let mut st = c.stream(Proto::V3);
    ok_pipeline(&st.pipeline(vec![execute(CREATE, &[])]).unwrap());
    let baton = st.baton().unwrap().to_string();
    let insert = || execute("INSERT INTO t(body) VALUES ('refused')", &[]);
    let cursor_batch =
        json!({"steps": [{"stmt": stmt("INSERT INTO t(body) VALUES ('refused')", &[])}]});
    let bodies = [
        (
            "/v2/pipeline",
            json!({"baton": null, "requests": [insert()]}),
        ),
        (
            "/v3/pipeline",
            json!({"baton": null, "requests": [insert()]}),
        ),
        (
            "/v3/pipeline",
            json!({"baton": baton, "requests": [insert()]}),
        ),
        ("/v3/cursor", json!({"baton": null, "batch": cursor_batch})),
        ("/v3/cursor", json!({"baton": baton, "batch": cursor_batch})),
    ];
    for (path, body) in &bodies {
        for name in ["x-turso-encryption-key", "X-Turso-Encryption-Key"] {
            let (status, text) = post_with(&s.url(), path, body, &[(name, KEY_HEADER_VALUE)]);
            assert_eq!(status, 400, "{path} {name}: {text}");
            let v: Value = serde_json::from_str(&text).unwrap();
            assert_eq!(v["code"], "ENCRYPTION_KEY_NOT_ACCEPTED", "{text}");
            let msg = v["message"].as_str().unwrap();
            assert!(msg.contains("x-turso-encryption-key"), "{msg}");
            assert!(
                !text.contains(KEY_HEADER_VALUE),
                "the value is echoed: {text}"
            );
        }
    }
    // An empty value is refused too.
    let (status, _) = post_with(
        &s.url(),
        "/v3/pipeline",
        &bodies[1].1,
        &[("x-turso-encryption-key", "")],
    );
    assert_eq!(status, 400);
    // The baton was not used (it still works) and no insert ran.
    let r = st
        .pipeline(vec![execute("SELECT count(*) FROM t", &[]), close()])
        .unwrap();
    ok_pipeline(&r);
    assert_eq!(result_rows(&r, 0).unwrap(), vec![vec![json!(0)]]);
    let report = s.stop().unwrap();
    assert_eq!(
        report.streams_closed, 0,
        "no stream was created: {report:?}"
    );
}

/// Authentication comes first: without the token the header gets 401, with it 400.
#[test]
fn encryption_key_header_after_auth() {
    let s = start(Opts {
        token: true,
        ..Opts::default()
    });
    let body = json!({"baton": null, "requests": [execute("SELECT 1", &[])]});
    let key = ("x-turso-encryption-key", KEY_HEADER_VALUE);
    let (status, text) = post_with(&s.url(), "/v3/pipeline", &body, &[key]);
    assert_eq!(status, 401, "{text}");
    let auth = format!("Bearer {TOKEN}");
    let (status, text) = post_with(
        &s.url(),
        "/v3/pipeline",
        &body,
        &[key, ("Authorization", &auth)],
    );
    assert_eq!(status, 400, "{text}");
    assert!(text.contains("ENCRYPTION_KEY_NOT_ACCEPTED"), "{text}");
    let (status, text) = post_with(&s.url(), "/v3/pipeline", &body, &[("Authorization", &auth)]);
    assert_eq!(status, 200, "{text}");
    s.stop().unwrap();
}

#[test]
fn auth_required_when_token_set() {
    let s = start(Opts {
        token: true,
        ..Opts::default()
    });
    let anon = s.client(None);
    let r = anon
        .post_raw("/v2/pipeline", pipeline_body(), None)
        .unwrap();
    assert_eq!(r.status, 401, "{}", r.body);
    assert_eq!(code(&r), "UNAUTHORIZED");
    let raw = reqwest::blocking::Client::builder()
        .no_proxy()
        .build()
        .unwrap()
        .post(format!("{}/v3/pipeline", s.url()))
        .body(pipeline_body())
        .send()
        .unwrap();
    assert_eq!(raw.status().as_u16(), 401);
    assert_eq!(
        raw.headers()
            .get("www-authenticate")
            .map(|v| v.to_str().unwrap()),
        Some("Bearer")
    );
    // No Content-Type is needed: the server parses the body itself.
    let raw = reqwest::blocking::Client::builder()
        .no_proxy()
        .build()
        .unwrap()
        .post(format!("{}/v2/pipeline", s.url()))
        .header("Authorization", format!("Bearer {TOKEN}"))
        .body(pipeline_body())
        .send()
        .unwrap();
    assert_eq!(raw.status().as_u16(), 200);
    let wrong = format!("Bearer {TOKEN}x");
    let r = anon
        .post_raw("/v2/pipeline", pipeline_body(), Some(&wrong))
        .unwrap();
    assert_eq!(r.status, 401, "{}", r.body);
    let (status, _) = anon
        .stream(Proto::V3)
        .cursor(batch(vec![(None, stmt("SELECT 1", &[]))])["batch"].clone())
        .unwrap();
    assert_eq!(status, 401, "the cursor needs the token too");
    let authed = s.client(Some(TOKEN));
    let mut st = authed.stream(Proto::V2);
    ok_pipeline(&st.pipeline(vec![execute("SELECT 1", &[])]).unwrap());
    let (status, entries) = authed
        .stream(Proto::V3)
        .cursor(batch(vec![(None, stmt("SELECT 1", &[]))])["batch"].clone())
        .unwrap();
    assert_eq!(status, 200, "{entries:?}");
    assert_eq!(anon.get("/health").unwrap().status, 200);
    assert_eq!(anon.get("/v3").unwrap().status, 200);
    assert_eq!(anon.get("/v2").unwrap().status, 200);
    s.stop().unwrap();
}

#[test]
fn auth_scheme_is_case_insensitive_and_basic_is_refused() {
    let s = start(Opts {
        token: true,
        ..Opts::default()
    });
    let c = s.client(None);
    for scheme in ["bearer", "BEARER", "Bearer"] {
        let r = c
            .post_raw(
                "/v2/pipeline",
                pipeline_body(),
                Some(&format!("{scheme} {TOKEN}")),
            )
            .unwrap();
        assert_eq!(r.status, 200, "{scheme}: {}", r.body);
    }
    for auth in [
        format!("Basic {TOKEN}"),
        format!("Bearer{TOKEN}"),
        format!("Token {TOKEN}"),
        TOKEN.to_string(),
        "Bearer".to_string(),
        "Bearer ".to_string(),
    ] {
        let r = c
            .post_raw("/v2/pipeline", pipeline_body(), Some(&auth))
            .unwrap();
        assert_eq!(r.status, 401, "{auth:?}: {}", r.body);
    }
    let r = c
        .post_raw(
            &format!("/v2/pipeline?token={TOKEN}"),
            pipeline_body(),
            None,
        )
        .unwrap();
    assert_eq!(r.status, 401, "a token in the query string is refused");
    s.stop().unwrap();
}

#[test]
fn health_is_starting_until_open_returns() {
    let (gate_tx, gate_rx) = mpsc::channel();
    let s = start(Opts {
        gate: Some(gate_rx),
        ..Opts::default()
    });
    let c = s.client(None);
    let h = c.get("/health").unwrap();
    assert_eq!(h.status, 503);
    assert_eq!(h.body, json!({"status":"starting"}));
    let r = c.post_raw("/v2/pipeline", pipeline_body(), None).unwrap();
    assert_eq!(r.status, 503, "{}", r.body);
    assert_eq!(code(&r), "NOT_READY");
    gate_tx.send(()).unwrap();
    s.wait_ready();
    let h = c.get("/health").unwrap();
    assert_eq!(h.status, 200);
    assert_eq!(h.body, json!({"status":"ready"}));
    ok_pipeline(&c.post_raw("/v2/pipeline", pipeline_body(), None).unwrap());
    s.stop().unwrap();
}

#[test]
fn fenced_server_reports_unhealthy() {
    let s = start(Opts::default());
    let c = s.client(None);
    let mut st = c.stream(Proto::V2);
    ok_pipeline(&st.pipeline(vec![execute(CREATE, &[])]).unwrap());
    let other = tempfile::tempdir().unwrap();
    takeover_with_store(db_config(&other.path().join("db")), s.store.clone()).unwrap();
    let r = st
        .pipeline(vec![execute("INSERT INTO t(body) VALUES ('late')", &[])])
        .unwrap();
    assert_eq!(r.status, 200, "{}", r.body);
    let (_, code) = result_error(&r, 0).expect("the insert fails");
    assert_eq!(code.as_deref(), Some("FENCED"));
    assert_eq!(r.body["baton"], Value::Null, "a fenced stream is closed");
    let h = c.get("/health").unwrap();
    assert_eq!(h.status, 503);
    assert_eq!(h.body, json!({"status":"fenced"}));
    let mut fresh = c.stream(Proto::V2);
    let r = fresh.pipeline(vec![execute("SELECT 1", &[])]).unwrap();
    assert_eq!(r.status, 200, "{}", r.body);
    let (_, code) = result_error(&r, 0).expect("every request is fenced");
    assert_eq!(code.as_deref(), Some("FENCED"));
    assert_eq!(r.body["baton"], Value::Null);
    let mut fresh = c.stream(Proto::V3);
    let r = fresh.pipeline(vec![describe("SELECT 1")]).unwrap();
    let (_, code) = result_error(&r, 0).expect("a describe is fenced too");
    assert_eq!(code.as_deref(), Some("FENCED"));
    let report = s.stop().unwrap();
    assert!(
        report.fenced,
        "a fenced stop must not look like success: {report:?}"
    );
}

#[test]
fn stream_cap_returns_503() {
    let s = start(Opts {
        max_streams: 1,
        ..Opts::default()
    });
    let c = s.client(None);
    let mut a = c.stream(Proto::V2);
    ok_pipeline(&a.pipeline(vec![execute("SELECT 1", &[])]).unwrap());
    assert!(a.baton().is_some());
    let mut b = c.stream(Proto::V2);
    let r = b.pipeline(vec![execute("SELECT 1", &[])]).unwrap();
    assert_eq!(r.status, 503, "{}", r.body);
    assert_eq!(code(&r), "TOO_MANY_STREAMS");
    ok_pipeline(&a.pipeline(vec![close()]).unwrap());
    ok_pipeline(&b.pipeline(vec![execute("SELECT 1", &[])]).unwrap());
    let report = s.stop().unwrap();
    assert_eq!(
        report,
        ShutdownReport {
            clean: true,
            fenced: false,
            in_flight_failed: 0,
            streams_closed: 1
        },
        "stream b was still open"
    );
}

#[test]
fn bind_fails_fast() {
    let cfg = ServeConfig::from_lookup(Some("127.0.0.1:0"), |_| None).unwrap();
    let first = bind(cfg.clone()).unwrap();
    let mut again = cfg.clone();
    again.listen = first.local_addr();
    let e = bind(again).unwrap_err();
    assert!(e.contains("cannot listen on"), "{e}");
    let mut tls = cfg.clone();
    tls.tls = Some(TlsFiles {
        cert: "c.pem".into(),
        key: "k.pem".into(),
    });
    let e = bind(tls).unwrap_err();
    assert!(
        e.contains("c.pem"),
        "the error names the certificate file: {e}"
    );
    let dir = tempfile::tempdir().unwrap();
    let mut missing = cfg.clone();
    missing.token_file = Some(dir.path().join("no-token"));
    let e = bind(missing).unwrap_err();
    assert!(e.contains("DURABLE_SERVE_TOKEN_FILE"), "{e}");
    let mut open = cfg;
    open.listen = "0.0.0.0:0".parse().unwrap();
    let e = bind(open).unwrap_err();
    assert!(e.contains("DURABLE_SERVE_TLS_CERT"), "{e}");
}

/// Once the port is free, another test running in parallel (or another process) can bind
/// the same ephemeral port, and the connect then reaches its listener. A listener left open
/// by `serve` would answer on every attempt, so the check fails only if all attempts connect.
#[test]
fn open_error_is_returned_and_the_port_closes() {
    let mut reached = Vec::new();
    for _ in 0..5 {
        let s = start(Opts {
            open_error: Some("open failed on purpose".into()),
            ..Opts::default()
        });
        let addr = s.addr;
        let e = s.stop().unwrap_err();
        assert_eq!(e, "open failed on purpose");
        if std::net::TcpStream::connect(addr).is_err() {
            return;
        }
        reached.push(addr);
    }
    panic!("something listened after serve returned, on every attempt: {reached:?}");
}

#[test]
fn shutdown_while_opening_waits_for_open_then_closes() {
    let (gate_tx, gate_rx) = mpsc::channel();
    let mut s = start(Opts {
        gate: Some(gate_rx),
        ..Opts::default()
    });
    let c = s.client(None);
    s.request_stop();
    // Health turns to shutting_down once serve saw the shutdown future resolve.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match c.get("/health") {
            Ok(h) if h.body == json!({"status":"shutting_down"}) => {
                assert_eq!(h.status, 503);
                break;
            }
            Ok(h) => assert_eq!(h.body, json!({"status":"starting"})),
            Err(e) => panic!("{e}"),
        }
        assert!(Instant::now() < deadline, "health never said shutting_down");
        std::thread::sleep(Duration::from_millis(5));
    }
    gate_tx.send(()).unwrap();
    let ready = std::mem::replace(&mut s.ready, mpsc::channel().1);
    let report = s.stop().unwrap();
    assert!(report.clean, "{report:?}");
    assert!(
        matches!(ready.try_recv(), Err(mpsc::TryRecvError::Disconnected)),
        "on_ready must not run once shutdown began"
    );
}

/// A TLS handshake with only `version` offered; returns the version and ALPN protocol agreed.
fn handshake(
    ca: &std::path::Path,
    port: u16,
    version: &'static rustls::SupportedProtocolVersion,
) -> (rustls::ProtocolVersion, Option<Vec<u8>>) {
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::{CertificateDer, ServerName};
    let mut roots = rustls::RootCertStore::empty();
    for c in CertificateDer::pem_file_iter(ca).unwrap() {
        roots.add(c.unwrap()).unwrap();
    }
    let mut cfg = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[version])
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    cfg.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    let name = ServerName::try_from("localhost").unwrap();
    let mut conn = rustls::ClientConnection::new(Arc::new(cfg), name).unwrap();
    let mut tcp = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
    tcp.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    while conn.is_handshaking() {
        conn.complete_io(&mut tcp).unwrap();
    }
    (
        conn.protocol_version().unwrap(),
        conn.alpn_protocol().map(<[u8]>::to_vec),
    )
}

#[test]
fn tls_listener_serves_https_and_refuses_plain() {
    let tmp = tempfile::tempdir().unwrap();
    let Some(m) = tls_material(tmp.path()) else {
        return;
    };
    let s = start(Opts {
        tls: Some(m.clone()),
        ..Opts::default()
    });
    let c = s.client(None);
    let h = c.get("/health").unwrap();
    assert_eq!(h.status, 200);
    assert_eq!(h.body, json!({"status":"ready"}));
    ok_pipeline(
        &c.stream(Proto::V3)
            .pipeline(vec![execute("SELECT 1", &[]), close()])
            .unwrap(),
    );
    let port = s.addr.port();
    let plain = Client::new(&format!("http://127.0.0.1:{port}"), None, None).unwrap();
    if let Ok(r) = plain.get("/health") {
        panic!(
            "plain HTTP must not be served, got {}: {}",
            r.status, r.body
        );
    }
    let untrusted = Client::new(&s.url(), None, None).unwrap();
    assert!(
        untrusted.get("/health").is_err(),
        "a client that does not trust the CA is refused"
    );
    let ca = m.join("ca.pem");
    for v in [&rustls::version::TLS13, &rustls::version::TLS12] {
        let (got, alpn) = handshake(&ca, port, v);
        assert_eq!(got, v.version);
        assert_eq!(
            alpn.as_deref(),
            Some(&b"http/1.1"[..]),
            "ALPN is http/1.1 only"
        );
    }
    assert_eq!(c.get("/health").unwrap().status, 200, "still serving");
    let report = s.stop().unwrap();
    assert!(report.clean, "{report:?}");
}

/// Create `t` and slow every commit log upload down by `delay`.
fn slow_commits(s: &common::Server, delay: Duration) {
    let c = s.client(None);
    ok_pipeline(
        &c.stream(Proto::V2)
            .pipeline(vec![execute(CREATE, &[]), close()])
            .unwrap(),
    );
    let fault = s.fault.as_ref().unwrap();
    fault.only_keys_containing("log/");
    fault.delay_puts(delay);
}

#[test]
fn sigterm_finishes_in_flight_then_closes() {
    let mut s = start(Opts {
        fault: true,
        ..Opts::default()
    });
    slow_commits(&s, Duration::from_millis(500));
    let url = s.url();
    let stopping = Arc::new(AtomicBool::new(false));
    let (acked_tx, acked_rx) = mpsc::channel::<i64>();
    let workers: Vec<_> = (0..4i64)
        .map(|w| {
            let (url, stopping, acked_tx) = (url.clone(), stopping.clone(), acked_tx.clone());
            std::thread::spawn(move || {
                let c = Client::new(&url, None, None).unwrap();
                let mut st = c.stream(Proto::V2);
                let mut acked_after_stop = 0;
                let mut refused = Vec::new();
                for i in 0.. {
                    let id = w * 1_000_000 + i;
                    let insert =
                        execute("INSERT INTO t(id, body) VALUES (?, 'x')", &[Arg::Int(id)]);
                    let Ok(r) = st.pipeline(vec![insert]) else {
                        break;
                    };
                    if r.status != 200 {
                        refused.push(r);
                        break;
                    }
                    ok_pipeline(&r);
                    if stopping.load(SeqCst) {
                        acked_after_stop += 1;
                    }
                    acked_tx.send(id).unwrap();
                }
                (acked_after_stop, refused)
            })
        })
        .collect();
    drop(acked_tx);
    let mut acked: Vec<i64> = (0..2)
        .map(|_| acked_rx.recv_timeout(Duration::from_secs(30)).unwrap())
        .collect();
    stopping.store(true, SeqCst);
    s.request_stop();
    // Once health says so (or the port is closed), a new pipeline is refused.
    let probe = Client::new(&url, None, None).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match probe.get("/health") {
            Ok(h) if h.body == json!({"status":"shutting_down"}) => {
                assert_eq!(h.status, 503);
                break;
            }
            Ok(h) => assert_eq!(h.body, json!({"status":"ready"})),
            Err(_) => break,
        }
        assert!(Instant::now() < deadline, "health never said shutting_down");
        std::thread::sleep(Duration::from_millis(2));
    }
    let fresh = Client::new(&url, None, None).unwrap();
    if let Ok(r) = fresh.post_raw("/v2/pipeline", pipeline_body(), None) {
        assert_eq!(r.status, 503, "{}", r.body);
        assert_eq!(code(&r), "SHUTTING_DOWN");
    }
    let store = s.store.clone();
    let report = s.stop().unwrap();
    let mut acked_after_stop = 0;
    for w in workers {
        let (n, refused) = w.join().unwrap();
        acked_after_stop += n;
        for r in refused {
            assert_eq!(r.status, 503, "{}", r.body);
            assert_eq!(code(&r), "SHUTTING_DOWN");
        }
    }
    acked.extend(acked_rx.iter());
    assert!(report.clean, "{report:?}");
    assert_eq!(report.in_flight_failed, 0, "{report:?}");
    assert!(report.streams_closed >= 1, "{report:?}");
    assert!(
        acked_after_stop >= 1,
        "an insert in flight at the signal finished and was acked"
    );
    let durable = restored(&store, "t", "id");
    for id in &acked {
        assert!(
            durable.contains(id),
            "acked insert {id} is not in the bucket"
        );
    }
}

#[test]
fn shutdown_bound_fails_in_flight_rather_than_ack() {
    let s = start(Opts {
        fault: true,
        shutdown_timeout: Duration::from_millis(300),
        ..Opts::default()
    });
    slow_commits(&s, Duration::from_secs(3));
    let seen = s.log_puts();
    let url = s.url();
    let insert = std::thread::spawn(move || {
        let c = Client::new(&url, None, None).unwrap();
        c.stream(Proto::V2)
            .pipeline(vec![execute("INSERT INTO t(body) VALUES ('late')", &[])])
    });
    s.wait_log_put(seen);
    let t = Instant::now();
    let report = s.stop().unwrap();
    let took = t.elapsed();
    if let Ok(r) = insert.join().unwrap() {
        assert_ne!(r.status, 200, "past the bound nothing is acked: {}", r.body);
    }
    assert!(!report.clean, "{report:?}");
    assert_eq!(report.in_flight_failed, 1, "{report:?}");
    assert!(
        took < Duration::from_secs(2),
        "serve returned at the bound, not after the upload: {took:?}"
    );
}

#[test]
// Checks the end state (the stream is free and usable again), not when it was given back.
fn stream_is_given_back_after_a_client_disconnect() {
    let s = start(Opts {
        fault: true,
        ..Opts::default()
    });
    slow_commits(&s, Duration::from_millis(500));
    let seen = s.log_puts();
    let body =
        json!({"baton": null, "requests": [execute("INSERT INTO t(body) VALUES ('gone')", &[])]})
            .to_string();
    let mut tcp = std::net::TcpStream::connect(s.addr).unwrap();
    write!(
        tcp,
        "POST /v2/pipeline HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
    s.wait_log_put(seen);
    tcp.shutdown(std::net::Shutdown::Both).unwrap();
    drop(tcp);
    let store = s.store.clone();
    let report = s.stop().unwrap();
    assert_eq!(
        report,
        ShutdownReport {
            clean: true,
            fenced: false,
            in_flight_failed: 0,
            streams_closed: 1
        },
        "the abandoned request's stream was given back, then closed at shutdown"
    );
    assert_eq!(restored(&store, "t", "id").len(), 1, "its insert committed");
}
