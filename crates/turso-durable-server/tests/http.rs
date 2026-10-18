//! Hrana over HTTP against a server on `127.0.0.1:0` over a `MemStore`, through the test
//! client (`turso_durable_hrana_client`), which builds requests in the real clients' shapes.
mod common;

use common::{db_config, start, Opts, TOKEN};
use serde_json::{json, Value};
use std::sync::mpsc;
use std::time::{Duration, Instant};
use turso_durable::takeover_with_store;
use turso_durable_hrana_client::{
    batch, close, cond_ok, execute, execute_named, get_autocommit, result_error, result_rows, stmt,
    tx_batch_request, Arg, Proto, Reply,
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
        ["step_begin", "row", "row", "step_end"],
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

#[test]
fn cursor_bad_condition_is_a_final_error_entry() {
    let s = start(Opts::default());
    let c = s.client(None);
    let mut st = c.stream(Proto::V3);
    // A condition on a later step fails the whole batch before anything runs.
    let b = batch(vec![(Some(cond_ok(1)), stmt("SELECT 1", &[]))]);
    let (status, entries) = st.cursor(b["batch"].clone()).unwrap();
    assert_eq!(status, 200, "{entries:?}");
    assert_eq!(entries.len(), 1, "{entries:?}");
    assert_eq!(entries[0]["type"], "error");
    assert_eq!(entries[0]["error"]["code"], "PROTO_ERROR");
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

#[test]
fn oversized_body_is_413() {
    let s = start(Opts::default());
    let c = s.client(None);
    let body = vec![b' '; 17 << 20];
    match c.post_raw("/v2/pipeline", body, None) {
        Ok(r) => {
            assert_eq!(r.status, 413, "{}", r.body);
            assert_eq!(code(&r), "PAYLOAD_TOO_LARGE");
        }
        Err(e) => panic!("expected a 413 reply, got a transport error: {e}"),
    }
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

fn pipeline_body() -> Vec<u8> {
    json!({"baton": null, "requests": [execute("SELECT 1", &[]), close()]})
        .to_string()
        .into_bytes()
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
    s.stop().unwrap();
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
    assert!(bind(tls).is_err());
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

#[test]
fn open_error_is_returned_and_the_port_closes() {
    let s = start(Opts {
        open_error: Some("open failed on purpose".into()),
        ..Opts::default()
    });
    let addr = s.addr;
    let e = s.stop().unwrap_err();
    assert_eq!(e, "open failed on purpose");
    assert!(
        std::net::TcpStream::connect(addr).is_err(),
        "nothing listens after serve returned"
    );
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
