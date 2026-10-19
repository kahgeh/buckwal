//! The axum router: health, the version probes, Hrana `/v2/pipeline`, `/v3/pipeline` and
//! `/v3/cursor`, the bearer-token check and the `durable.server.request` span.
//!
//! A pipeline runs in `spawn_blocking` inside the request span, so the library's commit
//! spans are its children. The blocking task checks the stream out, runs the requests and
//! gives the stream back itself, so a client that disconnects mid-request never leaks it.
//! The response is built only after the library calls returned (the ack rule).
//!
//! Never logged or put in a span: SQL text, arguments, batons, the token, a client's
//! encryption key, or a `ProtoError` message (a serde error can quote the input).
use crate::config::Token;
use crate::exec::{execute_request, run_batch, Fence, Version};
use crate::proto::{
    lib_error, BatchResult, CursorEntry, CursorReq, CursorRespBody, PipelineReq, PipelineResp,
    ProtoError, StreamResult,
};
use crate::streams::{Checkout, Leased, Streams};
use axum::body::{Body, Bytes};
use axum::extract::rejection::BytesRejection;
use axum::extract::{DefaultBodyLimit, FromRequest, Request, State};
use axum::http::header::{AUTHORIZATION, CONNECTION, CONTENT_TYPE, WWW_AUTHENTICATE};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::Response;
use axum::routing::{get, post};
use axum::Router;
use serde::Serialize;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::Semaphore;
use tracing::field::Empty;
use tracing::Span;

/// The largest request body, in bytes.
pub(crate) const BODY_LIMIT: usize = 16 << 20;

/// How long reading a request body may take. A slower body is answered 408 and its
/// connection closed, so it cannot hold a connection (or stall shutdown) for long.
pub(crate) const BODY_READ_TIMEOUT: Duration = Duration::from_secs(30);

const STARTING: u8 = 0;
const READY: u8 = 1;
const SHUTTING_DOWN: u8 = 2;

/// State shared by every handler.
pub(crate) struct App {
    phase: AtomicU8,
    streams: OnceLock<Arc<Streams>>,
    fence: Fence,
    token: Option<Token>,
    /// One permit per worker: a pipeline holds one while it runs in `spawn_blocking`.
    permits: Arc<Semaphore>,
    workers: u32,
    body_timeout: Duration,
    /// Shutdown gave up waiting: a pipeline dropped from now on counts as failed.
    past_bound: AtomicBool,
    /// Pipelines dropped unanswered after the bound (see [`Unanswered`]).
    failed: AtomicUsize,
}

impl App {
    pub(crate) fn new(token: Option<Token>, workers: usize, body_timeout: Duration) -> Arc<App> {
        let workers = u32::try_from(workers.clamp(1, Semaphore::MAX_PERMITS)).unwrap_or(u32::MAX);
        Arc::new(App {
            phase: AtomicU8::new(STARTING),
            streams: OnceLock::new(),
            fence: Fence::new(),
            token,
            permits: Arc::new(Semaphore::new(workers as usize)),
            workers,
            body_timeout,
            past_bound: AtomicBool::new(false),
            failed: AtomicUsize::new(0),
        })
    }

    /// The database is open: serve pipelines on `streams`.
    pub(crate) fn set_ready(&self, streams: Arc<Streams>) {
        let _ = self.streams.set(streams);
        let _ = self
            .phase
            .compare_exchange(STARTING, READY, Ordering::SeqCst, Ordering::SeqCst);
    }

    /// The shared latch, for shutdown to trip on a `Fenced` close error.
    pub(crate) fn fence(&self) -> Fence {
        self.fence.clone()
    }

    /// Whether a library call answered `Fenced` during the run.
    pub(crate) fn fenced(&self) -> bool {
        self.fence.is_set()
    }

    /// Health reports `shutting_down` and new pipelines get 503 `SHUTTING_DOWN`.
    pub(crate) fn begin_shutdown(&self) {
        self.phase.store(SHUTTING_DOWN, Ordering::SeqCst);
    }

    /// Wait until no pipeline runs any more, then refuse new ones.
    pub(crate) async fn drain(&self) {
        if let Ok(all) = self.permits.acquire_many(self.workers).await {
            all.forget();
        }
        self.permits.close();
    }

    /// The shutdown bound is reached: no pipeline starts any more, and each one dropped
    /// unanswered from now on (its connection aborted) counts in [`App::failed`].
    pub(crate) fn give_up(&self) {
        self.past_bound.store(true, Ordering::SeqCst);
        self.permits.close();
    }

    /// Pipelines dropped unanswered after [`App::give_up`].
    pub(crate) fn failed(&self) -> usize {
        self.failed.load(Ordering::SeqCst)
    }

    fn phase(&self) -> u8 {
        self.phase.load(Ordering::SeqCst)
    }
}

pub(crate) fn router(app: Arc<App>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v2", get(probe))
        .route("/v3", get(probe))
        .route("/v2/pipeline", post(pipeline_v2))
        .route("/v3/pipeline", post(pipeline_v3))
        .route("/v3/cursor", post(cursor))
        .fallback(not_found)
        .method_not_allowed_fallback(not_found)
        .layer(DefaultBodyLimit::max(BODY_LIMIT))
        .with_state(app)
}

/// How a request ended: its response and the span's `durable.outcome`.
struct Done {
    resp: Box<Response>,
    outcome: &'static str,
}

fn json_bytes(status: StatusCode, body: Vec<u8>, content_type: &'static str) -> Response {
    let mut r = Response::new(Body::from(body));
    *r.status_mut() = status;
    r.headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    r
}

fn to_json<T: Serialize>(v: &T) -> Vec<u8> {
    serde_json::to_vec(v).unwrap_or_else(|_| {
        br#"{"message":"the response could not be encoded","code":"INTERNAL_ERROR"}"#.to_vec()
    })
}

fn json<T: Serialize>(status: StatusCode, v: &T) -> Response {
    json_bytes(status, to_json(v), "application/json")
}

fn error(status: StatusCode, code: &str, message: &str, outcome: &'static str) -> Done {
    Done {
        resp: Box::new(json(status, &ProtoError::new(code, message))),
        outcome,
    }
}

fn shutting_down() -> Done {
    error(
        StatusCode::SERVICE_UNAVAILABLE,
        "SHUTTING_DOWN",
        "the server is shutting down",
        "shutting_down",
    )
}

fn not_ready() -> Done {
    error(
        StatusCode::SERVICE_UNAVAILABLE,
        "NOT_READY",
        "the database is not open yet",
        "not_ready",
    )
}

fn internal() -> Done {
    error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "INTERNAL_ERROR",
        "the request failed inside the server",
        "error",
    )
}

async fn health(State(app): State<Arc<App>>) -> Response {
    let status = match app.phase() {
        SHUTTING_DOWN => "shutting_down",
        _ if app.fence.is_set() => "fenced",
        READY => "ready",
        _ => "starting",
    };
    let code = if status == "ready" {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    json(code, &serde_json::json!({ "status": status }))
}

async fn probe() -> Response {
    json(StatusCode::OK, &serde_json::json!({}))
}

async fn not_found() -> Response {
    json(
        StatusCode::NOT_FOUND,
        &ProtoError::new("NOT_FOUND", "no such endpoint"),
    )
}

/// Whether `headers` carry `Authorization: Bearer <token>` (the scheme in any case).
fn authorized(token: &Token, headers: &HeaderMap) -> bool {
    let Some(value) = headers.get(AUTHORIZATION).and_then(|v| v.to_str().ok()) else {
        return false;
    };
    let Some((scheme, credentials)) = value.split_once(' ') else {
        return false;
    };
    scheme.eq_ignore_ascii_case("bearer") && token.matches(credentials.trim())
}

/// A client's remote encryption key (PROTOCOL.md 3.1). Its value is never read.
const ENCRYPTION_KEY_HEADER: &str = "x-turso-encryption-key";

/// The state check, the token check, then the refusal of a client encryption key.
fn admit(app: &App, headers: &HeaderMap) -> Result<Arc<Streams>, Done> {
    match app.phase() {
        SHUTTING_DOWN => return Err(shutting_down()),
        READY => {}
        _ => return Err(not_ready()),
    }
    if let Some(token) = &app.token {
        if !authorized(token, headers) {
            let mut done = error(
                StatusCode::UNAUTHORIZED,
                "UNAUTHORIZED",
                "a valid bearer token is required",
                "unauthorized",
            );
            done.resp
                .headers_mut()
                .insert(WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
            return Err(done);
        }
    }
    if headers.contains_key(ENCRYPTION_KEY_HEADER) {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "ENCRYPTION_KEY_NOT_ACCEPTED",
            "x-turso-encryption-key is not accepted: this server encrypts the database with \
             its own key file; remove the header",
            "bad_request",
        ));
    }
    app.streams.get().cloned().ok_or_else(not_ready)
}

fn body_rejection(rej: BytesRejection) -> Done {
    if rej.status() == StatusCode::PAYLOAD_TOO_LARGE {
        return error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "PAYLOAD_TOO_LARGE",
            &format!("the request body is larger than {BODY_LIMIT} bytes"),
            "bad_request",
        );
    }
    error(
        StatusCode::BAD_REQUEST,
        "PROTO_ERROR",
        "the request body could not be read",
        "bad_request",
    )
}

fn body_timeout() -> Done {
    let mut done = error(
        StatusCode::REQUEST_TIMEOUT,
        "PROTO_ERROR",
        "the request body was not received in time",
        "bad_request",
    );
    // The rest of the body is never read, so the connection cannot be reused.
    done.resp
        .headers_mut()
        .insert(CONNECTION, HeaderValue::from_static("close"));
    done
}

fn bad_json(e: serde_json::Error) -> Done {
    error(
        StatusCode::BAD_REQUEST,
        "PROTO_ERROR",
        &format!("invalid request body: {e}"),
        "bad_request",
    )
}

/// Record the status and outcome on the span; return the response.
fn finish(span: &Span, done: Done) -> Response {
    span.record("http.status", done.resp.status().as_u16());
    span.record("durable.outcome", done.outcome);
    *done.resp
}

fn request_span(route: &'static str) -> Span {
    tracing::info_span!(
        target: "turso_durable::server",
        "durable.server.request",
        http.route = route,
        http.status = Empty,
        durable.stream = Empty,
        durable.statements = Empty,
        durable.outcome = Empty,
    )
}

/// Statements a pipeline asks for: one per `execute` or `sequence`, a batch's steps.
fn statements(requests: &[serde_json::Value]) -> usize {
    requests
        .iter()
        .map(|r| match r.get("type").and_then(|t| t.as_str()) {
            Some("execute" | "sequence") => 1,
            Some("batch") => r
                .pointer("/batch/steps")
                .and_then(|s| s.as_array())
                .map_or(0, Vec::len),
            _ => 0,
        })
        .sum()
}

async fn pipeline_v2(State(app): State<Arc<App>>, req: Request) -> Response {
    let span = request_span("/v2/pipeline");
    parse_then_run(app, req, &span, |app, streams, body: PipelineReq| {
        run_pipeline(app, streams, Version::V2, body)
    })
    .await
}

async fn pipeline_v3(State(app): State<Arc<App>>, req: Request) -> Response {
    let span = request_span("/v3/pipeline");
    parse_then_run(app, req, &span, |app, streams, body: PipelineReq| {
        run_pipeline(app, streams, Version::V3, body)
    })
    .await
}

async fn cursor(State(app): State<Arc<App>>, req: Request) -> Response {
    let span = request_span("/v3/cursor");
    parse_then_run(app, req, &span, run_cursor).await
}

/// A request body that says how many statements it holds.
trait Counted {
    fn statements(&self) -> usize;
}

impl Counted for PipelineReq {
    fn statements(&self) -> usize {
        statements(&self.requests)
    }
}

impl Counted for CursorReq {
    fn statements(&self) -> usize {
        self.batch.steps.len()
    }
}

/// Counts a pipeline as failed when its handler is dropped unanswered (its connection
/// aborted) after the shutdown bound. A drop before the bound is a client that went away,
/// not a failure: its blocking job, if any, still runs to the end and gives its stream back.
struct Unanswered {
    app: Arc<App>,
    answered: bool,
}

impl Drop for Unanswered {
    fn drop(&mut self) {
        if !self.answered && self.app.past_bound.load(Ordering::SeqCst) {
            self.app.failed.fetch_add(1, Ordering::SeqCst);
        }
    }
}

/// Run the pipeline handler, tracked by [`Unanswered`].
async fn parse_then_run<T, F>(app: Arc<App>, req: Request, span: &Span, work: F) -> Response
where
    T: serde::de::DeserializeOwned + Counted + Send + 'static,
    F: FnOnce(&App, &Streams, T) -> Done + Send + 'static,
{
    let mut pending = Unanswered {
        app: app.clone(),
        answered: false,
    };
    let resp = admit_then_run(app, req, span, work).await;
    pending.answered = true;
    resp
}

/// Admit the request, read (within the body timeout) and parse its body, take a worker
/// permit, then run `work` in `spawn_blocking` inside `span`.
async fn admit_then_run<T, F>(app: Arc<App>, req: Request, span: &Span, work: F) -> Response
where
    T: serde::de::DeserializeOwned + Counted + Send + 'static,
    F: FnOnce(&App, &Streams, T) -> Done + Send + 'static,
{
    let streams = match admit(&app, req.headers()) {
        Ok(s) => s,
        Err(done) => return finish(span, done),
    };
    let read = tokio::time::timeout(app.body_timeout, Bytes::from_request(req, &()));
    let body = match read.await {
        Ok(Ok(b)) => b,
        Ok(Err(rej)) => return finish(span, body_rejection(rej)),
        Err(_) => return finish(span, body_timeout()),
    };
    let parsed: T = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => return finish(span, bad_json(e)),
    };
    drop(body);
    span.record("durable.statements", parsed.statements());
    let Ok(permit) = app.permits.clone().acquire_owned().await else {
        return finish(span, shutting_down());
    };
    let job_span = span.clone();
    let job = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        job_span.in_scope(|| finish(&job_span, work(&app, &streams, parsed)))
    });
    match job.await {
        Ok(resp) => resp,
        Err(_) => finish(span, internal()),
    }
}

/// Check a stream out of the registry, or the HTTP error that ends the request.
fn checkout(app: &App, streams: &Streams, baton: Option<&str>) -> Result<Leased, Done> {
    match streams.checkout(baton) {
        Checkout::Stream(leased) => {
            Span::current().record("durable.stream", leased.id);
            Ok(leased)
        }
        Checkout::Expired => Err(error(
            StatusCode::BAD_REQUEST,
            "STREAM_EXPIRED",
            "the stream is unknown, expired, closed or in use",
            "stream_expired",
        )),
        Checkout::Full => Err(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "TOO_MANY_STREAMS",
            "too many open streams",
            "too_many_streams",
        )),
        Checkout::Failed(e) => {
            let fenced = matches!(e, turso_durable::Error::Fenced(_));
            if fenced {
                app.fence.set();
            }
            if app.phase() == SHUTTING_DOWN {
                return Err(shutting_down());
            }
            Err(Done {
                resp: Box::new(json(StatusCode::SERVICE_UNAVAILABLE, &lib_error(&e))),
                outcome: if fenced { "fenced" } else { "error" },
            })
        }
    }
}

/// Run `f` on the leased stream, catching a panic. On a panic the stream is closed. Either
/// way it is given back; the new baton comes with the result.
fn on_stream<R>(
    streams: &Streams,
    mut leased: Leased,
    f: impl FnOnce(&mut Leased) -> R,
) -> Result<(R, Option<String>), Done> {
    match catch_unwind(AssertUnwindSafe(|| f(&mut leased))) {
        Ok(r) => Ok((r, streams.give_back(leased))),
        Err(_) => {
            let id = leased.id;
            leased.state.closed = true;
            streams.give_back(leased);
            tracing::error!(
                target: "turso_durable::server",
                stream = id,
                "request panicked; its stream is closed"
            );
            Err(internal())
        }
    }
}

fn outcome_of<'a>(errors: impl Iterator<Item = &'a ProtoError>) -> &'static str {
    let mut outcome = "ok";
    for e in errors {
        if e.code.as_deref() == Some("FENCED") {
            return "fenced";
        }
        outcome = "error";
    }
    outcome
}

fn run_pipeline(app: &App, streams: &Streams, v: Version, req: PipelineReq) -> Done {
    let leased = match checkout(app, streams, req.baton.as_deref()) {
        Ok(l) => l,
        Err(done) => return done,
    };
    let requests = req.requests;
    let run = on_stream(streams, leased, |l| {
        requests
            .into_iter()
            .map(|r| execute_request(&mut l.state, v, r, &app.fence))
            .collect::<Vec<StreamResult>>()
    });
    let (results, baton) = match run {
        Ok(r) => r,
        Err(done) => return done,
    };
    let outcome = outcome_of(results.iter().filter_map(|r| match r {
        StreamResult::Error { error } => Some(error),
        StreamResult::Ok { .. } => None,
    }));
    Done {
        resp: Box::new(json(
            StatusCode::OK,
            &PipelineResp {
                baton,
                base_url: None,
                results,
            },
        )),
        outcome,
    }
}

/// The cursor entries of a finished batch: per step that ran, `step_begin`, its rows and
/// `step_end`, or `step_error`, then the `replication_index` terminator. A whole-batch error
/// is one final `error` entry, with no terminator.
fn cursor_entries(result: Result<BatchResult, ProtoError>) -> Vec<CursorEntry> {
    let batch = match result {
        Ok(b) => b,
        Err(error) => return vec![CursorEntry::Error { error }],
    };
    let mut entries = Vec::new();
    let steps = batch.step_results.into_iter().zip(batch.step_errors);
    for (i, (res, err)) in steps.enumerate() {
        let step = u32::try_from(i).unwrap_or(u32::MAX);
        if let Some(error) = err {
            entries.push(CursorEntry::StepError { step, error });
            continue;
        }
        let Some(r) = res else { continue };
        entries.push(CursorEntry::StepBegin { step, cols: r.cols });
        entries.extend(r.rows.into_iter().map(|row| CursorEntry::Row { row }));
        entries.push(CursorEntry::StepEnd {
            affected_row_count: r.affected_row_count,
            last_insert_rowid: r.last_insert_rowid,
        });
    }
    entries.push(CursorEntry::ReplicationIndex {
        replication_index: None,
    });
    entries
}

fn run_cursor(app: &App, streams: &Streams, req: CursorReq) -> Done {
    let leased = match checkout(app, streams, req.baton.as_deref()) {
        Ok(l) => l,
        Err(done) => return done,
    };
    let batch = req.batch;
    let run = on_stream(streams, leased, |l| {
        run_batch(&mut l.state, Version::V3, &batch, &app.fence)
    });
    let (result, baton) = match run {
        Ok(r) => r,
        Err(done) => return done,
    };
    let entries = cursor_entries(result);
    let outcome = outcome_of(entries.iter().filter_map(|e| match e {
        CursorEntry::StepError { error, .. } | CursorEntry::Error { error } => Some(error),
        _ => None,
    }));
    let mut body = to_json(&CursorRespBody {
        baton,
        base_url: None,
    });
    body.push(b'\n');
    for e in &entries {
        body.extend(to_json(e));
        body.push(b'\n');
    }
    Done {
        resp: Box::new(json_bytes(StatusCode::OK, body, "application/x-ndjson")),
        outcome,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{Col, StmtResult};
    use serde_json::json;

    fn token() -> (tempfile::TempDir, Token) {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("t");
        std::fs::write(&p, "abcdefghijklmnop0123").unwrap();
        let t = Token::load(&p).unwrap();
        (d, t)
    }

    fn headers(auth: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(AUTHORIZATION, HeaderValue::from_str(auth).unwrap());
        h
    }

    #[test]
    fn a_panic_closes_the_stream_and_gives_it_back() {
        use crate::streams::StreamsCfg;
        use turso_durable::store::MemStore;
        let dir = tempfile::tempdir().unwrap();
        let mut c =
            turso_durable::Config::new(dir.path().join("db"), "http://127.0.0.1:1", "b", "k", "s");
        c.retry_deadline = Duration::from_secs(2);
        c.request_timeout = Duration::from_millis(500);
        c.lease = Duration::from_secs(5);
        let db = turso_durable::open_with_store(c, Arc::new(MemStore::new())).unwrap();
        let streams = Streams::new(
            db,
            StreamsCfg {
                timeout: Duration::from_secs(10),
                busy: Duration::from_secs(1),
                max: 4,
            },
        )
        .unwrap();
        let Checkout::Stream(leased) = streams.checkout(None) else {
            panic!("no stream");
        };
        assert_eq!(streams.busy(), 1);
        let r = on_stream(&streams, leased, |_| -> () { panic!("injected panic") });
        let Err(done) = r else {
            panic!("a panic is an error");
        };
        assert_eq!(done.resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(streams.busy(), 0, "the stream was given back");
        let Checkout::Stream(leased) = streams.checkout(None) else {
            panic!("no stream");
        };
        let Ok((n, baton)) = on_stream(&streams, leased, |_| 7) else {
            panic!("no panic, no error");
        };
        assert_eq!(n, 7);
        assert!(baton.is_some());
        assert_eq!(streams.close_all().streams, 1);
        streams.database().clone().close().unwrap();
    }

    #[test]
    fn bearer_scheme_any_case_and_nothing_else() {
        let (_d, t) = token();
        for ok in [
            "Bearer abcdefghijklmnop0123",
            "bearer abcdefghijklmnop0123",
            "BeArEr  abcdefghijklmnop0123",
        ] {
            assert!(authorized(&t, &headers(ok)), "{ok}");
        }
        for bad in [
            "Basic abcdefghijklmnop0123",
            "abcdefghijklmnop0123",
            "Bearer",
            "Bearer abcdefghijklmnop012",
            "Bearerabcdefghijklmnop0123",
        ] {
            assert!(!authorized(&t, &headers(bad)), "{bad}");
        }
        assert!(!authorized(&t, &HeaderMap::new()));
    }

    fn admit_error(app: &App, h: &HeaderMap) -> (StatusCode, &'static str, serde_json::Value) {
        let Err(done) = admit(app, h) else {
            panic!("admitted");
        };
        let status = done.resp.status();
        let body = body_json(*done.resp);
        (status, done.outcome, body)
    }

    fn body_json(r: Response) -> serde_json::Value {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let bytes = rt
            .block_on(axum::body::to_bytes(r.into_body(), usize::MAX))
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[test]
    fn encryption_key_header_is_refused_after_the_token_check() {
        let (_d, t) = token();
        let app = App::new(Some(t), 1, Duration::from_secs(1));
        app.phase.store(READY, Ordering::SeqCst);
        let mut h = headers("Bearer abcdefghijklmnop0123");
        h.insert(
            "X-Turso-Encryption-Key",
            HeaderValue::from_static("c2VjcmV0"),
        );
        let (status, outcome, body) = admit_error(&app, &h);
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(outcome, "bad_request");
        assert_eq!(body["code"], "ENCRYPTION_KEY_NOT_ACCEPTED");
        let msg = body["message"].as_str().unwrap();
        assert!(msg.contains("x-turso-encryption-key"), "{msg}");
        assert!(!msg.contains("c2VjcmV0"), "{msg}");
        h.remove(AUTHORIZATION);
        let (status, outcome, _) = admit_error(&app, &h);
        assert_eq!(
            (status, outcome),
            (StatusCode::UNAUTHORIZED, "unauthorized")
        );
    }

    #[test]
    fn statements_are_counted_per_execute_and_batch_step() {
        let reqs = vec![
            json!({"type":"execute","stmt":{"sql":"x"}}),
            json!({"type":"batch","batch":{"steps":[{"stmt":{}},{"stmt":{}}]}}),
            json!({"type":"sequence","sql":"a;b"}),
            json!({"type":"close"}),
            json!({"type":"get_autocommit"}),
            json!(5),
        ];
        assert_eq!(statements(&reqs), 4);
    }

    #[test]
    fn cursor_entries_follow_the_steps() {
        let r = StmtResult {
            cols: vec![Col {
                name: Some("a".into()),
                decltype: None,
            }],
            rows: vec![vec![], vec![]],
            affected_row_count: 0,
            last_insert_rowid: None,
            rows_read: 0,
            rows_written: 0,
            query_duration_ms: 0.0,
        };
        let e = ProtoError::new("SQLITE_ERROR", "boom");
        let entries = cursor_entries(Ok(BatchResult {
            step_results: vec![Some(r), None, None],
            step_errors: vec![None, Some(e.clone()), None],
        }));
        let kinds: Vec<String> = entries
            .iter()
            .map(|x| serde_json::to_value(x).unwrap()["type"].to_string())
            .collect();
        assert_eq!(
            kinds,
            [
                "\"step_begin\"",
                "\"row\"",
                "\"row\"",
                "\"step_end\"",
                "\"step_error\"",
                "\"replication_index\""
            ]
        );
        assert_eq!(
            serde_json::to_value(entries.last().unwrap()).unwrap(),
            json!({"type":"replication_index","replication_index":null})
        );
        assert_eq!(outcome_of([&e].into_iter()), "error");
        // A fatal error ends the cursor: no terminator after it.
        let entries = cursor_entries(Err(ProtoError::new("PROTO_ERROR", "bad")));
        assert!(matches!(entries.as_slice(), [CursorEntry::Error { .. }]));
        // An empty batch completes: the terminator alone.
        let entries = cursor_entries(Ok(BatchResult {
            step_results: vec![],
            step_errors: vec![],
        }));
        assert_eq!(
            serde_json::to_value(&entries).unwrap(),
            json!([{"type":"replication_index","replication_index":null}])
        );
        let f = ProtoError::new("FENCED", "fenced");
        assert_eq!(outcome_of([&e, &f].into_iter()), "fenced");
        assert_eq!(outcome_of(std::iter::empty()), "ok");
    }
}
