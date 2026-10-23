//! The webhook listener of a replica: `POST /notify` turns a bucket notification into
//! [`ReplicaWaker::wake`](buckwal::ReplicaWaker). It reuses the SQL server's accept loop, TLS
//! acceptor, bind rule and constant-time [`Token`], and needs none of the SQL server's types.
//!
//! [`serve`] binds before it returns (so a bad setting or a port in use fails there, and port 0
//! reports its real port through [`NotifyBound::local_addr`]) and then runs on its own thread
//! with its own tokio runtime, so the `buckwal` library itself has no async runtime. It stops
//! when the `shutdown` future resolves; [`NotifyBound::join`] waits for that.
//!
//! What a request must be:
//! - Credentials, when a token file is configured (without one, only a loopback address is
//!   allowed): `Authorization: Bearer <token>`, or HTTP basic authentication with the user
//!   `sns` and the token as the password (all SNS can send, from `https://sns:<token>@host/`).
//!   Both are compared in constant time and neither is ever logged. Missing or wrong
//!   credentials get 401, a WARN `notification rejected` (remote address and reason) and no
//!   wake. Authentication comes before the body is read.
//! - A body of at most 1 MiB (413 past it): an S3 event (`{"Records":[...]}` with at least one
//!   `ObjectCreated` record), or an SNS `Notification` whose `Message` is one, wakes the
//!   replica once and answers 200. The content is only a hint: any well-formed ObjectCreated
//!   event wakes, whatever its keys, because the replica lists the log itself.
//! - Or an SNS `SubscriptionConfirmation`, whose `SubscribeURL` is fetched only when it is
//!   `https://sns.<region>.amazonaws.com/...` (no port, no userinfo, no redirect). A refused URL
//!   gets 400 and a WARN `sns confirmation refused`; a failed fetch gets 502.
//! - Anything else gets 400 and wakes nothing.
use crate::config::{check_bind_rule, TlsFiles, Token};
use crate::listener::{accept_loop, tls_acceptor_named, Conns, MAX_CONNECTIONS};
use axum::body::{to_bytes, Body};
use axum::extract::Request;
use axum::http::header::{AUTHORIZATION, WWW_AUTHENTICATE};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Extension, Router};
use base64::Engine;
use buckwal::ReplicaWaker;
use serde_json::Value;
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio_rustls::TlsAcceptor;

const LISTEN_VAR: &str = "BUCKWAL_REPLICA_NOTIFY_LISTEN";
const CERT_VAR: &str = "BUCKWAL_REPLICA_NOTIFY_TLS_CERT";
const KEY_VAR: &str = "BUCKWAL_REPLICA_NOTIFY_TLS_KEY";
const TOKEN_VAR: &str = "BUCKWAL_REPLICA_NOTIFY_TOKEN_FILE";

/// The biggest request body read, in bytes. An S3 event with a few records is under 3 KB.
const BODY_LIMIT: usize = 1024 * 1024;

/// How long a client may take to send its body.
const BODY_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// How long shutdown waits for open connections before dropping them.
const SHUTDOWN_BOUND: Duration = Duration::from_secs(5);

/// The user name SNS's basic authentication carries.
const BASIC_USER: &str = "sns";

/// Where the webhook listens and what protects it.
#[derive(Debug, Clone)]
pub struct NotifyConfig {
    /// `BUCKWAL_REPLICA_NOTIFY_LISTEN`.
    pub listen: SocketAddr,
    /// HTTPS when set (`BUCKWAL_REPLICA_NOTIFY_TLS_CERT` and `_TLS_KEY`).
    pub tls: Option<TlsFiles>,
    /// The token file (`BUCKWAL_REPLICA_NOTIFY_TOKEN_FILE`); no authentication when unset.
    pub token_file: Option<PathBuf>,
}

impl NotifyConfig {
    /// Read the settings from the process environment: `None` when
    /// `BUCKWAL_REPLICA_NOTIFY_LISTEN` is unset.
    pub fn from_env() -> Result<Option<NotifyConfig>, String> {
        NotifyConfig::from_lookup(|k| std::env::var(k).ok())
    }

    /// Like [`NotifyConfig::from_env`], reading variables through `get`. An empty value counts
    /// as unset.
    pub fn from_lookup(
        get: impl Fn(&str) -> Option<String>,
    ) -> Result<Option<NotifyConfig>, String> {
        let get = |k: &str| get(k).filter(|v| !v.is_empty());
        let Some(text) = get(LISTEN_VAR) else {
            return Ok(None);
        };
        let listen: SocketAddr = text.parse().map_err(|_| {
            format!("{LISTEN_VAR} must be IP:port (for example 127.0.0.1:9396), not {text:?}")
        })?;
        let tls = match (get(CERT_VAR), get(KEY_VAR)) {
            (None, None) => None,
            (Some(cert), Some(key)) => Some(TlsFiles {
                cert: cert.into(),
                key: key.into(),
            }),
            (Some(_), None) => return Err(format!("{CERT_VAR} needs {KEY_VAR}")),
            (None, Some(_)) => return Err(format!("{KEY_VAR} needs {CERT_VAR}")),
        };
        Ok(Some(NotifyConfig {
            listen,
            tls,
            token_file: get(TOKEN_VAR).map(PathBuf::from),
        }))
    }

    /// A non-loopback address needs both TLS and a token; a loopback one needs neither.
    pub fn check_bind(&self) -> Result<(), String> {
        check_bind_rule(
            self.listen,
            self.tls.is_some(),
            self.token_file.is_some(),
            (CERT_VAR, KEY_VAR),
            TOKEN_VAR,
        )
    }
}

/// How the listener fetches an SNS `SubscribeURL`. Only URLs that passed the host check reach
/// it. Implementations must not follow redirects and must keep the URL (it holds a one-time
/// token) out of their errors.
pub trait Fetch: Send + Sync + 'static {
    /// GET `url`; `Ok` on a 2xx answer. Blocking: the listener calls it on a blocking thread.
    fn get(&self, url: &str) -> Result<(), String>;
}

/// The real fetch: reqwest's blocking client over rustls, no redirects, 10 s timeout.
#[derive(Debug, Clone, Copy, Default)]
pub struct HttpFetch;

impl Fetch for HttpFetch {
    fn get(&self, url: &str) -> Result<(), String> {
        // reqwest's rustls backend panics at build() without a process crypto provider; a host
        // app's earlier install wins.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| format!("http client: {}", e.without_url()))?;
        let r = client
            .get(url)
            .send()
            .map_err(|e| format!("request failed: {}", e.without_url()))?;
        match r.status() {
            s if s.is_success() => Ok(()),
            s => Err(format!("answered {s}")),
        }
    }
}

/// A running webhook listener.
#[derive(Debug)]
pub struct NotifyBound {
    addr: SocketAddr,
    tls: bool,
    auth: bool,
    thread: std::thread::JoinHandle<()>,
}

impl NotifyBound {
    /// The address bound (the real port when the config asked for port 0).
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Whether the listener speaks HTTPS.
    pub fn tls(&self) -> bool {
        self.tls
    }

    /// Whether requests need the token.
    pub fn auth(&self) -> bool {
        self.auth
    }

    /// Wait until the listener has stopped (its `shutdown` future resolved).
    pub fn join(self) -> Result<(), String> {
        self.thread
            .join()
            .map_err(|_| "the notification listener panicked".to_string())
    }
}

/// Bind the listener and run it on its own thread until `shutdown` resolves; each accepted
/// notification calls `waker.wake()`.
pub fn serve(
    config: NotifyConfig,
    waker: ReplicaWaker,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<NotifyBound, String> {
    serve_with(config, move || waker.wake(), HttpFetch, shutdown)
}

/// [`serve`] with the wake and the `SubscribeURL` fetch given, for tests and for a host that
/// does its own.
pub fn serve_with(
    config: NotifyConfig,
    wake: impl Fn() + Send + Sync + 'static,
    fetch: impl Fetch,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<NotifyBound, String> {
    config.check_bind()?;
    let tls = config
        .tls
        .as_ref()
        .map(|t| tls_acceptor_named(t, CERT_VAR, KEY_VAR))
        .transpose()?;
    let token = config
        .token_file
        .as_deref()
        .map(|p| Token::load_named(p, TOKEN_VAR))
        .transpose()?;
    let listen = config.listen;
    let fail = |e: std::io::Error| format!("cannot listen on {listen}: {e}");
    let listener = std::net::TcpListener::bind(listen).map_err(fail)?;
    listener.set_nonblocking(true).map_err(fail)?;
    let addr = listener.local_addr().map_err(fail)?;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(4)
        .thread_name("buckwal-notify")
        .enable_all()
        .build()
        .map_err(|e| format!("cannot start the notification runtime: {e}"))?;
    let (is_tls, auth) = (tls.is_some(), token.is_some());
    // Registered with the runtime here, so a failure is returned to the caller.
    let listener = {
        let _in_runtime = rt.enter();
        tokio::net::TcpListener::from_std(listener).map_err(fail)?
    };
    let state = Arc::new(State_ {
        token,
        wake: Box::new(wake),
        fetch: Arc::new(fetch),
    });
    let thread = std::thread::Builder::new()
        .name("buckwal-notify".to_string())
        .spawn(move || {
            rt.block_on(run(listener, router(state), tls, shutdown));
            rt.shutdown_background();
        })
        .map_err(|e| format!("cannot start the notification thread: {e}"))?;
    tracing::info!(target: "buckwal::notify", %addr, tls = is_tls, auth, "notification listener started");
    Ok(NotifyBound {
        addr,
        tls: is_tls,
        auth,
        thread,
    })
}

async fn run(
    listener: tokio::net::TcpListener,
    app: Router,
    tls: Option<TlsAcceptor>,
    shutdown: impl Future<Output = ()>,
) {
    let (stop_tx, stop_rx) = watch::channel(false);
    let accept = tokio::spawn(accept_loop(listener, app, tls, MAX_CONNECTIONS, stop_rx));
    shutdown.await;
    let _ = stop_tx.send(true);
    let mut conns: Option<Conns> = accept.await.ok();
    if let Some(c) = conns.as_mut() {
        if tokio::time::timeout(SHUTDOWN_BOUND, c.finish())
            .await
            .is_err()
        {
            c.abort().await;
        }
    }
}

/// What the handler needs (named apart from axum's `State` extractor).
struct State_ {
    token: Option<Token>,
    wake: Box<dyn Fn() + Send + Sync>,
    fetch: Arc<dyn Fetch>,
}

fn router(state: Arc<State_>) -> Router {
    Router::new()
        .route("/notify", post(notify))
        .with_state(state)
}

/// How the credentials of a request fared.
enum Auth {
    Ok,
    /// No `Authorization` header (or an empty one).
    Missing,
    Wrong,
}

/// Check `Authorization` against `token`: `Bearer <token>` or basic `sns:<token>`. The token
/// is compared through its hash in constant time; nothing here logs a credential.
fn authenticate(token: &Token, headers: &HeaderMap) -> Auth {
    let Some(value) = headers.get(AUTHORIZATION) else {
        return Auth::Missing;
    };
    let Ok(value) = value.to_str() else {
        return Auth::Wrong;
    };
    let value = value.trim();
    if value.is_empty() {
        return Auth::Missing;
    }
    let Some((scheme, credentials)) = value.split_once(' ') else {
        return Auth::Wrong;
    };
    let credentials = credentials.trim();
    let ok = if scheme.eq_ignore_ascii_case("bearer") {
        token.matches(credentials)
    } else if scheme.eq_ignore_ascii_case("basic") {
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(credentials)
            .ok()
            .and_then(|b| String::from_utf8(b).ok());
        match decoded.as_deref().and_then(|d| d.split_once(':')) {
            Some((user, password)) => {
                let password_ok = token.matches(password);
                user == BASIC_USER && password_ok
            }
            None => false,
        }
    } else {
        false
    };
    if ok {
        Auth::Ok
    } else {
        Auth::Wrong
    }
}

fn reply(status: StatusCode, text: &'static str) -> Response {
    (status, text).into_response()
}

/// A 400. The reason (never the body) goes to the debug log, so a topic or bucket set up
/// wrong can be diagnosed.
fn bad_request(why: &'static str) -> Response {
    tracing::debug!(
        target: "buckwal::notify",
        reason = why.trim_end(),
        "notification request refused with 400"
    );
    reply(StatusCode::BAD_REQUEST, why)
}

fn unauthorized() -> Response {
    let mut r = reply(StatusCode::UNAUTHORIZED, "credentials required\n");
    let h = r.headers_mut();
    h.append(WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
    h.append(
        WWW_AUTHENTICATE,
        HeaderValue::from_static("Basic realm=\"buckwal\""),
    );
    r
}

async fn notify(
    axum::extract::State(st): axum::extract::State<Arc<State_>>,
    Extension(peer): Extension<SocketAddr>,
    req: Request,
) -> Response {
    if let Some(token) = &st.token {
        let reason = match authenticate(token, req.headers()) {
            Auth::Ok => None,
            Auth::Missing => Some("no credentials"),
            Auth::Wrong => Some("wrong credentials"),
        };
        if let Some(reason) = reason {
            tracing::warn!(
                target: "buckwal::notify",
                remote = %peer,
                reason,
                "notification rejected"
            );
            return unauthorized();
        }
    }
    let (parts, body) = req.into_parts();
    let declared = parts
        .headers
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok());
    if declared.is_some_and(|n| n > BODY_LIMIT as u64) {
        return reply(StatusCode::PAYLOAD_TOO_LARGE, "body too large\n");
    }
    let bytes = match tokio::time::timeout(BODY_READ_TIMEOUT, read_body(body)).await {
        Ok(Ok(b)) => b,
        Ok(Err(StatusCode::BAD_REQUEST)) => return bad_request("cannot read the body\n"),
        Ok(Err(status)) => return reply(status, "cannot read the body\n"),
        Err(_) => return reply(StatusCode::REQUEST_TIMEOUT, "body read timed out\n"),
    };
    match classify(&bytes) {
        Err(why) => bad_request(why),
        Ok(Action::Wake) => {
            (st.wake)();
            reply(StatusCode::OK, "ok\n")
        }
        Ok(Action::Ack) => reply(StatusCode::OK, "ok\n"),
        Ok(Action::Confirm { url, topic }) => confirm(&st, url, topic).await,
    }
}

async fn read_body(body: Body) -> Result<axum::body::Bytes, StatusCode> {
    to_bytes(body, BODY_LIMIT)
        .await
        .map_err(|e| read_error_status(&e))
}

/// 413 for the body limit's own error, 400 for any other read error (a client that went away
/// mid-body). The limit error is a boxed `http_body_util::LengthLimitError`, a type this
/// crate does not depend on, so it is recognised by its text anywhere in the error chain.
fn read_error_status(e: &(dyn std::error::Error + 'static)) -> StatusCode {
    let mut cur = Some(e);
    while let Some(err) = cur {
        if err.to_string() == "length limit exceeded" {
            return StatusCode::PAYLOAD_TOO_LARGE;
        }
        cur = err.source();
    }
    StatusCode::BAD_REQUEST
}

enum Action {
    Wake,
    /// S3's `s3:TestEvent`: acknowledged, nothing to wake for.
    Ack,
    Confirm {
        url: String,
        topic: String,
    },
}

/// Sort a body into a wake, a confirmation or a refusal (the text is the 400's body).
fn classify(body: &[u8]) -> Result<Action, &'static str> {
    let v: Value = serde_json::from_slice(body).map_err(|_| "not JSON\n")?;
    let obj = v.as_object().ok_or("not a JSON object\n")?;
    let Some(kind) = obj.get("Type") else {
        return if is_object_created(&v) {
            Ok(Action::Wake)
        } else if is_test_event(&v) {
            Ok(Action::Ack)
        } else {
            Err("not an S3 ObjectCreated event\n")
        };
    };
    match kind.as_str() {
        Some("Notification") => {
            let msg = obj
                .get("Message")
                .and_then(Value::as_str)
                .ok_or("no Message\n")?;
            let inner: Value = serde_json::from_str(msg).map_err(|_| "Message is not JSON\n")?;
            if is_object_created(&inner) {
                Ok(Action::Wake)
            } else if is_test_event(&inner) {
                Ok(Action::Ack)
            } else {
                Err("Message is not an S3 ObjectCreated event\n")
            }
        }
        Some("SubscriptionConfirmation") => {
            let url = obj
                .get("SubscribeURL")
                .and_then(Value::as_str)
                .ok_or("no SubscribeURL\n")?;
            let topic = obj.get("TopicArn").and_then(Value::as_str).unwrap_or("");
            Ok(Action::Confirm {
                url: url.to_string(),
                topic: topic.chars().take(256).collect(),
            })
        }
        _ => Err("unsupported message Type\n"),
    }
}

/// S3 sends `{"Event":"s3:TestEvent",...}` once when a notification is configured. Answering
/// 400 would make SNS retry it.
fn is_test_event(v: &Value) -> bool {
    v.get("Event").and_then(Value::as_str) == Some("s3:TestEvent")
}

/// Whether `v` is an S3 event with at least one ObjectCreated record (`eventName` is
/// `ObjectCreated:Put` in a delivered event; `s3:` before it is accepted too).
fn is_object_created(v: &Value) -> bool {
    let Some(records) = v.get("Records").and_then(Value::as_array) else {
        return false;
    };
    records.iter().any(|r| {
        r.get("eventName")
            .and_then(Value::as_str)
            .map(|n| n.strip_prefix("s3:").unwrap_or(n))
            .is_some_and(|n| n.starts_with("ObjectCreated:"))
    })
}

async fn confirm(st: &State_, url: String, topic: String) -> Response {
    if let Err(reason) = check_subscribe_url(&url) {
        tracing::warn!(target: "buckwal::notify", reason, "sns confirmation refused");
        return bad_request("SubscribeURL refused\n");
    }
    let fetch = st.fetch.clone();
    let done = tokio::task::spawn_blocking(move || fetch.get(&url)).await;
    match done {
        Ok(Ok(())) => {
            tracing::info!(target: "buckwal::notify", topic_arn = ?topic, "sns subscription confirmed");
            reply(StatusCode::OK, "confirmed\n")
        }
        Ok(Err(e)) => {
            tracing::warn!(target: "buckwal::notify", error = %e, "sns confirmation failed");
            reply(StatusCode::BAD_GATEWAY, "confirmation failed\n")
        }
        Err(_) => reply(StatusCode::BAD_GATEWAY, "confirmation failed\n"),
    }
}

/// Accept only `https://sns.<region>.amazonaws.com/...`: the scheme, then an authority that is
/// exactly that host (no userinfo, port, escape, bracket or backslash, so no parser can read
/// another host from it). The reason is for the log; it never holds the URL.
fn check_subscribe_url(url: &str) -> Result<(), &'static str> {
    let rest = url.strip_prefix("https://").ok_or("not an https URL")?;
    if url
        .chars()
        .any(|c| c.is_control() || c.is_whitespace() || c == '\\')
    {
        return Err("control, space or backslash in the URL");
    }
    let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let authority = &rest[..end];
    if authority.contains(['@', ':', '%', '[', ']']) {
        return Err("userinfo, port or escape in the host");
    }
    let host = authority.to_ascii_lowercase();
    let region = host
        .strip_prefix("sns.")
        .and_then(|h| h.strip_suffix(".amazonaws.com"))
        .filter(|r| {
            !r.is_empty()
                && !r.starts_with('-')
                && !r.ends_with('-')
                && r.bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        });
    if region.is_none() {
        return Err("host is not sns.<region>.amazonaws.com");
    }
    // The HTTP client's own parser must read the same host.
    let parsed = reqwest::Url::parse(url).map_err(|_| "not a valid URL")?;
    let same = parsed.scheme() == "https"
        && parsed.host_str() == Some(host.as_str())
        && parsed.port().is_none()
        && parsed.username().is_empty()
        && parsed.password().is_none();
    if same {
        Ok(())
    } else {
        Err("the URL parser reads another host")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct Boxed(&'static str, Option<Box<Boxed>>);
    impl std::fmt::Display for Boxed {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.0)
        }
    }
    impl std::error::Error for Boxed {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            self.1.as_deref().map(|e| e as _)
        }
    }

    #[test]
    fn only_the_limit_error_is_413() {
        let limit = Boxed("length limit exceeded", None);
        assert_eq!(read_error_status(&limit), StatusCode::PAYLOAD_TOO_LARGE);
        let wrapped = Boxed("body error", Some(Box::new(limit)));
        assert_eq!(read_error_status(&wrapped), StatusCode::PAYLOAD_TOO_LARGE);
        let gone = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset");
        assert_eq!(read_error_status(&gone), StatusCode::BAD_REQUEST);
        let other = Boxed("body error", Some(Box::new(Boxed("closed", None))));
        assert_eq!(read_error_status(&other), StatusCode::BAD_REQUEST);
    }
}
