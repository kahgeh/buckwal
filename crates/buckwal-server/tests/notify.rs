//! The webhook listener (`buckwal_server::notify`): auth, bodies, SNS confirmation. Servers
//! listen on port 0; the wake and the confirmation fetch are recorded, never real.
mod common;

use base64::Engine;
use buckwal_server::notify::{serve_with, Fetch, NotifyBound, NotifyConfig};
use buckwal_server::TlsFiles;
use serde_json::json;
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};

const TOKEN: &str = "notify-token-0123456789abcdef";

type Rec = BTreeMap<String, String>;

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<Rec>>>);

struct Fields<'a>(&'a mut Rec);

impl Visit for Fields<'_> {
    fn record_str(&mut self, f: &Field, v: &str) {
        self.0.insert(f.name().to_string(), v.to_string());
    }
    fn record_debug(&mut self, f: &Field, v: &dyn std::fmt::Debug) {
        self.0.insert(f.name().to_string(), format!("{v:?}"));
    }
}

impl<S: Subscriber> Layer<S> for Capture {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut rec = Rec::new();
        let level = match *event.metadata().level() {
            Level::WARN => "WARN",
            Level::INFO => "INFO",
            Level::ERROR => "ERROR",
            _ => "OTHER",
        };
        rec.insert("level".into(), level.into());
        event.record(&mut Fields(&mut rec));
        self.0.lock().unwrap().push(rec);
    }
}

fn capture() -> &'static Capture {
    static CAP: OnceLock<Capture> = OnceLock::new();
    CAP.get_or_init(|| {
        let c = Capture::default();
        tracing::subscriber::set_global_default(tracing_subscriber::registry().with(c.clone()))
            .unwrap();
        c
    })
}

fn events(message: &str) -> Vec<Rec> {
    capture()
        .0
        .lock()
        .unwrap()
        .iter()
        .filter(|r| r.get("message").map(String::as_str) == Some(message))
        .cloned()
        .collect()
}

#[derive(Clone, Default)]
struct Recorder {
    urls: Arc<Mutex<Vec<String>>>,
    fail: bool,
}

impl Fetch for Recorder {
    fn get(&self, url: &str) -> Result<(), String> {
        self.urls.lock().unwrap().push(url.to_string());
        if self.fail {
            Err("connection refused".to_string())
        } else {
            Ok(())
        }
    }
}

#[derive(Default)]
struct Opts {
    token: bool,
    tls: bool,
    fail_fetch: bool,
}

struct Srv {
    bound: NotifyBound,
    addr: SocketAddr,
    wakes: Arc<AtomicUsize>,
    fetched: Arc<Mutex<Vec<String>>>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    ca: Option<PathBuf>,
    _dir: tempfile::TempDir,
}

fn start(o: Opts) -> Srv {
    capture();
    let dir = tempfile::tempdir().unwrap();
    let token_file = o.token.then(|| {
        let p = dir.path().join("token");
        std::fs::write(&p, format!("{TOKEN}\n")).unwrap();
        p
    });
    let (tls, ca) = if o.tls {
        let m = common::tls_material(dir.path()).expect("openssl is needed for the TLS test");
        (
            Some(TlsFiles {
                cert: m.join("server/rustfs_cert.pem"),
                key: m.join("server/rustfs_key.pem"),
            }),
            Some(m.join("ca.pem")),
        )
    } else {
        (None, None)
    };
    let cfg = NotifyConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        tls,
        token_file,
    };
    let wakes = Arc::new(AtomicUsize::new(0));
    let rec = Recorder {
        fail: o.fail_fetch,
        ..Recorder::default()
    };
    let fetched = rec.urls.clone();
    let w = wakes.clone();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let bound = serve_with(
        cfg,
        move || {
            w.fetch_add(1, Ordering::SeqCst);
        },
        rec,
        async move {
            let _ = rx.await;
        },
    )
    .unwrap();
    let addr = bound.local_addr();
    assert_ne!(addr.port(), 0);
    Srv {
        bound,
        addr,
        wakes,
        fetched,
        stop: Some(tx),
        ca,
        _dir: dir,
    }
}

impl Srv {
    fn wakes(&self) -> usize {
        self.wakes.load(Ordering::SeqCst)
    }
    fn fetched(&self) -> Vec<String> {
        self.fetched.lock().unwrap().clone()
    }
    fn stop(mut self) {
        let _ = self.stop.take().unwrap().send(());
        self.bound.join().unwrap();
    }
    /// A plain HTTP/1.1 request; returns the status, the body and the client's own address.
    fn raw(&self, path: &str, auth: Option<&str>, body: &[u8]) -> (u16, String, SocketAddr) {
        let mut s = TcpStream::connect(self.addr).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        let me = s.local_addr().unwrap();
        let mut head = format!(
            "POST {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\nContent-Length: {}\r\n",
            body.len()
        );
        if let Some(a) = auth {
            head.push_str(&format!("Authorization: {a}\r\n"));
        }
        head.push_str("\r\n");
        // An oversize body may be refused before it is all sent.
        let _ = s.write_all(head.as_bytes());
        let _ = s.write_all(body);
        let mut out = Vec::new();
        let _ = s.read_to_end(&mut out);
        let text = String::from_utf8_lossy(&out).to_string();
        let status = text
            .split_whitespace()
            .nth(1)
            .and_then(|c| c.parse().ok())
            .unwrap_or(0);
        let body = text.split_once("\r\n\r\n").map(|x| x.1).unwrap_or("");
        (status, body.to_string(), me)
    }
    fn post(&self, auth: Option<&str>, body: &serde_json::Value) -> u16 {
        self.raw("/notify", auth, body.to_string().as_bytes()).0
    }
}

fn bearer() -> String {
    format!("Bearer {TOKEN}")
}

fn basic(user: &str, pass: &str) -> String {
    let b = base64::engine::general_purpose::STANDARD.encode(format!("{user}:{pass}"));
    format!("Basic {b}")
}

fn s3_event(keys: &[&str]) -> serde_json::Value {
    let records: Vec<_> = keys
        .iter()
        .map(|k| {
            json!({"eventVersion":"2.1","eventSource":"aws:s3","eventName":"ObjectCreated:Put",
                   "s3":{"bucket":{"name":"b"},"object":{"key":k,"size":10}}})
        })
        .collect();
    json!({ "Records": records })
}

fn sns_notification(event: &serde_json::Value) -> serde_json::Value {
    json!({"Type":"Notification","MessageId":"m","TopicArn":"arn:aws:sns:us-east-1:1:t",
           "Message": event.to_string()})
}

fn confirmation(url: &str) -> serde_json::Value {
    json!({"Type":"SubscriptionConfirmation","Token":"tok","SubscribeURL":url,
           "TopicArn":"arn:aws:sns:us-east-1:123456789012:buckwal-topic"})
}

#[test]
fn loopback_without_token_accepted() {
    let s = start(Opts::default());
    assert_eq!(s.post(None, &s3_event(&["log/1/00000001"])), 200);
    assert_eq!(s.wakes(), 1);
    s.stop();
}

#[test]
fn non_loopback_without_tls_and_token_refused_at_bind() {
    let dir = tempfile::tempdir().unwrap();
    let token = dir.path().join("token");
    std::fs::write(&token, TOKEN).unwrap();
    let tls = TlsFiles {
        cert: "c.pem".into(),
        key: "k.pem".into(),
    };
    for (tls, token) in [
        (None, None),
        (Some(tls.clone()), None),
        (None, Some(token.clone())),
    ] {
        let cfg = NotifyConfig {
            listen: "0.0.0.0:0".parse().unwrap(),
            tls,
            token_file: token,
        };
        let e = cfg.check_bind().unwrap_err();
        assert!(e.contains("BUCKWAL_REPLICA_NOTIFY_TLS_CERT"), "{e}");
        assert!(e.contains("BUCKWAL_REPLICA_NOTIFY_TOKEN_FILE"), "{e}");
        let cfg = NotifyConfig {
            listen: "0.0.0.0:0".parse().unwrap(),
            ..cfg
        };
        let r = serve_with(cfg, || {}, Recorder::default(), async {});
        assert!(
            r.is_err(),
            "bound a non-loopback address without TLS and token"
        );
    }
}

#[test]
fn config_from_lookup_rules() {
    let get = |pairs: &'static [(&'static str, &'static str)]| {
        move |k: &str| {
            pairs
                .iter()
                .find(|(n, _)| *n == k)
                .map(|(_, v)| v.to_string())
        }
    };
    assert!(NotifyConfig::from_lookup(get(&[])).unwrap().is_none());
    let empty = get(&[("BUCKWAL_REPLICA_NOTIFY_LISTEN", "")]);
    assert!(NotifyConfig::from_lookup(empty).unwrap().is_none());
    let c = NotifyConfig::from_lookup(get(&[
        ("BUCKWAL_REPLICA_NOTIFY_LISTEN", "127.0.0.1:9396"),
        ("BUCKWAL_REPLICA_NOTIFY_TOKEN_FILE", "/t"),
        ("BUCKWAL_REPLICA_NOTIFY_TLS_CERT", ""),
    ]))
    .unwrap()
    .unwrap();
    assert_eq!(c.listen.port(), 9396);
    assert!(c.tls.is_none());
    assert_eq!(c.token_file, Some(PathBuf::from("/t")));
    let e = NotifyConfig::from_lookup(get(&[
        ("BUCKWAL_REPLICA_NOTIFY_LISTEN", "127.0.0.1:1"),
        ("BUCKWAL_REPLICA_NOTIFY_TLS_CERT", "c.pem"),
    ]))
    .unwrap_err();
    assert!(e.contains("BUCKWAL_REPLICA_NOTIFY_TLS_KEY"), "{e}");
    let e = NotifyConfig::from_lookup(get(&[
        ("BUCKWAL_REPLICA_NOTIFY_LISTEN", "127.0.0.1:1"),
        ("BUCKWAL_REPLICA_NOTIFY_TLS_KEY", "k.pem"),
    ]))
    .unwrap_err();
    assert!(e.contains("BUCKWAL_REPLICA_NOTIFY_TLS_CERT"), "{e}");
    let e = NotifyConfig::from_lookup(get(&[("BUCKWAL_REPLICA_NOTIFY_LISTEN", "localhost:1")]))
        .unwrap_err();
    assert!(e.contains("BUCKWAL_REPLICA_NOTIFY_LISTEN"), "{e}");
}

#[test]
fn missing_token_401_and_no_wake() {
    let s = start(Opts {
        token: true,
        ..Opts::default()
    });
    let ev = s3_event(&["log/1/00000001"]);
    let (status, _, me) = s.raw("/notify", None, ev.to_string().as_bytes());
    assert_eq!(status, 401);
    // A confirmation without credentials fetches nothing either.
    let c = confirmation("https://sns.us-east-1.amazonaws.com/?Action=ConfirmSubscription");
    assert_eq!(s.post(None, &c), 401);
    assert_eq!(s.wakes(), 0);
    assert!(s.fetched().is_empty());
    let mine: Vec<Rec> = events("notification rejected")
        .into_iter()
        .filter(|r| r["remote"] == me.to_string())
        .collect();
    assert_eq!(mine.len(), 1, "{mine:?}");
    assert_eq!(mine[0]["reason"], "no credentials");
    assert_eq!(mine[0]["level"], "WARN");
    s.stop();
}

#[test]
fn wrong_token_401_and_no_wake() {
    let s = start(Opts {
        token: true,
        ..Opts::default()
    });
    let ev = s3_event(&["log/1/00000001"]).to_string();
    let wrong = [
        "Bearer wrong-token-0123456789abcdef".to_string(),
        format!("{}x", bearer()),
        basic("sns", "wrong-token-0123456789abcdef"),
        basic("other", TOKEN),
        "Basic !!!not-base64".to_string(),
        "Digest abc".to_string(),
        "Bearer".to_string(),
    ];
    for a in &wrong {
        let (status, _, me) = s.raw("/notify", Some(a), ev.as_bytes());
        assert_eq!(status, 401, "{a}");
        let mine: Vec<Rec> = events("notification rejected")
            .into_iter()
            .filter(|r| r["remote"] == me.to_string())
            .collect();
        assert_eq!(mine.len(), 1, "{a}: {mine:?}");
        assert_eq!(mine[0]["reason"], "wrong credentials", "{a}");
    }
    assert_eq!(s.wakes(), 0);
    s.stop();
}

#[test]
fn bearer_token_wakes() {
    let s = start(Opts {
        token: true,
        ..Opts::default()
    });
    let ev = s3_event(&["log/1/00000001"]);
    assert_eq!(s.post(Some(&bearer()), &ev), 200);
    assert_eq!(s.post(Some(&format!("bearer {TOKEN}")), &ev), 200);
    assert_eq!(s.wakes(), 2);
    s.stop();
}

#[test]
fn sns_basic_auth_wakes() {
    let s = start(Opts {
        token: true,
        ..Opts::default()
    });
    let ev = sns_notification(&s3_event(&["log/1/00000001"]));
    assert_eq!(s.post(Some(&basic("sns", TOKEN)), &ev), 200);
    assert_eq!(s.wakes(), 1);
    s.stop();
}

#[test]
fn token_never_in_logs() {
    let s = start(Opts {
        token: true,
        ..Opts::default()
    });
    let ev = s3_event(&["log/1/00000001"]);
    let guess = "guessed-secret-9f8e7d6c5b4a";
    s.post(Some(&bearer()), &ev);
    s.post(Some(&basic("sns", TOKEN)), &ev);
    s.post(Some(&format!("Bearer {guess}")), &ev);
    s.post(Some(&basic("sns", guess)), &ev);
    s.post(None, &ev);
    s.stop();
    let b64 = |t: &str| base64::engine::general_purpose::STANDARD.encode(t);
    let needles = [
        TOKEN.to_string(),
        guess.to_string(),
        b64(&format!("sns:{TOKEN}")),
        b64(&format!("sns:{guess}")),
        b64(TOKEN),
    ];
    let all = format!("{:?}", capture().0.lock().unwrap());
    for n in needles {
        assert!(!all.contains(&n), "{n} found in the logs");
    }
}

#[test]
fn s3_event_body_wakes_once() {
    let s = start(Opts::default());
    let ev = s3_event(&["log/1/00000001", "log/1/00000002", "head"]);
    assert_eq!(s.post(None, &ev), 200);
    assert_eq!(
        s.wakes(),
        1,
        "one request is one wake, whatever its record count"
    );
    // The key names are only a hint: a pages/ key is still an ObjectCreated event.
    assert_eq!(s.post(None, &s3_event(&["pages/1/x"])), 200);
    assert_eq!(s.wakes(), 2);
    s.stop();
}

#[test]
fn oversize_body_413_no_wake() {
    let s = start(Opts::default());
    let big = vec![b' '; 1024 * 1024 + 1];
    let (status, _, _) = s.raw("/notify", None, &big);
    assert_eq!(status, 413);
    assert_eq!(s.wakes(), 0);
    // Without a Content-Length the limit is enforced while reading.
    let mut c = TcpStream::connect(s.addr).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let _ = c.write_all(b"POST /notify HTTP/1.1\r\nHost: x\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n\r\n");
    let chunk = [b"10000\r\n".as_slice(), &vec![b' '; 0x10000], b"\r\n"].concat();
    for _ in 0..17 {
        if c.write_all(&chunk).is_err() {
            break;
        }
    }
    let _ = c.write_all(b"0\r\n\r\n");
    let mut out = String::new();
    let _ = c.read_to_string(&mut out);
    assert!(out.starts_with("HTTP/1.1 413"), "{out:?}");
    assert_eq!(s.wakes(), 0);
    // A body of exactly the limit is read (and then rejected as malformed).
    let mut ok = vec![b' '; 1024 * 1024];
    ok[0] = b'{';
    let (status, _, _) = s.raw("/notify", None, &ok);
    assert_eq!(status, 400);
    // Unauthenticated oversize bodies are refused before they are read.
    s.stop();
    let s = start(Opts {
        token: true,
        ..Opts::default()
    });
    let (status, _, _) = s.raw("/notify", None, &big);
    assert_eq!(status, 401);
    s.stop();
}

#[test]
fn malformed_body_400_no_wake() {
    let s = start(Opts::default());
    let bodies: Vec<Vec<u8>> = vec![
        b"".to_vec(),
        b"not json".to_vec(),
        b"[1,2]".to_vec(),
        b"null".to_vec(),
        b"{}".to_vec(),
        b"\xff\xfe".to_vec(),
        json!({"Records":[]}).to_string().into_bytes(),
        json!({"Records":"x"}).to_string().into_bytes(),
        json!({"Records":[{"eventName":"ObjectRemoved:Delete"}]})
            .to_string()
            .into_bytes(),
        json!({"Records":[{"eventName":5}]})
            .to_string()
            .into_bytes(),
        json!({"Type":"Notification"}).to_string().into_bytes(),
        json!({"Type":"Notification","Message":5})
            .to_string()
            .into_bytes(),
        json!({"Type":"Notification","Message":"not json"})
            .to_string()
            .into_bytes(),
        json!({"Type":"Notification","Message":"{\"Records\":[]}"})
            .to_string()
            .into_bytes(),
        // An SNS envelope inside an SNS envelope is not unwrapped twice.
        sns_notification(&sns_notification(&s3_event(&["log/1/1"])))
            .to_string()
            .into_bytes(),
        json!({"Type":"SubscriptionConfirmation"})
            .to_string()
            .into_bytes(),
        json!({"Type":"SubscriptionConfirmation","SubscribeURL":5})
            .to_string()
            .into_bytes(),
        json!({"Type":"UnsubscribeConfirmation","SubscribeURL":"x"})
            .to_string()
            .into_bytes(),
        json!({"Type":7}).to_string().into_bytes(),
    ];
    for b in &bodies {
        let (status, _, _) = s.raw("/notify", None, b);
        assert_eq!(status, 400, "{}", String::from_utf8_lossy(b));
    }
    assert_eq!(s.wakes(), 0);
    assert!(s.fetched().is_empty());
    // The listener still works afterwards.
    assert_eq!(s.post(None, &s3_event(&["head"])), 200);
    // Other routes and methods are not the webhook.
    let (status, _, _) = s.raw("/other", None, b"{}");
    assert_eq!(status, 404);
    s.stop();
}

#[test]
fn sns_confirmation_off_host_refused() {
    let s = start(Opts::default());
    let urls = [
        "https://evil.example/?Action=ConfirmSubscription",
        "https://sns.us-east-1.amazonaws.com.evil.example/x",
        "https://sns.us-east-1.amazonaws.com@evil.example/x",
        "https://evil.example@sns.us-east-1.amazonaws.com/x",
        "https://evil.example\\@sns.us-east-1.amazonaws.com/x",
        "https://evil.example#@sns.us-east-1.amazonaws.com/x",
        "https://evil.example/@sns.us-east-1.amazonaws.com/x",
        "https://sns.us-east-1.amazonaws.com%2f@evil.example/x",
        "https://sns.us-east-1.amazonaws.com:8443/x",
        "https://sns.us-east-1.amazonaws.com:@evil.example/x",
        "http://sns.us-east-1.amazonaws.com/x",
        "ftp://sns.us-east-1.amazonaws.com/x",
        "//sns.us-east-1.amazonaws.com/x",
        "sns.us-east-1.amazonaws.com/x",
        "https://xsns.us-east-1.amazonaws.com/x",
        "https://sns..amazonaws.com/x",
        "https://sns.us.east.amazonaws.com/x",
        "https://sns.us-east-1.amazonaws.com.cn/x",
        "https://sns.us-east-1.amazonaws.com./x",
        "https://sns.us-east-1.amazonaws.com\n.evil.example/x",
        "https://evil.example/x https://sns.us-east-1.amazonaws.com/",
        "https://127.0.0.1/x",
        "https://[::1]/x",
        "",
    ];
    for u in urls {
        let before = events("sns confirmation refused").len();
        let (status, _, _) = s.raw("/notify", None, confirmation(u).to_string().as_bytes());
        assert_eq!(status, 400, "{u:?}");
        assert!(
            events("sns confirmation refused").len() > before,
            "{u:?} not logged"
        );
    }
    assert!(s.fetched().is_empty(), "fetched {:?}", s.fetched());
    assert_eq!(s.wakes(), 0);
    let refused = events("sns confirmation refused");
    assert!(refused
        .iter()
        .all(|r| r["level"] == "WARN" && r.contains_key("reason")));
    s.stop();
}

#[test]
fn sns_confirmation_on_host_fetches() {
    let s = start(Opts {
        token: true,
        ..Opts::default()
    });
    let url = "https://sns.us-east-1.amazonaws.com/?Action=ConfirmSubscription&TopicArn=arn:aws:sns:us-east-1:123456789012:buckwal-topic&Token=abc";
    assert_eq!(s.post(Some(&basic("sns", TOKEN)), &confirmation(url)), 200);
    assert_eq!(s.fetched(), vec![url.to_string()]);
    assert_eq!(s.wakes(), 0, "a confirmation is not a bucket event");
    let ok: Vec<Rec> = events("sns subscription confirmed")
        .into_iter()
        .filter(|r| r.values().any(|v| v.contains("buckwal-topic")))
        .collect();
    assert_eq!(ok.len(), 1, "{ok:?}");
    assert_eq!(ok[0]["level"], "INFO");
    let eu = "https://sns.eu-west-2.amazonaws.com/?Action=ConfirmSubscription&Token=t2";
    assert_eq!(s.post(Some(&bearer()), &confirmation(eu)), 200);
    assert_eq!(s.fetched().len(), 2);
    s.stop();
}

#[test]
fn sns_confirmation_fetch_failure_is_502_not_a_panic() {
    let s = start(Opts {
        fail_fetch: true,
        ..Opts::default()
    });
    let url = "https://sns.us-east-1.amazonaws.com/?Action=ConfirmSubscription&Token=t";
    assert_eq!(s.post(None, &confirmation(url)), 502);
    assert_eq!(s.fetched().len(), 1);
    assert_eq!(s.post(None, &s3_event(&["head"])), 200, "still serving");
    s.stop();
}

#[test]
fn https_with_token_wakes() {
    let s = start(Opts {
        token: true,
        tls: true,
        ..Opts::default()
    });
    let _ = rustls::crypto::ring::default_provider().install_default();
    let ca = std::fs::read(s.ca.as_ref().unwrap()).unwrap();
    let client = reqwest::blocking::Client::builder()
        .no_proxy()
        .add_root_certificate(reqwest::Certificate::from_pem(&ca).unwrap())
        .build()
        .unwrap();
    let url = format!("https://localhost:{}/notify", s.addr.port());
    let body = sns_notification(&s3_event(&["log/1/00000001"])).to_string();
    let r = client.post(&url).body(body.clone()).send().unwrap();
    assert_eq!(r.status().as_u16(), 401);
    let r = client
        .post(&url)
        .basic_auth("sns", Some(TOKEN))
        .body(body.clone())
        .send()
        .unwrap();
    assert_eq!(r.status().as_u16(), 200);
    let r = client
        .post(&url)
        .bearer_auth(TOKEN)
        .body(body)
        .send()
        .unwrap();
    assert_eq!(r.status().as_u16(), 200);
    assert_eq!(s.wakes(), 2);
    // Plain HTTP is not served on a TLS listener.
    let (status, _, _) = s.raw("/notify", Some(&bearer()), b"{}");
    assert_ne!(status, 200);
    assert_eq!(s.wakes(), 2);
    s.stop();
}

/// The real fetch, on a blocking thread of a runtime as the listener runs it: 2xx is `Ok`, any
/// other answer (a redirect included, which is not followed) is an error without the URL.
#[test]
fn http_fetch_checks_status_and_does_not_follow_redirects() {
    use buckwal_server::notify::HttpFetch;
    let serve = |answer: &'static str| {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = l.local_addr().unwrap().port();
        std::thread::spawn(move || {
            if let Ok((mut c, _)) = l.accept() {
                let mut buf = [0u8; 1024];
                let _ = c.read(&mut buf);
                let _ = c.write_all(answer.as_bytes());
            }
        });
        format!("http://127.0.0.1:{port}/confirm?Token=SECRET-TOKEN-ABC")
    };
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let get = |url: String| {
        rt.block_on(async move {
            tokio::task::spawn_blocking(move || HttpFetch.get(&url))
                .await
                .unwrap()
        })
    };
    assert_eq!(
        get(serve(
            "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        )),
        Ok(())
    );
    let e = get(serve(
        "HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:1/x\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
    ))
    .unwrap_err();
    assert!(e.contains("302"), "{e}");
    assert!(!e.contains("SECRET-TOKEN-ABC"), "{e}");
    let e = get("http://127.0.0.1:1/confirm?Token=SECRET-TOKEN-ABC".to_string()).unwrap_err();
    assert!(!e.contains("SECRET-TOKEN-ABC"), "{e}");
}

#[test]
fn s3_test_event_is_acknowledged_without_a_wake() {
    let s = start(Opts::default());
    let test = json!({"Service":"Amazon S3","Event":"s3:TestEvent","Bucket":"b","RequestId":"r"});
    assert_eq!(s.post(None, &test), 200);
    assert_eq!(s.post(None, &sns_notification(&test)), 200);
    assert_eq!(s.wakes(), 0);
    // Other events that are not ObjectCreated stay refused.
    let other = json!({"Event":"s3:Other"});
    assert_eq!(s.post(None, &other), 400);
    s.stop();
}

#[test]
fn a_400_is_logged_at_debug_with_its_reason_and_not_the_body() {
    let s = start(Opts::default());
    let secret = "BODY-SECRET-77aa";
    let (status, _, _) = s.raw(
        "/notify",
        None,
        format!("{{\"x\":\"{secret}\"}}").as_bytes(),
    );
    assert_eq!(status, 400);
    let all = capture().0.lock().unwrap().clone();
    let mine: Vec<&Rec> = all
        .iter()
        .filter(|r| {
            r.get("message").map(String::as_str) == Some("notification request refused with 400")
        })
        .collect();
    assert!(
        mine.iter()
            .any(|r| r["reason"] == "not an S3 ObjectCreated event"),
        "{mine:?}"
    );
    assert!(!format!("{all:?}").contains(secret));
    s.stop();
}

#[test]
fn a_client_that_leaves_mid_body_wakes_nothing_and_changes_nothing() {
    let s = start(Opts::default());
    let mut c = TcpStream::connect(s.addr).unwrap();
    let _ = c.write_all(
        b"POST /notify HTTP/1.1\r\nHost: x\r\nContent-Length: 5000\r\n\r\n{\"Records\":",
    );
    drop(c);
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(s.wakes(), 0);
    assert_eq!(s.post(None, &s3_event(&["head"])), 200, "still serving");
    s.stop();
}
