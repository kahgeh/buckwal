//! The listener: TLS setup ([`tls_acceptor`]) and the accept loop. Each TCP connection (after
//! its TLS handshake, when serving HTTPS) is served by hyper's HTTP/1 server with the axum
//! router, watched by a [`GracefulShutdown`] and tracked in a [`JoinSet`], so shutdown can
//! wait for open connections and abort them past its bound.
use crate::config::TlsFiles;
use axum::Router;
use hyper::server::conn::http1;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::server::graceful::{GracefulShutdown, Watcher};
use hyper_util::service::TowerToHyperService;
use rustls::pki_types::pem::{self, PemObject};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::future::Future;
use std::path::Path;
use std::pin::{pin, Pin};
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{watch, Semaphore};
use tokio::task::JoinSet;
use tokio_rustls::TlsAcceptor;

/// The most connections served at once. Each costs a task, buffers and a file descriptor, so
/// the cap keeps a flood of clients from exhausting them. Past it the accept loop waits for
/// a connection to close; new ones queue in the kernel's listen backlog meanwhile.
pub(crate) const MAX_CONNECTIONS: usize = 1024;

/// How long a client may take to finish its TLS handshake.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Which of two futures finished first.
pub(crate) enum Either<A, B> {
    Left(A),
    Right(B),
}

/// Wait for the first of `a` and `b` (`a` is polled first). The other is left as it is.
pub(crate) async fn race<A, B>(mut a: A, mut b: B) -> Either<A::Output, B::Output>
where
    A: Future + Unpin,
    B: Future + Unpin,
{
    std::future::poll_fn(|cx| {
        if let Poll::Ready(x) = Pin::new(&mut a).poll(cx) {
            return Poll::Ready(Either::Left(x));
        }
        if let Poll::Ready(x) = Pin::new(&mut b).poll(cx) {
            return Poll::Ready(Either::Right(x));
        }
        Poll::Pending
    })
    .await
}

/// A PEM error as text that never quotes the file: some variants carry a line or base64
/// detail from it, and a key file's content is secret.
fn pem_problem(e: &pem::Error) -> String {
    match e {
        pem::Error::Io(io) => format!("cannot read: {}", io.kind()),
        pem::Error::NoItemsFound => "no PEM item found".to_string(),
        _ => "not a valid PEM file".to_string(),
    }
}

/// Load the PEM certificate chain and private key, check that they match, and build the
/// acceptor: rustls with ring, TLS 1.3 and 1.2, ALPN `http/1.1`. Errors name the file at
/// fault and never hold key material.
pub(crate) fn tls_acceptor(files: &TlsFiles) -> Result<TlsAcceptor, String> {
    let (cert_path, key_path) = (files.cert.display(), files.key.display());
    let certs = load_certs(&files.cert)?;
    let key = PrivateKeyDer::from_pem_file(&files.key).map_err(|e| match e {
        pem::Error::NoItemsFound => {
            format!("DURABLE_SERVE_TLS_KEY {key_path}: no PEM private key in the file")
        }
        e => format!("DURABLE_SERVE_TLS_KEY {key_path}: {}", pem_problem(&e)),
    })?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut config = rustls::ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
        .map_err(|e| format!("cannot set up TLS: {e}"))?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| match e {
            rustls::Error::InconsistentKeys(_) => format!(
                "DURABLE_SERVE_TLS_KEY {key_path}: does not match the certificate in \
                 DURABLE_SERVE_TLS_CERT {cert_path}"
            ),
            // rustls errors describe the key's kind, never its bytes.
            e => format!(
                "DURABLE_SERVE_TLS_CERT {cert_path} and DURABLE_SERVE_TLS_KEY {key_path}: {e}"
            ),
        })?;
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(TlsAcceptor::from(Arc::new(config)))
}

fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, String> {
    let shown = path.display();
    let certs = CertificateDer::pem_file_iter(path)
        .and_then(|it| it.collect::<Result<Vec<_>, _>>())
        .map_err(|e| format!("DURABLE_SERVE_TLS_CERT {shown}: {}", pem_problem(&e)))?;
    if certs.is_empty() {
        return Err(format!(
            "DURABLE_SERVE_TLS_CERT {shown}: no PEM certificate in the file"
        ));
    }
    Ok(certs)
}

/// The connections the accept loop started.
pub(crate) struct Conns {
    graceful: Option<GracefulShutdown>,
    tasks: JoinSet<()>,
}

impl Conns {
    /// Tell every connection to finish its current request and close, then wait for all of
    /// them. Dropping this future part way (at the shutdown bound) leaves the rest to
    /// [`Conns::abort`].
    pub(crate) async fn finish(&mut self) {
        if let Some(g) = self.graceful.take() {
            g.shutdown().await;
        }
        while self.tasks.join_next().await.is_some() {}
    }

    /// Abort every connection still open and wait until each task is gone. An aborted
    /// connection drops its handler, so a response not yet written is never sent.
    pub(crate) async fn abort(&mut self) {
        self.tasks.abort_all();
        while self.tasks.join_next().await.is_some() {}
    }
}

/// Accept connections until `stop` turns true (or its sender is dropped), serving at most
/// `max_conns` at once, over TLS when `tls` is set.
pub(crate) async fn accept_loop(
    listener: TcpListener,
    app: Router,
    tls: Option<TlsAcceptor>,
    max_conns: usize,
    stop: watch::Receiver<bool>,
) -> Conns {
    let graceful = GracefulShutdown::new();
    let mut tasks = JoinSet::new();
    let slots = Arc::new(Semaphore::new(max_conns.max(1)));
    let mut stop_loop = stop.clone();
    let mut stopped = pin!(stop_loop.wait_for(|s| *s));
    loop {
        // Finished connections leave the set as they go, so it holds only live ones.
        while tasks.try_join_next().is_some() {}
        let slot = match race(pin!(slots.clone().acquire_owned()), stopped.as_mut()).await {
            Either::Left(Ok(slot)) => slot,
            Either::Left(Err(_)) | Either::Right(_) => break,
        };
        let accepted = match race(pin!(listener.accept()), stopped.as_mut()).await {
            Either::Left(a) => a,
            Either::Right(_) => break,
        };
        let tcp = match accepted {
            Ok((tcp, _peer)) => tcp,
            Err(e) => {
                // Usually out of file descriptors; back off instead of spinning.
                tracing::warn!(target: "turso_durable::server", error = %e, "accept failed");
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        let conn = serve_tcp(
            tcp,
            app.clone(),
            tls.clone(),
            graceful.watcher(),
            stop.clone(),
        );
        tasks.spawn(async move {
            let _slot = slot;
            conn.await;
        });
    }
    Conns {
        graceful: Some(graceful),
        tasks,
    }
}

/// Serve one TCP connection: the TLS handshake first when `tls` is set (given up after
/// [`HANDSHAKE_TIMEOUT`] or when shutdown begins), then HTTP/1.
async fn serve_tcp(
    tcp: TcpStream,
    app: Router,
    tls: Option<TlsAcceptor>,
    watcher: Watcher,
    mut stop: watch::Receiver<bool>,
) {
    let _ = tcp.set_nodelay(true);
    let Some(tls) = tls else {
        return serve_http(tcp, app, watcher).await;
    };
    let handshake = tokio::time::timeout(HANDSHAKE_TIMEOUT, tls.accept(tcp));
    let stopping = async {
        let _ = stop.wait_for(|s| *s).await;
    };
    let stream = match race(pin!(handshake), pin!(stopping)).await {
        Either::Left(Ok(Ok(stream))) => stream,
        Either::Left(Ok(Err(e))) => {
            tracing::debug!(target: "turso_durable::server", error = %e, "TLS handshake failed");
            return;
        }
        Either::Left(Err(_)) => {
            tracing::debug!(target: "turso_durable::server", "TLS handshake timed out");
            return;
        }
        Either::Right(()) => return,
    };
    serve_http(stream, app, watcher).await
}

async fn serve_http<I>(io: I, app: Router, watcher: Watcher)
where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let conn = http1::Builder::new()
        .timer(TokioTimer::new())
        .serve_connection(TokioIo::new(io), TowerToHyperService::new(app));
    if let Err(e) = watcher.watch(conn).await {
        tracing::debug!(target: "turso_durable::server", error = %e, "connection ended");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::{self, App, BODY_READ_TIMEOUT};
    use crate::streams::{Streams, StreamsCfg};
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpStream};
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::Instant;
    use turso_durable::store::MemStore;

    /// Run `scripts/tls-material.sh` into `dir/name`, or `None` (reason printed) without
    /// openssl.
    fn tls_material(dir: &Path, name: &str) -> Option<PathBuf> {
        let has_openssl = std::process::Command::new("sh")
            .args(["-c", "command -v openssl"])
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);
        if !has_openssl {
            eprintln!("skipped: openssl is not installed, so no TLS material can be made");
            return None;
        }
        let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/tls-material.sh");
        let out = dir.join(name);
        let o = std::process::Command::new("bash")
            .arg(&script)
            .arg(&out)
            .output()
            .unwrap();
        assert!(
            o.status.success(),
            "tls-material.sh failed: {}",
            String::from_utf8_lossy(&o.stderr)
        );
        Some(out)
    }

    fn acceptor_error(cert: &Path, key: &Path) -> String {
        let files = TlsFiles {
            cert: cert.into(),
            key: key.into(),
        };
        match tls_acceptor(&files) {
            Ok(_) => panic!("{} with {} was accepted", cert.display(), key.display()),
            Err(e) => e,
        }
    }

    #[test]
    fn tls_files_are_checked() {
        let tmp = tempfile::tempdir().unwrap();
        let (Some(m), Some(other)) = (
            tls_material(tmp.path(), "m"),
            tls_material(tmp.path(), "other"),
        ) else {
            return;
        };
        let cert = m.join("server/rustfs_cert.pem");
        let key = m.join("server/rustfs_key.pem");
        let ok = TlsFiles {
            cert: cert.clone(),
            key: key.clone(),
        };
        assert!(tls_acceptor(&ok).is_ok());
        let fake = tmp.path().join("fake_key.pem");
        std::fs::write(
            &fake,
            "-----BEGIN PRIVATE KEY-----\nSECRETKEYBYTES!!\n-----END PRIVATE KEY-----\n",
        )
        .unwrap();
        let mut secrets: Vec<String> = std::fs::read_to_string(&key)
            .unwrap()
            .lines()
            .filter(|l| !l.starts_with("-----"))
            .map(str::to_string)
            .collect();
        secrets.push("SECRETKEYBYTES".to_string());
        let missing = tmp.path().join("missing.pem");
        let other_key = other.join("server/rustfs_key.pem");
        let ca = m.join("ca.pem");
        for (cert, key, names, says) in [
            (&missing, &key, &missing, "DURABLE_SERVE_TLS_CERT"),
            (&key, &key, &key, "no PEM certificate"),
            (&cert, &missing, &missing, "DURABLE_SERVE_TLS_KEY"),
            (&cert, &ca, &ca, "no PEM private key"),
            (&cert, &fake, &fake, "DURABLE_SERVE_TLS_KEY"),
            (&cert, &other_key, &other_key, "does not match"),
        ] {
            let e = acceptor_error(cert, key);
            assert!(e.contains(&names.display().to_string()), "{e}");
            assert!(e.contains(says), "{e}");
            for s in &secrets {
                assert!(!e.contains(s.as_str()), "the error holds key material: {e}");
            }
        }
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
    }

    struct Served {
        rt: tokio::runtime::Runtime,
        addr: SocketAddr,
        stop: watch::Sender<bool>,
        conns: tokio::task::JoinHandle<Conns>,
    }

    impl Served {
        fn new(app: Arc<App>, max_conns: usize) -> Served {
            let rt = runtime();
            let listener = rt.block_on(TcpListener::bind("127.0.0.1:0")).unwrap();
            let addr = listener.local_addr().unwrap();
            let (stop, rx) = watch::channel(false);
            let conns = rt.spawn(accept_loop(
                listener,
                http::router(app),
                None,
                max_conns,
                rx,
            ));
            Served {
                rt,
                addr,
                stop,
                conns,
            }
        }

        fn stop(self) {
            let _ = self.stop.send(true);
            self.rt.block_on(async {
                let mut conns = self.conns.await.unwrap();
                conns.finish().await;
            });
        }
    }

    /// Read one HTTP/1.1 response (headers and a Content-Length body) within `wait`, or
    /// `None` on timeout or EOF.
    fn read_response(tcp: &mut TcpStream, wait: Duration) -> Option<String> {
        let deadline = Instant::now() + wait;
        let mut buf = Vec::new();
        loop {
            let left = deadline.checked_duration_since(Instant::now())?;
            tcp.set_read_timeout(Some(left.max(Duration::from_millis(1))))
                .unwrap();
            let mut chunk = [0u8; 4096];
            let n = tcp.read(&mut chunk).ok()?;
            if n == 0 {
                return None;
            }
            buf.extend_from_slice(&chunk[..n]);
            let text = String::from_utf8_lossy(&buf).to_string();
            let Some(end) = text.find("\r\n\r\n") else {
                continue;
            };
            let len = text[..end]
                .lines()
                .find_map(|l| {
                    let (k, v) = l.split_once(':')?;
                    k.eq_ignore_ascii_case("content-length")
                        .then(|| v.trim().parse::<usize>().ok())?
                })
                .unwrap_or(0);
            if buf.len() >= end + 4 + len {
                return Some(text);
            }
        }
    }

    const HEALTH: &[u8] = b"GET /health HTTP/1.1\r\nHost: x\r\n\r\n";

    #[test]
    fn connections_past_the_cap_wait_for_a_free_one() {
        let s = Served::new(App::new(None, 1, BODY_READ_TIMEOUT), 1);
        let mut first = TcpStream::connect(s.addr).unwrap();
        first.write_all(HEALTH).unwrap();
        let r = read_response(&mut first, Duration::from_secs(5)).expect("first is served");
        assert!(r.contains("starting"), "{r}");
        // `first` stays open (keep-alive), so it holds the only connection.
        let mut second = TcpStream::connect(s.addr).unwrap();
        second.write_all(HEALTH).unwrap();
        assert_eq!(
            read_response(&mut second, Duration::from_millis(200)),
            None,
            "the second connection waits"
        );
        drop(first);
        let r = read_response(&mut second, Duration::from_secs(5)).expect("then it is served");
        assert!(r.contains("starting"), "{r}");
        drop(second);
        s.stop();
    }

    fn ready_app(body_timeout: Duration) -> (tempfile::TempDir, Arc<Streams>, Arc<App>) {
        let dir = tempfile::tempdir().unwrap();
        let mut c =
            turso_durable::Config::new(dir.path().join("db"), "http://127.0.0.1:1", "b", "k", "s");
        c.retry_deadline = Duration::from_secs(2);
        c.request_timeout = Duration::from_millis(500);
        c.lease = Duration::from_secs(5);
        let db = turso_durable::open_with_store(c, Arc::new(MemStore::new())).unwrap();
        let streams = Arc::new(
            Streams::new(
                db,
                StreamsCfg {
                    timeout: Duration::from_secs(10),
                    busy: Duration::from_secs(1),
                    max: 4,
                },
            )
            .unwrap(),
        );
        let app = App::new(None, 2, body_timeout);
        app.set_ready(streams.clone());
        (dir, streams, app)
    }

    #[test]
    fn a_slow_body_is_408_and_the_connection_closes() {
        let (_dir, streams, app) = ready_app(Duration::from_millis(200));
        let s = Served::new(app, 4);
        let mut tcp = TcpStream::connect(s.addr).unwrap();
        tcp.write_all(
            b"POST /v2/pipeline HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\n\r\n{\"baton\"",
        )
        .unwrap();
        let r = read_response(&mut tcp, Duration::from_secs(5)).expect("a response in time");
        assert!(r.starts_with("HTTP/1.1 408"), "{r}");
        assert!(r.contains(r#""code":"PROTO_ERROR""#), "{r}");
        assert_eq!(
            read_response(&mut tcp, Duration::from_secs(5)),
            None,
            "the server closed the connection"
        );
        s.stop();
        assert_eq!(streams.close_all(), 0);
        streams.database().clone().close().unwrap();
    }
}
