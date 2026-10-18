//! Shared helpers for the HTTP tests: a server on `127.0.0.1:0` over a `MemStore`, run by
//! `bind(..).serve(..)` on its own thread and stopped through a oneshot channel.
#![allow(dead_code)]
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{mpsc, Arc};
use std::thread::JoinHandle;
use std::time::Duration;
use tempfile::TempDir;
use turso_durable::store::{MemStore, ObjectStore};
use turso_durable::{open_with_store, Config};
use turso_durable_hrana_client::Client;
use turso_durable_server::{bind, ServeConfig, ShutdownReport};

/// The bearer token of servers started with `Opts::token`.
pub const TOKEN: &str = "test-token-0123456789abcdef";

/// The database config: short timeouts so a test never waits long on a fault.
pub fn db_config(dir: &Path) -> Config {
    let mut c = Config::new(dir, "http://127.0.0.1:1", "b", "k", "s");
    c.retry_deadline = Duration::from_secs(2);
    c.request_timeout = Duration::from_millis(500);
    c.lease = Duration::from_secs(5);
    c
}

pub struct Opts {
    /// Require `TOKEN`.
    pub token: bool,
    pub max_streams: usize,
    pub stream_timeout: Duration,
    /// `open` waits for a message on this before opening; `start` then returns at once
    /// instead of waiting for ready.
    pub gate: Option<mpsc::Receiver<()>>,
    /// `open` returns this error instead of opening.
    pub open_error: Option<String>,
}

impl Default for Opts {
    fn default() -> Self {
        Opts {
            token: false,
            max_streams: 256,
            stream_timeout: Duration::from_secs(10),
            gate: None,
            open_error: None,
        }
    }
}

pub struct Server {
    pub addr: SocketAddr,
    pub store: Arc<MemStore>,
    pub dir: TempDir,
    /// One message when `on_ready` ran.
    pub ready: mpsc::Receiver<()>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    handle: Option<JoinHandle<Result<ShutdownReport, String>>>,
}

pub fn start(opts: Opts) -> Server {
    // reqwest's rustls backend needs a process crypto provider; an earlier install wins.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(MemStore::new());
    let mut cfg = ServeConfig::from_lookup(Some("127.0.0.1:0"), |_| None).unwrap();
    cfg.max_streams = opts.max_streams;
    cfg.stream_timeout = opts.stream_timeout;
    if opts.token {
        let p = dir.path().join("token");
        std::fs::write(&p, format!("{TOKEN}\n")).unwrap();
        cfg.token_file = Some(p);
    }
    let bound = bind(cfg).unwrap();
    let addr = bound.local_addr();
    assert!(!bound.tls());
    assert_eq!(bound.auth(), opts.token);
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let (ready_tx, ready_rx) = mpsc::channel();
    let db_dir = dir.path().join("db");
    let db_store: Arc<dyn ObjectStore> = store.clone();
    let gate = opts.gate;
    let open_error = opts.open_error;
    let wait_ready = gate.is_none() && open_error.is_none();
    let handle = std::thread::spawn(move || {
        bound.serve(
            move || {
                if let Some(g) = gate {
                    let _ = g.recv();
                }
                if let Some(e) = open_error {
                    return Err(e);
                }
                open_with_store(db_config(&db_dir), db_store).map_err(|e| e.to_string())
            },
            move |_db| {
                let _ = ready_tx.send(());
            },
            async move {
                let _ = stop_rx.await;
            },
        )
    });
    let s = Server {
        addr,
        store,
        dir,
        ready: ready_rx,
        stop: Some(stop_tx),
        handle: Some(handle),
    };
    if wait_ready {
        s.wait_ready();
    }
    s
}

impl Server {
    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub fn client(&self, token: Option<&str>) -> Client {
        Client::new(&self.url(), token, None).unwrap()
    }

    pub fn wait_ready(&self) {
        self.ready
            .recv_timeout(Duration::from_secs(30))
            .expect("server did not become ready");
    }

    /// Keys in the bucket that hold commit log records.
    pub fn log_keys(&self) -> usize {
        self.store
            .list("")
            .unwrap()
            .iter()
            .filter(|k| k.contains("log/"))
            .count()
    }

    /// Resolve the shutdown future without waiting.
    pub fn request_stop(&mut self) {
        if let Some(tx) = self.stop.take() {
            let _ = tx.send(());
        }
    }

    /// Resolve the shutdown future and wait for `serve` to return.
    pub fn stop(mut self) -> Result<ShutdownReport, String> {
        self.request_stop();
        self.handle.take().unwrap().join().expect("serve panicked")
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(tx) = self.stop.take() {
            let _ = tx.send(());
        }
        // A failed test may still hold a gate `open` waits on; joining would hang.
        if std::thread::panicking() {
            return;
        }
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}
