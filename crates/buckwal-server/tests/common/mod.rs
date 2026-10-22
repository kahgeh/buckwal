//! Shared helpers for the HTTP tests: a server on `127.0.0.1:0` over a `MemStore` (or a
//! `FaultStore` over it), run by `bind(..).serve(..)` on its own thread and stopped through a
//! oneshot channel.
#![allow(dead_code)]
use buckwal::store::{FaultStore, MemStore, ObjectStore};
use buckwal::{open_read_only_with_store, open_with_store, restore_with_store, Config};
use buckwal_hrana_client::Client;
use buckwal_server::{bind, ServeConfig, ShutdownReport, TlsFiles};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tempfile::TempDir;

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

/// Held for writing while a child process starts, for reading while a server binds. On
/// macOS std creates a socket and only then marks it close-on-exec, so a child spawned in
/// between by a test running in parallel would inherit the listener and keep its port open
/// after `serve` returned.
static SPAWN: RwLock<()> = RwLock::new(());

/// Run `scripts/tls-material.sh` into `dir/m` and return that directory, or `None` (with
/// the reason printed) when openssl is not installed.
pub fn tls_material(dir: &Path) -> Option<PathBuf> {
    let has_openssl = {
        let _spawning = SPAWN.write().unwrap_or_else(|e| e.into_inner());
        std::process::Command::new("sh")
            .args(["-c", "command -v openssl"])
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    };
    if !has_openssl {
        eprintln!("skipped: openssl is not installed, so no TLS material can be made");
        return None;
    }
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/tls-material.sh");
    let out = dir.join("m");
    let o = {
        let _spawning = SPAWN.write().unwrap_or_else(|e| e.into_inner());
        std::process::Command::new("bash")
            .arg(&script)
            .arg(&out)
            .output()
            .unwrap()
    };
    assert!(
        o.status.success(),
        "tls-material.sh failed: {}",
        String::from_utf8_lossy(&o.stderr)
    );
    Some(out)
}

/// Restore `store` into a new directory and read the integer column `col` of `table`.
pub fn restored(store: &Arc<MemStore>, table: &str, col: &str) -> Vec<i64> {
    let dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = store.clone();
    restore_with_store(&db_config(dir.path()), store.clone(), dir.path()).unwrap();
    let db = open_read_only_with_store(db_config(dir.path()), store).unwrap();
    let c = db.connect().unwrap();
    let rows = c
        .query(&format!("SELECT {col} FROM {table} ORDER BY {col}"), &[])
        .unwrap();
    let ids = rows.iter().map(|r| r[0].as_int().unwrap()).collect();
    drop(c);
    db.close().unwrap();
    ids
}

pub struct Opts {
    /// Require `TOKEN`.
    pub token: bool,
    pub max_streams: usize,
    pub stream_timeout: Duration,
    pub shutdown_timeout: Duration,
    /// `open` waits for a message on this before opening; `start` then returns at once
    /// instead of waiting for ready.
    pub gate: Option<mpsc::Receiver<()>>,
    /// `open` returns this error instead of opening.
    pub open_error: Option<String>,
    /// The database writes through a `FaultStore` over the `MemStore` (`Server::fault`).
    pub fault: bool,
    /// Serve HTTPS with the material `tls_material` made in this directory.
    pub tls: Option<PathBuf>,
}

impl Default for Opts {
    fn default() -> Self {
        Opts {
            token: false,
            max_streams: 256,
            stream_timeout: Duration::from_secs(10),
            shutdown_timeout: Duration::from_secs(10),
            gate: None,
            open_error: None,
            fault: false,
            tls: None,
        }
    }
}

pub struct Server {
    pub addr: SocketAddr,
    pub store: Arc<MemStore>,
    /// Set with `Opts::fault`: the store the database writes through.
    pub fault: Option<Arc<FaultStore>>,
    pub dir: TempDir,
    /// One message when `on_ready` ran.
    pub ready: mpsc::Receiver<()>,
    /// The CA that signed the server certificate (with `Opts::tls`).
    ca: Option<PathBuf>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    handle: Option<JoinHandle<Result<ShutdownReport, String>>>,
}

pub fn start(opts: Opts) -> Server {
    // reqwest's rustls backend needs a process crypto provider; an earlier install wins.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(MemStore::new());
    let fault = opts.fault.then(|| Arc::new(FaultStore::new(store.clone())));
    let mut cfg = ServeConfig::from_lookup(Some("127.0.0.1:0"), |_| None).unwrap();
    cfg.max_streams = opts.max_streams;
    cfg.stream_timeout = opts.stream_timeout;
    cfg.shutdown_timeout = opts.shutdown_timeout;
    if opts.token {
        let p = dir.path().join("token");
        std::fs::write(&p, format!("{TOKEN}\n")).unwrap();
        cfg.token_file = Some(p);
    }
    let ca = opts.tls.as_ref().map(|m| m.join("ca.pem"));
    cfg.tls = opts.tls.as_ref().map(|m| TlsFiles {
        cert: m.join("server/rustfs_cert.pem"),
        key: m.join("server/rustfs_key.pem"),
    });
    let bound = {
        let _binding = SPAWN.read().unwrap_or_else(|e| e.into_inner());
        bind(cfg).unwrap()
    };
    let addr = bound.local_addr();
    assert_eq!(bound.tls(), opts.tls.is_some());
    assert_eq!(bound.auth(), opts.token);
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let (ready_tx, ready_rx) = mpsc::channel();
    let db_dir = dir.path().join("db");
    let db_store: Arc<dyn ObjectStore> = match &fault {
        Some(f) => f.clone(),
        None => store.clone(),
    };
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
        fault,
        dir,
        ready: ready_rx,
        ca,
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
        match self.ca {
            Some(_) => format!("https://localhost:{}", self.addr.port()),
            None => format!("http://{}", self.addr),
        }
    }

    pub fn client(&self, token: Option<&str>) -> Client {
        Client::new(&self.url(), token, self.ca.as_deref()).unwrap()
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

    /// The `FaultStore`'s puts (of any flavour) of commit log records so far.
    pub fn log_puts(&self) -> usize {
        self.fault
            .as_ref()
            .expect("started with Opts::fault")
            .events()
            .iter()
            .filter(|e| e.starts_with("put") && e.contains("log/"))
            .count()
    }

    /// Wait until more than `seen` log puts started (a commit is uploading).
    pub fn wait_log_put(&self, seen: usize) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while self.log_puts() <= seen {
            assert!(Instant::now() < deadline, "no commit started uploading");
            std::thread::sleep(Duration::from_millis(2));
        }
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
