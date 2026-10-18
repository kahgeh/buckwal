//! Hrana-over-HTTP server for `turso_durable` databases.
//!
//! [`bind`] takes the port and loads the token, so a bad setting fails before the database
//! is opened. [`Bound::serve`] then runs the server on its own tokio runtime: `/health`
//! answers `starting` at once, the database opens on a blocking thread, and pipelines are
//! served from then until the `shutdown` future resolves.
pub mod config;
pub mod exec;
mod http;
mod listener;
mod proto;
pub mod streams;

pub use config::{ServeConfig, TlsFiles, Token};

use crate::http::App;
use crate::listener::{race, Either};
use crate::streams::{Streams, StreamsCfg};
use std::future::Future;
use std::net::SocketAddr;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use turso_durable::Database;

/// Tokio worker threads: they only do I/O, the statements run on blocking threads.
const IO_WORKERS: usize = 2;

/// How serving ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShutdownReport {
    /// Every request finished and the database closed (its uploads drained).
    pub clean: bool,
    /// Pipelines still running when shutdown gave up on them.
    pub in_flight_failed: usize,
    /// Idle streams closed at shutdown.
    pub streams_closed: usize,
}

/// A listening socket and loaded settings, ready to [`serve`](Bound::serve).
pub struct Bound {
    listener: std::net::TcpListener,
    addr: SocketAddr,
    cfg: ServeConfig,
    token: Option<Token>,
}

impl std::fmt::Debug for Bound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Bound")
            .field("addr", &self.addr)
            .field("tls", &self.tls())
            .field("auth", &self.auth())
            .finish_non_exhaustive()
    }
}

/// Bind the listener (a port in use fails here) and load the token.
pub fn bind(cfg: ServeConfig) -> Result<Bound, String> {
    cfg.check_bind()?;
    if cfg.tls.is_some() {
        return Err("TLS listener lands in Task 8".to_string());
    }
    let token = cfg.token_file.as_deref().map(Token::load).transpose()?;
    let listener = std::net::TcpListener::bind(cfg.listen)
        .map_err(|e| format!("cannot listen on {}: {e}", cfg.listen))?;
    listener
        .set_nonblocking(true)
        .map_err(|e| format!("cannot listen on {}: {e}", cfg.listen))?;
    let addr = listener
        .local_addr()
        .map_err(|e| format!("cannot listen on {}: {e}", cfg.listen))?;
    Ok(Bound {
        listener,
        addr,
        cfg,
        token,
    })
}

impl Bound {
    /// The address bound (the real port when the config asked for port 0).
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Whether the server speaks HTTPS.
    pub fn tls(&self) -> bool {
        false
    }

    /// Whether pipelines need the bearer token.
    pub fn auth(&self) -> bool {
        self.token.is_some()
    }

    /// Serve until `shutdown` resolves. `open` opens the database on a blocking thread
    /// while `/health` answers `starting`; `on_ready` runs once it is open. An `open` error
    /// is returned after the listener stopped.
    pub fn serve(
        self,
        open: impl FnOnce() -> Result<Database, String> + Send + 'static,
        on_ready: impl FnOnce(&Database) + Send + 'static,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> Result<ShutdownReport, String> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(IO_WORKERS)
            .max_blocking_threads(self.cfg.workers.saturating_add(4))
            .thread_name("turso-durable-server")
            .enable_all()
            .build()
            .map_err(|e| format!("cannot start the server runtime: {e}"))?;
        rt.block_on(self.run(open, on_ready, shutdown))
    }

    async fn run(
        self,
        open: impl FnOnce() -> Result<Database, String> + Send + 'static,
        on_ready: impl FnOnce(&Database) + Send + 'static,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> Result<ShutdownReport, String> {
        let Bound {
            listener,
            cfg,
            token,
            ..
        } = self;
        let app = App::new(token, cfg.workers);
        let listener = tokio::net::TcpListener::from_std(listener)
            .map_err(|e| format!("cannot listen on {}: {e}", cfg.listen))?;
        let (stop_tx, stop_rx) = watch::channel(false);
        let accept = tokio::spawn(listener::accept_loop(
            listener,
            http::router(app.clone()),
            stop_rx.clone(),
        ));
        let streams_cfg = StreamsCfg {
            timeout: cfg.stream_timeout,
            busy: cfg.busy_timeout,
            max: cfg.max_streams,
        };
        let mut opening = tokio::task::spawn_blocking(move || open_streams(open, streams_cfg));
        let mut shutdown = pin!(shutdown);
        let mut stopping = false;
        let opened = match race(&mut opening, shutdown.as_mut()).await {
            Either::Left(r) => r,
            Either::Right(()) => {
                // The open cannot be interrupted; wait for it, then close at once.
                stopping = true;
                app.begin_shutdown();
                opening.await
            }
        };
        let opened = opened.unwrap_or_else(|_| Err("opening the database panicked".to_string()));
        let streams = match opened {
            Ok(s) => Arc::new(s),
            Err(e) => {
                stop_serving(&app, &stop_tx, accept).await;
                return Err(e);
            }
        };
        let reaper = spawn_reaper(streams.clone(), cfg.stream_timeout, stop_rx);
        if !stopping {
            app.set_ready(streams.clone());
            on_ready(streams.database());
            shutdown.await;
        }
        stop_serving(&app, &stop_tx, accept).await;
        let _ = reaper.await;
        let report = tokio::task::spawn_blocking(move || close(&streams)).await;
        Ok(report.unwrap_or(ShutdownReport {
            clean: false,
            in_flight_failed: 0,
            streams_closed: 0,
        }))
    }
}

/// Open the database and the stream registry (with its anchor connection).
fn open_streams(
    open: impl FnOnce() -> Result<Database, String>,
    cfg: StreamsCfg,
) -> Result<Streams, String> {
    let db = open()?;
    Streams::new(db.clone(), cfg).map_err(|e| {
        let msg = format!("cannot connect to the database: {e}");
        if let Err(e) = db.close() {
            tracing::warn!(target: "turso_durable::server", error = %e, "database close failed");
        }
        msg
    })
}

/// Health goes to `shutting_down`, accepting stops, open connections finish their requests,
/// and every pipeline still running on a blocking thread finishes. Task 8 bounds the waits.
async fn stop_serving(app: &App, stop: &watch::Sender<bool>, accept: JoinHandle<listener::Conns>) {
    app.begin_shutdown();
    let _ = stop.send(true);
    if let Ok(conns) = accept.await {
        conns.finish().await;
    }
    app.drain().await;
}

/// Close idle streams every `min(timeout / 2, 1 s)` until `stop` changes.
fn spawn_reaper(
    streams: Arc<Streams>,
    timeout: Duration,
    mut stop: watch::Receiver<bool>,
) -> JoinHandle<()> {
    let period = (timeout / 2)
        .min(Duration::from_secs(1))
        .max(Duration::from_millis(1));
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(period);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut stopped = pin!(stop.changed());
        loop {
            if let Either::Right(_) = race(pin!(tick.tick()), stopped.as_mut()).await {
                return;
            }
            let s = streams.clone();
            let reaped = tokio::task::spawn_blocking(move || s.reap())
                .await
                .unwrap_or(0);
            if reaped > 0 {
                tracing::debug!(target: "turso_durable::server", reaped, "idle streams closed");
            }
        }
    })
}

/// Close the idle streams and the anchor, then the database when no stream is still busy.
fn close(streams: &Streams) -> ShutdownReport {
    let streams_closed = streams.close_all();
    let busy = streams.busy();
    let mut clean = busy == 0;
    if clean {
        if let Err(e) = streams.database().clone().close() {
            tracing::warn!(target: "turso_durable::server", error = %e, "database close failed");
            clean = false;
        }
    } else {
        tracing::warn!(
            target: "turso_durable::server",
            busy,
            "streams still busy at shutdown; the database is left open"
        );
    }
    ShutdownReport {
        clean,
        in_flight_failed: 0,
        streams_closed,
    }
}

/// Resolves on SIGTERM or SIGINT (Ctrl-C elsewhere than Unix).
pub async fn os_signals() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        if let (Ok(mut term), Ok(mut int)) = (
            signal(SignalKind::terminate()),
            signal(SignalKind::interrupt()),
        ) {
            race(pin!(term.recv()), pin!(int.recv())).await;
            return;
        }
        tracing::warn!(
            target: "turso_durable::server",
            "cannot install the SIGTERM handler; only Ctrl-C stops the server"
        );
    }
    let _ = tokio::signal::ctrl_c().await;
}
