//! A server of tursodb's SQL over HTTP v3 (`serverless/PROTOCOL.md` in the turso repo) for
//! `turso_durable` databases. Its clients are tursodb's serverless drivers.
//!
//! [`bind`] takes the port and loads the TLS files and the token, so a bad setting fails
//! before the database is opened. [`Bound::serve`] then runs the server on its own tokio
//! runtime: `/health` answers `starting` at once, the database opens on a blocking thread,
//! and pipelines are served from then until the `shutdown` future resolves.
//!
//! Shutdown, in order: health turns `shutting_down` and new pipelines get 503
//! `SHUTTING_DOWN`; accepting stops; in-flight pipelines get up to `shutdown_timeout`, past
//! which their connections are aborted (a response not yet written is never sent); idle
//! streams and the anchor close; and the database closes only when no request still holds
//! a connection.
pub mod config;
pub mod exec;
mod http;
mod listener;
mod proto;
pub mod streams;

pub use config::{ServeConfig, TlsFiles, Token};

use crate::exec::Fence;
use crate::http::App;
use crate::listener::{race, Either, MAX_CONNECTIONS};
use crate::streams::{Streams, StreamsCfg};
use std::future::Future;
use std::net::SocketAddr;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;
use turso_durable::Database;

/// Tokio worker threads: they only do I/O, the statements run on blocking threads.
const IO_WORKERS: usize = 2;

/// How serving ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShutdownReport {
    /// Every request finished and the database closed (its uploads drained).
    pub clean: bool,
    /// A takeover fenced this server while it ran: a library call answered `Fenced`. An idle
    /// server never sees the fence, so it stays false until a write hits it.
    pub fenced: bool,
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
    tls: Option<TlsAcceptor>,
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

/// Load the TLS files (a missing, unreadable or mismatched one fails here) and the token,
/// and bind the listener (a port in use fails here).
pub fn bind(cfg: ServeConfig) -> Result<Bound, String> {
    cfg.check_bind()?;
    let tls = cfg.tls.as_ref().map(listener::tls_acceptor).transpose()?;
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
        tls,
    })
}

impl Bound {
    /// The address bound (the real port when the config asked for port 0).
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Whether the server speaks HTTPS.
    pub fn tls(&self) -> bool {
        self.tls.is_some()
    }

    /// Whether pipelines need the bearer token.
    pub fn auth(&self) -> bool {
        self.token.is_some()
    }

    /// Serve until `shutdown` resolves. `open` opens the database on a blocking thread
    /// while `/health` answers `starting`; `on_ready` runs once it is open. An `open` error
    /// is returned after the listener stopped.
    ///
    /// Past `shutdown_timeout` a library call still running cannot be cancelled: it is left
    /// on its blocking thread, the database is not closed, and the report says `clean: false`.
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
        let report = rt.block_on(self.run(open, on_ready, shutdown));
        // Dropping the runtime would wait for every blocking task without limit, including a
        // library call abandoned at the shutdown bound.
        rt.shutdown_background();
        report
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
            tls,
            ..
        } = self;
        let app = App::new(token, cfg.workers, http::BODY_READ_TIMEOUT);
        let listener = tokio::net::TcpListener::from_std(listener)
            .map_err(|e| format!("cannot listen on {}: {e}", cfg.listen))?;
        let (stop_tx, stop_rx) = watch::channel(false);
        let accept = tokio::spawn(listener::accept_loop(
            listener,
            http::router(app.clone()),
            tls,
            MAX_CONNECTIONS,
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
                stop_serving(&app, &stop_tx, accept, cfg.shutdown_timeout).await;
                return Err(e);
            }
        };
        let reaper = spawn_reaper(streams.clone(), cfg.stream_timeout, stop_rx);
        if !stopping {
            app.set_ready(streams.clone());
            on_ready(streams.database());
            shutdown.await;
        }
        let in_flight_failed = stop_serving(&app, &stop_tx, accept, cfg.shutdown_timeout).await;
        let _ = reaper.await;
        let fence = app.fence();
        let report =
            tokio::task::spawn_blocking(move || close(&streams, in_flight_failed, &fence)).await;
        let report = report.unwrap_or(ShutdownReport {
            clean: false,
            fenced: false,
            in_flight_failed,
            streams_closed: 0,
        });
        // Read after every request finished or was abandoned, so a late `Fenced` counts.
        Ok(ShutdownReport {
            fenced: app.fenced(),
            ..report
        })
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
/// and every pipeline still running on a blocking thread finishes, all within `bound`. Past
/// it the connections are aborted; returns how many pipelines that left unanswered.
async fn stop_serving(
    app: &App,
    stop: &watch::Sender<bool>,
    accept: JoinHandle<listener::Conns>,
    bound: Duration,
) -> usize {
    app.begin_shutdown();
    let _ = stop.send(true);
    let deadline = tokio::time::Instant::now() + bound;
    // The accept loop returns as soon as it sees `stop`.
    let mut conns = accept.await.ok();
    let drained = tokio::time::timeout_at(deadline, async {
        if let Some(c) = conns.as_mut() {
            c.finish().await;
        }
        app.drain().await;
    })
    .await;
    if drained.is_ok() {
        return 0;
    }
    app.give_up();
    if let Some(c) = conns.as_mut() {
        c.abort().await;
    }
    let failed = app.failed();
    tracing::warn!(
        target: "turso_durable::server",
        bound_ms = u64::try_from(bound.as_millis()).unwrap_or(u64::MAX),
        in_flight_failed = failed,
        "shutdown bound reached; in-flight requests aborted"
    );
    failed
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

/// A close error makes the stop unclean; a `Fenced` one also trips the latch.
fn note_close_error(fence: &Fence, clean: &mut bool, e: &turso_durable::Error) {
    if matches!(e, turso_durable::Error::Fenced(_)) {
        fence.set();
    }
    *clean = false;
}

/// Close the idle streams and the anchor, then the database when no stream is still busy.
/// Clean only when nothing failed in flight and every close succeeded. A `Fenced` close error
/// trips `fence`, so the report says fenced.
fn close(streams: &Streams, in_flight_failed: usize, fence: &Fence) -> ShutdownReport {
    let closed = streams.close_all();
    let busy = streams.busy();
    let mut clean = in_flight_failed == 0;
    if let Some(e) = &closed.error {
        note_close_error(fence, &mut clean, e);
    }
    if busy == 0 {
        if let Err(e) = streams.database().clone().close() {
            tracing::warn!(target: "turso_durable::server", error = %e, "database close failed");
            note_close_error(fence, &mut clean, &e);
        }
    } else {
        // A library call is still running on a blocking thread and cannot be cancelled.
        tracing::warn!(
            target: "turso_durable::server",
            busy,
            "shutdown bound reached; exiting without draining"
        );
        clean = false;
    }
    ShutdownReport {
        clean,
        fenced: fence.is_set(),
        in_flight_failed,
        streams_closed: closed.streams,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fenced_close_error_trips_the_latch_and_unsets_clean() {
        let fence = Fence::new();
        let mut clean = true;
        note_close_error(
            &fence,
            &mut clean,
            &turso_durable::Error::Fenced("x".into()),
        );
        assert!(fence.is_set());
        assert!(!clean);
    }

    #[test]
    fn another_close_error_unsets_clean_only() {
        let fence = Fence::new();
        let mut clean = true;
        note_close_error(
            &fence,
            &mut clean,
            &turso_durable::Error::Config("x".into()),
        );
        assert!(!fence.is_set());
        assert!(!clean);
    }
}
