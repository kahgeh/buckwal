//! The accept loop: each TCP connection is served by hyper's HTTP/1 server with the axum
//! router, watched by a [`GracefulShutdown`] and tracked in a [`JoinSet`], so shutdown can
//! wait for open connections (and, with Task 8's bound, abort them).
use axum::Router;
use hyper::server::conn::http1;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::server::graceful::GracefulShutdown;
use hyper_util::service::TowerToHyperService;
use std::future::Future;
use std::pin::{pin, Pin};
use std::task::Poll;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinSet;

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

/// The connections the accept loop started.
pub(crate) struct Conns {
    graceful: GracefulShutdown,
    tasks: JoinSet<()>,
}

impl Conns {
    /// Tell every connection to finish its current request and close, then wait for all of
    /// them. Task 8 bounds this wait and aborts `tasks` past the bound.
    pub(crate) async fn finish(mut self) {
        self.graceful.shutdown().await;
        while self.tasks.join_next().await.is_some() {}
    }
}

/// Accept connections until `stop` changes (or its sender is dropped).
pub(crate) async fn accept_loop(
    listener: TcpListener,
    app: Router,
    mut stop: watch::Receiver<bool>,
) -> Conns {
    let graceful = GracefulShutdown::new();
    let mut tasks = JoinSet::new();
    let mut stopped = pin!(stop.changed());
    loop {
        // Finished connections leave the set as they go, so it holds only live ones.
        while tasks.try_join_next().is_some() {}
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
        let _ = tcp.set_nodelay(true);
        let conn = http1::Builder::new()
            .timer(TokioTimer::new())
            .serve_connection(TokioIo::new(tcp), TowerToHyperService::new(app.clone()));
        let conn = graceful.watch(conn);
        tasks.spawn(async move {
            if let Err(e) = conn.await {
                tracing::debug!(target: "turso_durable::server", error = %e, "connection ended");
            }
        });
    }
    Conns { graceful, tasks }
}
