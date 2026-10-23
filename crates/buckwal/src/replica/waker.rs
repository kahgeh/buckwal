//! The wake-up of the follower thread: a flag and a condvar. Notifications only set the flag,
//! so any number of `wake` calls while a pass runs cause exactly one more pass.
use std::sync::{
    atomic::{AtomicU64, Ordering::Relaxed},
    Arc, Condvar, Mutex, MutexGuard,
};
use std::time::Instant;

use crate::replica::sync::Trigger;

#[derive(Default)]
struct Flags {
    pending: bool,
    stop: bool,
}

#[derive(Default)]
pub(crate) struct WakeState {
    flags: Mutex<Flags>,
    cv: Condvar,
    received: AtomicU64,
}

impl WakeState {
    fn flags(&self) -> MutexGuard<'_, Flags> {
        self.flags.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub(crate) fn received(&self) -> u64 {
        self.received.load(Relaxed)
    }

    pub(crate) fn stop(&self) {
        self.flags().stop = true;
        self.cv.notify_all();
    }

    /// Wait for a wake (`Notification`, clearing the flag) or `deadline` (`SlowPoll`); `None`
    /// once stopped.
    pub(crate) fn wait(&self, deadline: Instant) -> Option<Trigger> {
        let mut g = self.flags();
        loop {
            if g.stop {
                return None;
            }
            if g.pending {
                g.pending = false;
                return Some(Trigger::Notification);
            }
            let now = Instant::now();
            if now >= deadline {
                return Some(Trigger::SlowPoll);
            }
            g = self
                .cv
                .wait_timeout(g, deadline - now)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
    }
}

/// Tells a replica that something may have changed in the bucket: call [`ReplicaWaker::wake`]
/// when a notification arrives. The notification's contents are not needed; the replica lists
/// the log itself. Wakes that come while a pass runs are merged into one further pass.
#[derive(Clone)]
pub struct ReplicaWaker {
    state: Arc<WakeState>,
}

impl ReplicaWaker {
    pub(crate) fn new(state: Arc<WakeState>) -> ReplicaWaker {
        ReplicaWaker { state }
    }

    pub fn wake(&self) {
        self.state.received.fetch_add(1, Relaxed);
        self.state.flags().pending = true;
        self.state.cv.notify_all();
    }
}
