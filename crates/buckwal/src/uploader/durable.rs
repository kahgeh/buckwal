//! The durable position and the background commits behind it. Shared by the uploader thread
//! (which confirms records) and the callers (which read the position and wait on it).
//!
//! The uploader uploads records in commit order, so the bucket always holds a prefix of the
//! commits: `durable` is the highest commit number of the last record in the bucket, and every
//! lower commit is in the bucket too (a commit rolled back locally has no record, so it does
//! not count).
use crate::error::{Error, Result};
use std::collections::VecDeque;
use std::sync::{Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

#[derive(Default)]
struct State {
    /// The highest commit number whose record is in the bucket, 0 before the first.
    durable: u64,
    /// Background commits that returned and are not yet durable, oldest first, with the
    /// time they returned.
    behind: VecDeque<(u64, Instant)>,
    /// Why the last record failed; cleared by the next that lands.
    failure: Option<Error>,
    /// The writer is fenced: nothing uploads again, and what is behind is dropped.
    fenced: bool,
    /// The uploader thread has ended.
    stopped: bool,
}

/// What a fenced writer dropped: the background commits that never reached the bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Dropped {
    pub count: u64,
    pub first: u64,
    pub last: u64,
}

#[derive(Default)]
pub(crate) struct Durable {
    state: Mutex<State>,
    changed: Condvar,
}

fn stopped() -> Error {
    Error::UploadFailed("uploader stopped".into())
}

impl Durable {
    fn lock(&self) -> MutexGuard<'_, State> {
        crate::db::lock(&self.state)
    }

    /// A background commit returned at `now` with number `commit`.
    pub(crate) fn add_behind(&self, commit: u64, now: Instant) {
        self.lock().behind.push_back((commit, now));
    }

    /// A record whose last commit is `upto` is in the bucket.
    pub(crate) fn confirmed(&self, upto: u64) {
        let mut s = self.lock();
        s.durable = s.durable.max(upto);
        let durable = s.durable;
        while s.behind.front().is_some_and(|&(c, _)| c <= durable) {
            s.behind.pop_front();
        }
        s.failure = None;
        drop(s);
        self.changed.notify_all();
    }

    /// A record failed with `e`. A fenced writer stays fenced.
    pub(crate) fn failed(&self, e: &Error) {
        let mut s = self.lock();
        if matches!(e, Error::Fenced(_)) {
            s.fenced = true;
        }
        s.failure = Some(e.clone());
        drop(s);
        self.changed.notify_all();
    }

    /// The uploader thread ended: nobody waits for a record any more.
    pub(crate) fn stop(&self) {
        self.lock().stopped = true;
        self.changed.notify_all();
    }

    pub(crate) fn durable(&self) -> u64 {
        self.lock().durable
    }

    /// The count of background commits behind, and how long ago the oldest returned.
    pub(crate) fn behind(&self) -> (u64, Duration) {
        let s = self.lock();
        let oldest = s
            .behind
            .front()
            .map_or(Duration::ZERO, |&(_, at)| at.elapsed());
        (s.behind.len() as u64, oldest)
    }

    /// The commits behind, as a fence drops them; `None` when none.
    pub(crate) fn dropped(&self) -> Option<Dropped> {
        let s = self.lock();
        let first = s.behind.front()?.0;
        let last = s.behind.back()?.0;
        Some(Dropped {
            count: s.behind.len() as u64,
            first,
            last,
        })
    }

    /// Block while `done(state)` is false, until the writer fails, is fenced or stops.
    fn wait_for(
        &self,
        deadline: Option<Instant>,
        what: &str,
        done: impl Fn(&State) -> bool,
    ) -> Result<()> {
        let mut s = self.lock();
        loop {
            if done(&s) {
                return Ok(());
            }
            if s.fenced {
                return Err(s
                    .failure
                    .clone()
                    .unwrap_or_else(|| Error::Fenced("another writer took over".into())));
            }
            if let Some(e) = &s.failure {
                return Err(e.clone());
            }
            if s.stopped {
                return Err(stopped());
            }
            s = match deadline {
                None => self.changed.wait(s).unwrap_or_else(|p| p.into_inner()),
                Some(d) => {
                    let left = d.saturating_duration_since(Instant::now());
                    if left.is_zero() {
                        return Err(Error::Timeout(what.into()));
                    }
                    self.changed
                        .wait_timeout(s, left)
                        .unwrap_or_else(|p| p.into_inner())
                        .0
                }
            };
        }
    }

    /// Block until `durable() >= commit`, or fail: `Timeout` after `timeout`, the upload's
    /// error while a record fails, `Fenced`, or "uploader stopped".
    pub(crate) fn wait_durable(&self, commit: u64, timeout: Duration) -> Result<()> {
        let deadline = Instant::now().checked_add(timeout);
        self.wait_for(
            deadline,
            &format!("commit {commit} is not durable yet"),
            |s| s.durable >= commit,
        )
    }

    /// Block until at most `max_behind` background commits wait, or fail as `wait_durable`
    /// does (with no timeout: the uploader retries a failing record without limit, and a
    /// record out of retries fails the wait).
    pub(crate) fn wait_behind(&self, max_behind: u32) -> Result<()> {
        self.wait_for(None, "", |s| s.behind.len() as u64 <= u64::from(max_behind))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn confirmed_moves_the_position_and_pops_what_it_covers() {
        let d = Durable::default();
        let t = Instant::now();
        for c in 1..=4 {
            d.add_behind(c, t);
        }
        assert_eq!(d.behind().0, 4);
        d.confirmed(2);
        assert_eq!((d.durable(), d.behind().0), (2, 2));
        d.confirmed(1);
        assert_eq!(d.durable(), 2, "the position never moves back");
        d.confirmed(4);
        assert_eq!((d.durable(), d.behind().0), (4, 0));
        assert_eq!(d.behind().1, Duration::ZERO);
    }

    #[test]
    fn wait_durable_times_out_fails_and_returns() {
        let d = Arc::new(Durable::default());
        let r = d.wait_durable(3, Duration::from_millis(20));
        assert!(matches!(r, Err(Error::Timeout(_))), "{r:?}");
        let d2 = d.clone();
        let h = std::thread::spawn(move || d2.wait_durable(3, Duration::from_secs(10)));
        std::thread::sleep(Duration::from_millis(20));
        d.confirmed(3);
        assert!(h.join().unwrap().is_ok());
        assert!(d.wait_durable(2, Duration::ZERO).is_ok(), "already durable");
        d.failed(&Error::UploadFailed("503".into()));
        let r = d.wait_durable(9, Duration::from_secs(10));
        assert!(matches!(r, Err(Error::UploadFailed(_))), "{r:?}");
        d.confirmed(9);
        assert!(d.wait_durable(9, Duration::ZERO).is_ok());
    }

    #[test]
    fn a_fenced_writer_fails_every_wait_and_keeps_what_it_dropped() {
        let d = Durable::default();
        let t = Instant::now();
        d.add_behind(5, t);
        d.add_behind(6, t);
        d.failed(&Error::Fenced("taken".into()));
        assert!(matches!(d.wait_behind(0), Err(Error::Fenced(_))));
        assert!(matches!(
            d.wait_durable(5, Duration::from_secs(1)),
            Err(Error::Fenced(_))
        ));
        assert_eq!(
            d.dropped(),
            Some(Dropped {
                count: 2,
                first: 5,
                last: 6
            })
        );
        assert_eq!(d.durable(), 0);
    }

    #[test]
    fn wait_behind_returns_when_few_enough_wait() {
        let d = Arc::new(Durable::default());
        let t = Instant::now();
        for c in 1..=3 {
            d.add_behind(c, t);
        }
        assert!(d.wait_behind(3).is_ok());
        let d2 = d.clone();
        let h = std::thread::spawn(move || d2.wait_behind(1));
        std::thread::sleep(Duration::from_millis(20));
        d.confirmed(1);
        std::thread::sleep(Duration::from_millis(20));
        assert!(!h.is_finished(), "two still wait");
        d.confirmed(2);
        assert!(h.join().unwrap().is_ok());
    }

    #[test]
    fn stop_wakes_a_waiter() {
        let d = Arc::new(Durable::default());
        d.add_behind(1, Instant::now());
        let d2 = d.clone();
        let h = std::thread::spawn(move || d2.wait_behind(0));
        std::thread::sleep(Duration::from_millis(20));
        d.stop();
        assert!(matches!(h.join().unwrap(), Err(Error::UploadFailed(_))));
    }
}
