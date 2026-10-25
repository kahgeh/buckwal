// Derived from tursodatabase/turso spike/objstore (MIT), commit 417bd81c0.
//! Bounded retry with exponential backoff for transient store errors.
use super::StoreError;
use crate::config::defaults;
use crate::error::{Error, Result};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub deadline: Duration,
    pub first_backoff: Duration,
    pub max_backoff: Duration,
}

/// The policy of a default [`crate::Config`]: every value from [`crate::config::defaults`].
impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            deadline: defaults::RETRY_DEADLINE,
            first_backoff: defaults::FIRST_BACKOFF,
            max_backoff: defaults::MAX_BACKOFF,
        }
    }
}

/// Run `op` (given the attempt number from 0) until it succeeds, fails permanently, or the
/// deadline passes. The last sleep is clipped to the deadline, so the call ends within the
/// deadline plus one attempt.
pub fn with_retry<T>(
    p: &RetryPolicy,
    what: &str,
    mut op: impl FnMut(u32) -> std::result::Result<T, StoreError>,
) -> Result<T> {
    let start = Instant::now();
    let mut backoff = p.first_backoff;
    let mut attempt = 0u32;
    loop {
        match op(attempt) {
            Ok(v) => return Ok(v),
            Err(StoreError::Permanent(m)) => return Err(Error::Bucket(format!("{what}: {m}"))),
            Err(StoreError::Transient(m)) => {
                let elapsed = start.elapsed();
                if elapsed >= p.deadline {
                    return Err(Error::Timeout(format!(
                        "{what}: gave up after {attempt_count} attempts in {elapsed:?}: {m}",
                        attempt_count = attempt + 1
                    )));
                }
                std::thread::sleep(backoff.min(p.deadline - elapsed));
                backoff = (backoff * 2).min(p.max_backoff);
                attempt = attempt.saturating_add(1);
            }
        }
    }
}

/// [`with_retry`] for a caller that returns a [`StoreError`]: a timeout stays transient (a
/// caller above may try again), anything else is permanent.
pub fn retry_store<T>(
    p: &RetryPolicy,
    what: &str,
    op: impl FnMut(u32) -> std::result::Result<T, StoreError>,
) -> std::result::Result<T, StoreError> {
    with_retry(p, what, op).map_err(|e| match e {
        Error::Timeout(m) => StoreError::Transient(m),
        Error::Bucket(m) => StoreError::Permanent(m),
        e => StoreError::Permanent(e.to_string()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fast() -> RetryPolicy {
        RetryPolicy {
            deadline: Duration::from_secs(5),
            first_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(4),
        }
    }

    #[test]
    fn retry_succeeds_after_transient_failures() {
        let mut seen = vec![];
        let r = with_retry(&fast(), "op", |a| {
            seen.push(a);
            if a < 3 {
                Err(StoreError::Transient("boom".into()))
            } else {
                Ok(a)
            }
        });
        assert_eq!(r, Ok(3));
        assert_eq!(seen, vec![0, 1, 2, 3]);
    }

    #[test]
    fn retry_times_out_at_deadline() {
        let p = RetryPolicy {
            deadline: Duration::from_millis(200),
            first_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_millis(500),
        };
        let t = Instant::now();
        let r: Result<()> = with_retry(&p, "op", |_| Err(StoreError::Transient("x".into())));
        let el = t.elapsed();
        assert!(matches!(r, Err(Error::Timeout(_))), "{r:?}");
        assert!(
            el > Duration::from_millis(150) && el < Duration::from_secs(1),
            "{el:?}"
        );
    }

    #[test]
    fn retry_permanent_fails_fast() {
        let mut n = 0;
        let r: Result<()> = with_retry(&fast(), "op", |_| {
            n += 1;
            Err(StoreError::Permanent("403".into()))
        });
        assert!(matches!(r, Err(Error::Bucket(_))), "{r:?}");
        assert_eq!(n, 1);
    }

    #[test]
    fn default_policy_values() {
        let p = RetryPolicy::default();
        assert_eq!(
            (p.deadline, p.first_backoff, p.max_backoff),
            (
                Duration::from_secs(30),
                Duration::from_millis(50),
                Duration::from_secs(2)
            )
        );
    }

    #[test]
    fn retry_store_keeps_the_store_error_class() {
        let mut n = 0;
        let r = retry_store(&fast(), "part 3", |_| {
            n += 1;
            if n < 3 {
                Err(StoreError::Transient("boom".into()))
            } else {
                Ok(n)
            }
        });
        assert!(matches!(r, Ok(3)), "{r:?}");
        let r: std::result::Result<(), StoreError> = retry_store(&fast(), "part 3", |_| {
            Err(StoreError::Permanent("403".into()))
        });
        assert!(
            matches!(&r, Err(StoreError::Permanent(m)) if m.contains("part 3") && m.contains("403")),
            "{r:?}"
        );
        let p = RetryPolicy {
            deadline: Duration::from_millis(20),
            first_backoff: Duration::from_millis(5),
            max_backoff: Duration::from_millis(5),
        };
        let r: std::result::Result<(), StoreError> =
            retry_store(&p, "part 3", |_| Err(StoreError::Transient("x".into())));
        assert!(matches!(r, Err(StoreError::Transient(_))), "{r:?}");
    }
}
