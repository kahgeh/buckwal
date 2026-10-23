//! `follow`: a read replica of the bucket's database, with a status line per event.
use crate::say;
use crate::serve::Failure;
use buckwal::{open_replica, Config, ReplicaStats};
use buckwal_server::notify::{serve as notify_serve, NotifyConfig};
use buckwal_server::watch_signals;
use std::path::Path;
use std::time::Duration;

/// How often the stats are looked at.
const POLL: Duration = Duration::from_millis(200);

pub fn run(dir: &Path, poll_ms: Option<u64>) -> Result<(), Failure> {
    // Every setting is read and checked before the directory, the bucket or a port is touched.
    let cfg = apply_poll(crate::config(dir).map_err(Failure::Usage)?, poll_ms)?;
    let notify = NotifyConfig::from_env().map_err(Failure::Usage)?;
    if let Some(n) = &notify {
        n.check_bind().map_err(Failure::Usage)?;
    }
    let stop = watch_signals().map_err(Failure::Failed)?;
    let replica = open_replica(cfg).map_err(|e| Failure::Failed(e.to_string()))?;
    let listener = match notify {
        None => None,
        Some(n) => match notify_serve(n, replica.waker(), stop.clone().stopped()) {
            Ok(b) => {
                say(&format!(
                    "notify listening addr={} tls={} auth={}",
                    b.local_addr(),
                    b.tls(),
                    b.auth()
                ));
                Some(b)
            }
            Err(e) => {
                let _ = replica.close();
                return Err(Failure::Failed(e));
            }
        },
    };
    let mut prev = replica.stats();
    say(&format!(
        "following generation={} seq={}",
        prev.generation, prev.seq
    ));
    let mut stopped_for = None;
    while !stop.is_set() {
        std::thread::sleep(POLL);
        let now = replica.stats();
        for l in events(&prev, &now) {
            say(&l);
        }
        stopped_for = now.stopped.clone();
        prev = now;
        if stopped_for.is_some() {
            break;
        }
    }
    // Signalled, the listener's shutdown has resolved; stopped for good, it ends with the process.
    if stop.is_set() {
        if let Some(b) = listener {
            b.join().map_err(Failure::Failed)?;
        }
    }
    replica
        .close()
        .map_err(|e| Failure::Failed(e.to_string()))?;
    match stopped_for {
        Some(reason) => Err(Failure::Failed(format!("the replica stopped: {reason}"))),
        None => Ok(()),
    }
}

/// `cfg` with the poll interval `--poll-ms` gives, checked as `BUCKWAL_REPLICA_POLL_MS` is.
pub fn apply_poll(mut cfg: Config, poll_ms: Option<u64>) -> Result<Config, Failure> {
    if let Some(ms) = poll_ms {
        cfg.replica_poll_interval = Duration::from_millis(ms);
    }
    cfg.validate_replica()
        .map_err(|e| Failure::Usage(e.to_string()))?;
    Ok(cfg)
}

/// The status lines for the step from `prev` to `now`, in a fixed order.
pub fn events(prev: &ReplicaStats, now: &ReplicaStats) -> Vec<String> {
    let mut out = Vec::new();
    if (now.generation, now.seq) != (prev.generation, prev.seq) {
        out.push(format!(
            "applied generation={} seq={}",
            now.generation, now.seq
        ));
    }
    if now.notifications_received > prev.notifications_received {
        out.push(format!("notified received={}", now.notifications_received));
    }
    if now.deferred_checkpoints > prev.deferred_checkpoints {
        out.push(format!(
            "checkpoint deferred count={}",
            now.deferred_checkpoints
        ));
    }
    if now.fenced_records_skipped > prev.fenced_records_skipped {
        out.push(format!(
            "skipped fenced_records={}",
            now.fenced_records_skipped
        ));
    }
    if now.re_restores > prev.re_restores {
        let reason = now
            .last_re_restore_at
            .as_ref()
            .map_or("unknown", |(_, r)| r.as_str());
        out.push(format!(
            "re-restored count={} reason={reason}",
            now.re_restores
        ));
    }
    if prev.stopped.is_none() {
        if let Some(reason) = &now.stopped {
            out.push(format!("stopped reason={reason}"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn follow_poll_ms_at_or_above_trim_grace_is_a_usage_error() {
        let c = buckwal::Config::new("/d", "http://e", "b", "ak", "sk");
        let grace = c.trim_grace.as_millis() as u64;
        for ms in [0, grace, grace + 1] {
            let e = apply_poll(c.clone(), Some(ms)).unwrap_err();
            assert_eq!(e.exit_code(), 2, "{ms}: {e:?}");
            assert!(
                e.message().contains("BUCKWAL_REPLICA_POLL_MS"),
                "{}",
                e.message()
            );
        }
        let ok = apply_poll(c.clone(), Some(grace - 1)).unwrap();
        assert_eq!(ok.replica_poll_interval.as_millis() as u64, grace - 1);
        assert_eq!(
            apply_poll(c.clone(), None).unwrap().replica_poll_interval,
            c.replica_poll_interval
        );
    }

    fn st() -> ReplicaStats {
        ReplicaStats {
            generation: 2,
            seq: 5,
            ..Default::default()
        }
    }

    #[test]
    fn events_follow_the_stats_deltas() {
        let a = st();
        assert!(events(&a, &a).is_empty());
        let mut b = a.clone();
        b.seq = 9;
        assert_eq!(events(&a, &b), ["applied generation=2 seq=9"]);
        let mut c = b.clone();
        c.notifications_received = 2;
        c.deferred_checkpoints = 1;
        c.fenced_records_skipped = 3;
        assert_eq!(
            events(&b, &c),
            [
                "notified received=2",
                "checkpoint deferred count=1",
                "skipped fenced_records=3"
            ]
        );
        let mut d = c.clone();
        d.re_restores = 1;
        d.generation = 3;
        d.seq = 0;
        d.last_re_restore_at = Some((std::time::SystemTime::now(), "gap".into()));
        assert_eq!(
            events(&c, &d),
            [
                "applied generation=3 seq=0",
                "re-restored count=1 reason=gap"
            ]
        );
        let mut e = d.clone();
        e.stopped = Some("mvcc".into());
        assert_eq!(events(&d, &e), ["stopped reason=mvcc"]);
        assert!(events(&e, &e).is_empty(), "stopped is reported once");
    }
}
