use super::*;
use crate::error::Error;

#[test]
fn parses_the_spec_examples() {
    assert_eq!("off".parse::<Batching>().unwrap(), Batching::Off);
    assert_eq!("OFF".parse::<Batching>().unwrap(), Batching::Off);
    assert_eq!(
        "window 50ms 64 1MiB".parse::<Batching>().unwrap(),
        Batching::Window {
            max_wait: Duration::from_millis(50),
            max_commits: 64,
            max_bytes: 1 << 20
        }
    );
    assert_eq!(
        " Window  1s 1 512 ".parse::<Batching>().unwrap(),
        Batching::Window {
            max_wait: Duration::from_secs(1),
            max_commits: 1,
            max_bytes: 512
        }
    );
}

#[test]
fn display_round_trips() {
    for s in [
        "off",
        "window 50ms 64 1MiB",
        "window 250us 3 1000",
        "window 2s 1 4KiB",
        "window 0ms 1 1",
    ] {
        let b: Batching = s.parse().unwrap();
        assert_eq!(b.to_string(), s);
        assert_eq!(b.to_string().parse::<Batching>().unwrap(), b);
    }
    let odd = Batching::Window {
        max_wait: Duration::from_micros(1500),
        max_commits: 2,
        max_bytes: 1025,
    };
    assert_eq!(odd.to_string(), "window 1500us 2 1025");
}

#[test]
fn bad_text_and_zero_limits_are_config_errors() {
    for s in [
        "",
        "on",
        "window",
        "window 50 64 1MiB",
        "window 50ms 64",
        "window 50ms x 1MiB",
        "window 50ms 64 1MB",
        "window -1ms 64 1",
        "window 50ms 64 1MiB extra",
    ] {
        assert!(
            matches!(s.parse::<Batching>(), Err(Error::Config(_))),
            "{s:?}"
        );
    }
    for s in ["window 50ms 0 1MiB", "window 50ms 64 0"] {
        let b: Batching = s.parse().unwrap_or_else(|_| panic!("{s} parses"));
        assert!(matches!(b.validate(), Err(Error::Config(_))), "{s}");
    }
}

#[test]
fn errors_name_the_bad_text_and_overflow_is_refused() {
    match "window 50ms 64 1MB".parse::<Batching>() {
        Err(Error::Config(m)) => assert!(m.contains("1MB"), "{m}"),
        r => panic!("{r:?}"),
    }
    // Overflow of the byte-unit multiplication is an error, not a panic.
    assert!(matches!(
        "window 1ms 1 18446744073709551615GiB".parse::<Batching>(),
        Err(Error::Config(_))
    ));
    // The largest wait is representable and round-trips.
    let big: Batching = "window 18446744073709551615s 1 1".parse().unwrap();
    assert_eq!(big.to_string(), "window 18446744073709551615s 1 1");
    assert!(Batching::Off.validate().is_ok());
}

fn done(commit: u64) -> PendingAck {
    PendingAck::Done {
        commit,
        waited: Duration::ZERO,
        batching: Batching::Off,
        span: tracing::Span::none(),
    }
}

fn commits(acks: &[PendingAck]) -> Vec<u64> {
    acks.iter()
        .map(|a| match a {
            PendingAck::Done { commit, .. } => *commit,
            PendingAck::Waiting { ticket, .. } => ticket.commit,
        })
        .collect()
}

#[test]
fn ctx_holds_the_setting_and_acks_until_the_guard_drops() {
    let w = "window 5ms 2 3".parse::<Batching>().unwrap();
    assert_eq!(ctx::current(), None);
    ctx::push(done(1)); // outside every call: dropped
    {
        let _g = ctx::enter(w);
        assert_eq!(ctx::current(), Some(w));
        assert!(ctx::take().is_empty(), "a call starts with no acks");
        ctx::push(done(2));
        ctx::push(done(3));
        {
            let _inner = ctx::enter(Batching::Off);
            assert_eq!(ctx::current(), Some(Batching::Off));
            assert!(ctx::take().is_empty(), "a nested call has its own acks");
        }
        assert_eq!(ctx::current(), Some(w), "the outer call's context is back");
        assert_eq!(commits(&ctx::take()), vec![2, 3]);
        assert!(ctx::take().is_empty());
        ctx::push(done(4));
    }
    assert_eq!(ctx::current(), None);
    assert!(ctx::take().is_empty(), "the dropped guard dropped its acks");
}

#[test]
fn ctx_is_cleared_by_a_panic() {
    let r = std::panic::catch_unwind(|| {
        let _g = ctx::enter(Batching::Off);
        ctx::push(done(1));
        panic!("inside a call");
    });
    assert!(r.is_err());
    assert_eq!(ctx::current(), None);
    assert!(ctx::take().is_empty());
}

#[test]
fn ctx_is_per_thread() {
    let _g = ctx::enter(Batching::Off);
    std::thread::spawn(|| assert_eq!(ctx::current(), None))
        .join()
        .unwrap();
}
