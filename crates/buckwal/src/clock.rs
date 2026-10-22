//! Two-clock timestamps for the writer's lease.
//!
//! `Instant` stands still while a machine is suspended (on Linux and macOS it does), so a
//! lease measured on it alone would survive a suspend. The wall clock does advance, but it
//! can be stepped. A `Stamp` keeps both and its age is the larger: a sleep or a forward jump
//! costs at most one extra head check, and a backward jump never makes it younger than
//! `Instant` measures.
use std::time::{Duration, Instant, SystemTime};

/// When something happened, on both the monotonic and the wall clock.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Stamp {
    at: Instant,
    wall: SystemTime,
}

impl Stamp {
    pub(crate) fn now() -> Stamp {
        Stamp {
            at: Instant::now(),
            wall: SystemTime::now(),
        }
    }

    /// How long ago this was: the larger of the two clocks' measures. A wall time in the
    /// future (the clock was set back) counts as zero, leaving `Instant`'s.
    pub(crate) fn age(&self) -> Duration {
        let wall = self.wall.elapsed().unwrap_or(Duration::ZERO);
        self.at.elapsed().max(wall)
    }

    /// Move the stamp back by `instant_by` on the monotonic clock and `wall_by` on the wall
    /// clock, as if it had been taken that much earlier on each.
    #[cfg(test)]
    pub(crate) fn back(&mut self, instant_by: Duration, wall_by: Duration) {
        self.at = self
            .at
            .checked_sub(instant_by)
            .expect("an instant that far back");
        self.wall = self
            .wall
            .checked_sub(wall_by)
            .expect("a time that far back");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The age is the larger of the two clocks: a machine that slept (`Instant` stood still)
    /// and a wall clock that jumped forward each count in full, and a wall clock that jumped
    /// back never makes a stamp younger than `Instant` measures.
    #[test]
    fn stamp_age_is_the_larger_clock() {
        let ten = Duration::from_secs(10);

        let mut s = Stamp::now();
        s.back(Duration::ZERO, ten);
        assert!(s.age() >= ten, "wall backed by 10 s: {:?}", s.age());

        let mut s = Stamp::now();
        s.back(ten, Duration::ZERO);
        assert!(s.age() >= ten, "Instant backed by 10 s: {:?}", s.age());

        let s = Stamp {
            at: Instant::now(),
            wall: SystemTime::now() + ten,
        };
        assert!(
            s.age() < Duration::from_secs(1),
            "a wall stamp in the future: the Instant measure, {:?}",
            s.age()
        );
    }
}
