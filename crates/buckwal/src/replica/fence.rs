//! The fence rule a follower applies while reading a generation's log. After a fence record
//! `F(w, k)` only records of writer `w` count; a later fence must raise the epoch (or repeat it
//! for the same writer, a resumed claim). Records of other writers after the fence are late
//! writes of a fenced-out writer: the follower skips them where a restore refuses the log.

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct FenceRule {
    current: Option<(String, u64)>,
}

impl FenceRule {
    /// A fence record. True when it moves the rule (first fence, higher epoch, or the same
    /// writer at the same epoch); false for a stale fence, which changes nothing.
    pub(crate) fn on_fence(&mut self, writer_id: &str, epoch: u64) -> bool {
        if let Some((w, k)) = &self.current {
            if !(epoch > *k || (epoch == *k && writer_id == w)) {
                return false;
            }
        }
        self.current = Some((writer_id.to_string(), epoch));
        true
    }

    /// Whether an extents record by `writer_id` counts under the rule.
    pub(crate) fn admits(&self, writer_id: &str) -> bool {
        self.current.as_ref().is_none_or(|(w, _)| w == writer_id)
    }

    #[cfg(test)]
    pub(crate) fn writer(&self) -> Option<&str> {
        self.current.as_ref().map(|(w, _)| w.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rule_moves_with_rising_epochs_and_ignores_stale_fences() {
        let mut f = FenceRule::default();
        assert!(f.admits("a") && f.admits("b"));
        assert!(f.on_fence("b", 2));
        assert!(!f.admits("a") && f.admits("b"));
        assert!(!f.on_fence("a", 1));
        assert_eq!(f.writer(), Some("b"));
        assert!(f.on_fence("b", 2));
        assert!(!f.on_fence("c", 2));
        assert!(f.on_fence("c", 3));
        assert!(f.admits("c") && !f.admits("b"));
    }
}
