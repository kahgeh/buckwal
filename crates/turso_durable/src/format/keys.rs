//! Object key layout. Generation is `{:010}`, seq `{:020}`, page `{:010}`.

#[derive(Debug, Clone)]
pub(crate) struct Keys {
    prefix: String,
}

/// Parse exactly `width` ASCII digits.
fn digits(s: &str, width: usize) -> Option<u64> {
    if s.len() != width || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

impl Keys {
    pub(crate) fn new(prefix: &str) -> Self {
        Keys {
            prefix: prefix.to_string(),
        }
    }

    fn join(&self, rest: &str) -> String {
        if self.prefix.is_empty() {
            rest.to_string()
        } else {
            format!("{}/{rest}", self.prefix)
        }
    }

    /// The part of `key` after the prefix, or None when it is outside it.
    fn strip<'a>(&self, key: &'a str) -> Option<&'a str> {
        if self.prefix.is_empty() {
            return Some(key);
        }
        key.strip_prefix(self.prefix.as_str())?.strip_prefix('/')
    }

    pub(crate) fn head(&self) -> String {
        self.join("head")
    }

    pub(crate) fn log(&self, gen: u64, seq: u64) -> String {
        self.join(&format!("log/{gen:010}/{seq:020}"))
    }

    pub(crate) fn log_prefix(&self, gen: u64) -> String {
        self.join(&format!("log/{gen:010}/"))
    }

    pub(crate) fn page(&self, gen: u64, page: u32) -> String {
        self.join(&format!("pages/{gen:010}/{page:010}"))
    }

    pub(crate) fn manifest(&self, gen: u64) -> String {
        self.join(&format!("manifest/{gen:010}"))
    }

    pub(crate) fn parse_log(&self, key: &str) -> Option<(u64, u64)> {
        let rest = self.strip(key)?.strip_prefix("log/")?;
        let (g, s) = rest.split_once('/')?;
        Some((digits(g, 10)?, digits(s, 20)?))
    }

    pub(crate) fn parse_page(&self, key: &str) -> Option<(u64, u32)> {
        let rest = self.strip(key)?.strip_prefix("pages/")?;
        let (g, p) = rest.split_once('/')?;
        Some((digits(g, 10)?, u32::try_from(digits(p, 10)?).ok()?))
    }

    pub(crate) fn parse_manifest(&self, key: &str) -> Option<u64> {
        digits(self.strip(key)?.strip_prefix("manifest/")?, 10)
    }
}
