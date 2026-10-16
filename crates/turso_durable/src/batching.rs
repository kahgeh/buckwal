//! The batching setting: how long and how much the uploader gathers before it ships a log
//! object. Parsed from and shown as the PRAGMA text, `off` or `window <max_wait>
//! <max_commits> <max_bytes>`, e.g. `window 50ms 64 1MiB`.
use crate::error::{Error, Result};
use std::fmt;
use std::str::FromStr;
use std::time::Duration;

/// Whether, and how, commits are gathered into one upload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Batching {
    /// Every commit is uploaded on its own.
    Off,
    /// Gather commits until `max_wait` has passed since the first, or `max_commits` or
    /// `max_bytes` is reached. Both counts must be at least 1 ([`Batching::validate`]).
    Window {
        max_wait: Duration,
        max_commits: u32,
        max_bytes: u64,
    },
}

fn bad(text: &str, why: &str) -> Error {
    Error::Config(format!("batching {text:?}: {why}"))
}

fn parse_wait(text: &str, tok: &str) -> Result<Duration> {
    let (digits, unit) = match tok.find(|c: char| !c.is_ascii_digit()) {
        Some(i) => tok.split_at(i),
        None => {
            return Err(bad(
                text,
                &format!("max_wait {tok:?} needs a unit (us, ms or s)"),
            ))
        }
    };
    let n: u64 = digits
        .parse()
        .map_err(|_| bad(text, &format!("max_wait {tok:?} is not <integer>(us|ms|s)")))?;
    match unit {
        "us" => Ok(Duration::from_micros(n)),
        "ms" => Ok(Duration::from_millis(n)),
        "s" => Ok(Duration::from_secs(n)),
        _ => Err(bad(
            text,
            &format!("max_wait {tok:?} has unknown unit {unit:?}"),
        )),
    }
}

fn parse_bytes(text: &str, tok: &str) -> Result<u64> {
    let split = tok.find(|c: char| !c.is_ascii_digit()).unwrap_or(tok.len());
    let (digits, unit) = tok.split_at(split);
    let n: u64 = digits.parse().map_err(|_| {
        bad(
            text,
            &format!("max_bytes {tok:?} is not <integer>[B|KiB|MiB|GiB]"),
        )
    })?;
    let shift = match unit {
        "" | "B" => 0,
        "KiB" => 10,
        "MiB" => 20,
        "GiB" => 30,
        _ => {
            return Err(bad(
                text,
                &format!("max_bytes {tok:?} has unknown unit {unit:?}"),
            ))
        }
    };
    n.checked_mul(1u64 << shift)
        .ok_or_else(|| bad(text, &format!("max_bytes {tok:?} is too large")))
}

impl FromStr for Batching {
    type Err = Error;

    /// `off` or `window <max_wait> <max_commits> <max_bytes>`; keywords are case-insensitive
    /// and any whitespace separates the words. Zero limits parse; [`Batching::validate`]
    /// refuses them.
    fn from_str(text: &str) -> Result<Batching> {
        let words: Vec<&str> = text.split_whitespace().collect();
        match words.as_slice() {
            [w] if w.eq_ignore_ascii_case("off") => Ok(Batching::Off),
            [w, wait, commits, bytes] if w.eq_ignore_ascii_case("window") => {
                let max_commits = commits.parse::<u32>().map_err(|_| {
                    bad(text, &format!("max_commits {commits:?} is not an integer"))
                })?;
                Ok(Batching::Window {
                    max_wait: parse_wait(text, wait)?,
                    max_commits,
                    max_bytes: parse_bytes(text, bytes)?,
                })
            }
            _ => Err(bad(
                text,
                "expected `off` or `window <max_wait> <max_commits> <max_bytes>`",
            )),
        }
    }
}

impl fmt::Display for Batching {
    /// The canonical text: the largest unit that divides the value evenly.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Batching::Window {
            max_wait,
            max_commits,
            max_bytes,
        } = *self
        else {
            return f.write_str("off");
        };
        let us = max_wait.as_micros();
        let wait = if us == 0 {
            "0ms".to_string()
        } else if us % 1_000_000 == 0 {
            format!("{}s", us / 1_000_000)
        } else if us % 1_000 == 0 {
            format!("{}ms", us / 1_000)
        } else {
            format!("{us}us")
        };
        let bytes = [(30, "GiB"), (20, "MiB"), (10, "KiB")]
            .into_iter()
            .find(|&(shift, _)| max_bytes != 0 && max_bytes % (1u64 << shift) == 0)
            .map(|(shift, unit)| format!("{}{unit}", max_bytes >> shift))
            .unwrap_or_else(|| max_bytes.to_string());
        write!(f, "window {wait} {max_commits} {bytes}")
    }
}

impl Batching {
    /// A window needs room for at least one commit and one byte.
    pub fn validate(&self) -> Result<()> {
        if let Batching::Window {
            max_commits,
            max_bytes,
            ..
        } = *self
        {
            if max_commits == 0 {
                return Err(Error::Config(
                    "batching max_commits must be at least 1".into(),
                ));
            }
            if max_bytes == 0 {
                return Err(Error::Config(
                    "batching max_bytes must be at least 1".into(),
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
