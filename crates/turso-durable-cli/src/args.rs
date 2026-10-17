//! Hand-written command-line parsing.
use std::path::PathBuf;
use turso_durable::Batching;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Checkpoint {
    Passive,
    Full,
    Restart,
    Truncate,
    Reopen,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SoakArgs {
    pub dir: PathBuf,
    pub checkpoint: Option<Checkpoint>,
    pub every: u64,
    pub start: i64,
    pub count: Option<u64>,
    pub page_put_delay_ms: Option<u64>,
    /// Threads, one connection each (at least 1).
    pub connections: usize,
    /// Set on the database after each open.
    pub batching: Option<Batching>,
    /// Re-read before each commit when its mtime (or length) changed.
    pub batching_file: Option<PathBuf>,
    /// Print the upload stats every N acks.
    pub stats_every: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BenchArgs {
    pub dir: PathBuf,
    pub commits: u64,
    /// Threads sharing the commits, one connection each (at least 1).
    pub connections: usize,
    pub batching: Option<Batching>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Restore {
        dir: PathBuf,
    },
    Verify {
        dir: PathBuf,
        acked: Option<PathBuf>,
    },
    Sql {
        dir: PathBuf,
        statements: Vec<String>,
        /// Allowed to restore, claim or create the database (`is_claimed` is false).
        claim: bool,
        batching: Option<Batching>,
    },
    /// Like `Sql`, each argument its own commit, then the upload stats.
    Stats {
        dir: PathBuf,
        sql: Vec<String>,
        claim: bool,
        batching: Option<Batching>,
    },
    /// Restore into an empty directory and take the database over there.
    Takeover {
        dir: PathBuf,
    },
    /// Delete what an old enough head no longer reaches; no directory.
    Gc,
    Soak(SoakArgs),
    Bench(BenchArgs),
}

pub const USAGE: &str = "usage: turso-durable <command> [options]
  restore --dir DIR
  takeover --dir DIR
  gc
  verify  --dir DIR [--acked FILE]
  sql     --dir DIR [--claim] [--batching SPEC] SQL...
  stats   --dir DIR [--claim] [--batching SPEC] SQL...
  soak    --dir DIR [--checkpoint passive|full|restart|truncate|reopen] [--every N]
          [--start ID] [--count N] [--page-put-delay-ms N] [--connections N]
          [--batching SPEC] [--batching-file PATH] [--stats-every N]
  bench   --dir DIR --commits N [--connections N] [--batching SPEC]
SPEC is `off` or `window <max_wait> <max_commits> <max_bytes>` (e.g. 'window 10ms 64 1MiB');
without --batching, DURABLE_BATCHING applies.
S3 settings come from the DURABLE_* environment variables.";

fn num<T: std::str::FromStr>(flag: &str, v: &str) -> Result<T, String> {
    v.parse()
        .map_err(|_| format!("{flag} needs a number, not {v:?}"))
}

/// A number of at least 1.
fn positive<T: std::str::FromStr + PartialEq + From<u8>>(flag: &str, v: &str) -> Result<T, String> {
    let n: T = num(flag, v)?;
    if n == T::from(0) {
        return Err(format!("{flag} must be at least 1"));
    }
    Ok(n)
}

/// A batching setting in the PRAGMA's text, valid.
pub fn batching(flag: &str, v: &str) -> Result<Batching, String> {
    let b: Batching = v.parse().map_err(|e| format!("{flag}: {e}"))?;
    b.validate().map_err(|e| format!("{flag}: {e}"))?;
    Ok(b)
}

/// Parse the arguments after the program name.
pub fn parse(args: &[String]) -> Result<Command, String> {
    let (cmd, rest) = args.split_first().ok_or("missing command")?;
    let mut dir: Option<PathBuf> = None;
    let mut acked: Option<PathBuf> = None;
    let mut positional: Vec<String> = Vec::new();
    let mut checkpoint = None;
    let mut every = 100u64;
    let mut start = 1i64;
    let mut count = None;
    let mut delay = None;
    let mut commits = None;
    let mut claim = false;
    let mut connections = 1usize;
    let mut batching_spec = None;
    let mut batching_file = None;
    let mut stats_every = None;

    let mut it = rest.iter();
    while let Some(a) = it.next() {
        if !a.starts_with("--") {
            positional.push(a.clone());
            continue;
        }
        let mut value = |flag: &str| -> Result<String, String> {
            it.next()
                .cloned()
                .ok_or_else(|| format!("{flag} needs a value"))
        };
        let known = match (cmd.as_str(), a.as_str()) {
            (c, "--dir") if c != "gc" => {
                dir = Some(PathBuf::from(value(a)?));
                true
            }
            ("sql" | "stats", "--claim") => {
                claim = true;
                true
            }
            ("sql" | "stats" | "soak" | "bench", "--batching") => {
                batching_spec = Some(batching(a, &value(a)?)?);
                true
            }
            ("soak" | "bench", "--connections") => {
                connections = positive(a, &value(a)?)?;
                true
            }
            ("verify", "--acked") => {
                acked = Some(PathBuf::from(value(a)?));
                true
            }
            ("soak", "--checkpoint") => {
                let v = value(a)?;
                checkpoint = Some(match v.as_str() {
                    "passive" => Checkpoint::Passive,
                    "full" => Checkpoint::Full,
                    "restart" => Checkpoint::Restart,
                    "truncate" => Checkpoint::Truncate,
                    "reopen" => Checkpoint::Reopen,
                    _ => return Err(format!("unknown checkpoint mode {v:?}")),
                });
                true
            }
            ("soak", "--every") => {
                every = positive(a, &value(a)?)?;
                true
            }
            ("soak", "--start") => {
                start = num(a, &value(a)?)?;
                true
            }
            ("soak", "--count") => {
                count = Some(num(a, &value(a)?)?);
                true
            }
            ("soak", "--page-put-delay-ms") => {
                delay = Some(num(a, &value(a)?)?);
                true
            }
            ("soak", "--batching-file") => {
                batching_file = Some(PathBuf::from(value(a)?));
                true
            }
            ("soak", "--stats-every") => {
                stats_every = Some(positive(a, &value(a)?)?);
                true
            }
            ("bench", "--commits") => {
                commits = Some(num(a, &value(a)?)?);
                true
            }
            _ => false,
        };
        if !known {
            return Err(format!("unknown flag {a} for {cmd}"));
        }
    }

    if cmd == "gc" {
        return match positional.first() {
            Some(a) => Err(format!("unexpected argument {a:?}")),
            None => Ok(Command::Gc),
        };
    }
    let dir = dir.ok_or("--dir is required")?;
    if !matches!(cmd.as_str(), "sql" | "stats") && !positional.is_empty() {
        return Err(format!("unexpected argument {:?}", positional[0]));
    }
    match cmd.as_str() {
        "restore" => Ok(Command::Restore { dir }),
        "takeover" => Ok(Command::Takeover { dir }),
        "verify" => Ok(Command::Verify { dir, acked }),
        "sql" | "stats" if positional.is_empty() => {
            Err(format!("{cmd} needs at least one SQL argument"))
        }
        "sql" => Ok(Command::Sql {
            dir,
            statements: positional,
            claim,
            batching: batching_spec,
        }),
        "stats" => Ok(Command::Stats {
            dir,
            sql: positional,
            claim,
            batching: batching_spec,
        }),
        "soak" => Ok(Command::Soak(SoakArgs {
            dir,
            checkpoint,
            every,
            start,
            count,
            page_put_delay_ms: delay,
            connections,
            batching: batching_spec,
            batching_file,
            stats_every,
        })),
        "bench" => Ok(Command::Bench(BenchArgs {
            dir,
            commits: commits.ok_or("--commits is required")?,
            connections,
            batching: batching_spec,
        })),
        _ => Err(format!("unknown command {cmd:?}")),
    }
}

/// Ids from an acked file: lines `ack <id>` or a bare `<id>`. Any other line (events, errors,
/// a torn last line) is skipped; duplicates are kept so the caller can count them.
pub fn parse_acked(text: &str) -> Vec<i64> {
    text.lines()
        .filter_map(|line| {
            let line = line.trim();
            line.strip_prefix("ack ")
                .unwrap_or(line)
                .trim()
                .parse()
                .ok()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn p(a: &[&str]) -> Result<Command, String> {
        parse(&a.iter().map(|s| s.to_string()).collect::<Vec<_>>())
    }

    #[test]
    fn parses_each_command() {
        let d = PathBuf::from("d");
        assert_eq!(
            p(&["restore", "--dir", "d"]),
            Ok(Command::Restore { dir: d.clone() })
        );
        assert_eq!(
            p(&["verify", "--dir", "d", "--acked", "a.txt"]),
            Ok(Command::Verify {
                dir: d.clone(),
                acked: Some("a.txt".into())
            })
        );
        assert_eq!(
            p(&["verify", "--dir", "d"]),
            Ok(Command::Verify {
                dir: d.clone(),
                acked: None
            })
        );
        assert_eq!(
            p(&["sql", "--dir", "d", "SELECT 1", "SELECT 2"]),
            Ok(Command::Sql {
                dir: d.clone(),
                statements: vec!["SELECT 1".into(), "SELECT 2".into()],
                claim: false,
                batching: None,
            })
        );
        assert_eq!(
            p(&["sql", "--claim", "--dir", "d", "SELECT 1"]),
            Ok(Command::Sql {
                dir: d.clone(),
                statements: vec!["SELECT 1".into()],
                claim: true,
                batching: None,
            })
        );
        assert_eq!(
            p(&[
                "soak",
                "--dir",
                "d",
                "--checkpoint",
                "reopen",
                "--every",
                "7",
                "--start",
                "5",
                "--count",
                "50",
                "--page-put-delay-ms",
                "30"
            ]),
            Ok(Command::Soak(SoakArgs {
                dir: d.clone(),
                checkpoint: Some(Checkpoint::Reopen),
                every: 7,
                start: 5,
                count: Some(50),
                page_put_delay_ms: Some(30),
                connections: 1,
                batching: None,
                batching_file: None,
                stats_every: None,
            }))
        );
        match p(&["soak", "--dir", "d"]).unwrap() {
            Command::Soak(s) => {
                assert_eq!((s.checkpoint, s.start, s.count), (None, 1, None));
            }
            c => panic!("{c:?}"),
        }
        assert_eq!(
            p(&["bench", "--dir", "d", "--commits", "100"]),
            Ok(Command::Bench(BenchArgs {
                dir: d,
                commits: 100,
                connections: 1,
                batching: None,
            }))
        );
    }

    #[test]
    fn parses_takeover() {
        assert_eq!(
            p(&["takeover", "--dir", "d"]),
            Ok(Command::Takeover {
                dir: PathBuf::from("d")
            })
        );
        assert!(p(&["takeover"]).is_err(), "--dir is required");
        assert!(p(&["takeover", "--dir", "d", "--claim"]).is_err());
        assert!(p(&["takeover", "--dir", "d", "extra"]).is_err());
    }

    /// `gc` takes no directory: it works on the bucket alone.
    #[test]
    fn parses_gc() {
        assert_eq!(p(&["gc"]), Ok(Command::Gc));
        assert!(p(&["gc", "--dir", "x"]).is_err(), "--dir is a usage error");
        assert!(p(&["gc", "extra"]).is_err());
        assert!(p(&["gc", "--claim"]).is_err());
    }

    #[test]
    fn unknown_flag_is_usage_error() {
        assert!(p(&["restore", "--dir", "d", "--bogus"]).is_err());
        assert!(p(&["restore", "--dir", "d", "--acked", "x"]).is_err());
        assert!(p(&["nope", "--dir", "d"]).is_err());
        assert!(p(&["restore"]).is_err());
        assert!(p(&[]).is_err());
        assert!(p(&["bench", "--dir", "d"]).is_err());
        assert!(p(&["soak", "--dir", "d", "--checkpoint", "weird"]).is_err());
        assert!(p(&["soak", "--dir", "d", "--count", "x"]).is_err());
        assert!(p(&["sql", "--dir", "d"]).is_err());
        assert!(p(&["sql", "--dir", "d", "--claim"]).is_err());
        // --claim belongs to sql alone.
        assert!(p(&["restore", "--dir", "d", "--claim"]).is_err());
        assert!(p(&["verify", "--dir", "d", "--claim"]).is_err());
    }

    fn window(ms: u64, commits: u32, bytes: u64) -> Batching {
        Batching::Window {
            max_wait: Duration::from_millis(ms),
            max_commits: commits,
            max_bytes: bytes,
        }
    }

    #[test]
    fn parses_stats_and_batching() {
        let d = PathBuf::from("d");
        assert_eq!(
            p(&[
                "stats",
                "--dir",
                "d",
                "--batching",
                "window 10ms 8 64KiB",
                "INSERT INTO t VALUES (1)",
                "INSERT INTO t VALUES (2)"
            ]),
            Ok(Command::Stats {
                dir: d.clone(),
                sql: vec![
                    "INSERT INTO t VALUES (1)".into(),
                    "INSERT INTO t VALUES (2)".into()
                ],
                claim: false,
                batching: Some(window(10, 8, 64 << 10)),
            })
        );
        assert_eq!(
            p(&["stats", "--claim", "--dir", "d", "SELECT 1"]),
            Ok(Command::Stats {
                dir: d.clone(),
                sql: vec!["SELECT 1".into()],
                claim: true,
                batching: None,
            })
        );
        assert_eq!(
            p(&["sql", "--dir", "d", "--batching", "off", "SELECT 1"]),
            Ok(Command::Sql {
                dir: d.clone(),
                statements: vec!["SELECT 1".into()],
                claim: false,
                batching: Some(Batching::Off),
            })
        );
        assert_eq!(
            p(&[
                "bench",
                "--dir",
                "d",
                "--commits",
                "10",
                "--connections",
                "8",
                "--batching",
                "window 5ms 8 1MiB"
            ]),
            Ok(Command::Bench(BenchArgs {
                dir: d.clone(),
                commits: 10,
                connections: 8,
                batching: Some(window(5, 8, 1 << 20)),
            }))
        );
        match p(&["bench", "--dir", "d", "--commits", "1"]).unwrap() {
            Command::Bench(b) => assert_eq!((b.connections, b.batching), (1, None)),
            c => panic!("{c:?}"),
        }
    }

    #[test]
    fn parses_soak_connections_batching_file_and_stats_every() {
        match p(&[
            "soak",
            "--dir",
            "d",
            "--connections",
            "4",
            "--batching-file",
            "f",
            "--stats-every",
            "50",
            "--batching",
            "window 20ms 64 1MiB",
        ])
        .unwrap()
        {
            Command::Soak(s) => {
                assert_eq!(s.connections, 4);
                assert_eq!(s.batching_file, Some(PathBuf::from("f")));
                assert_eq!(s.stats_every, Some(50));
                assert_eq!(s.batching, Some(window(20, 64, 1 << 20)));
            }
            c => panic!("{c:?}"),
        }
        match p(&["soak", "--dir", "d"]).unwrap() {
            Command::Soak(s) => {
                assert_eq!(
                    (s.connections, s.batching_file, s.stats_every, s.batching),
                    (1, None, None, None)
                );
            }
            c => panic!("{c:?}"),
        }
    }

    #[test]
    fn bad_batching_and_connections_are_usage_errors() {
        for bad in ["nope", "window 10ms 0 1MiB", "window 10ms 8 0", "window"] {
            let e = p(&["stats", "--dir", "d", "--batching", bad, "SELECT 1"]).unwrap_err();
            assert!(e.contains("--batching"), "{e}");
        }
        assert!(p(&["soak", "--dir", "d", "--batching", "nope"]).is_err());
        assert!(p(&["sql", "--dir", "d", "--batching"]).is_err());
        assert!(p(&[
            "bench",
            "--dir",
            "d",
            "--commits",
            "5",
            "--connections",
            "0"
        ])
        .is_err());
        assert!(p(&["soak", "--dir", "d", "--connections", "0"]).is_err());
        assert!(p(&["soak", "--dir", "d", "--stats-every", "0"]).is_err());
        assert!(p(&["stats", "--dir", "d"]).is_err());
        // Each flag belongs to its commands alone.
        assert!(p(&["verify", "--dir", "d", "--batching", "off"]).is_err());
        assert!(p(&["restore", "--dir", "d", "--batching", "off"]).is_err());
        assert!(p(&["sql", "--dir", "d", "--connections", "2", "SELECT 1"]).is_err());
        assert!(p(&[
            "bench",
            "--dir",
            "d",
            "--commits",
            "5",
            "--stats-every",
            "2"
        ])
        .is_err());
        assert!(p(&[
            "bench",
            "--dir",
            "d",
            "--commits",
            "5",
            "--batching-file",
            "f"
        ])
        .is_err());
        assert!(p(&["soak", "--dir", "d", "--claim"]).is_err());
    }

    #[test]
    fn acked_file_parsing_accepts_both_forms() {
        let text = "ack 1\n2\n\nevent seal-started\nack  3 \nerror boom\n";
        assert_eq!(parse_acked(text), vec![1, 2, 3]);
        // Torn or odd lines never fail the parse.
        assert_eq!(parse_acked("ack 1\nack \nack x\n12abc\nack 2"), vec![1, 2]);
        // Duplicates are kept.
        assert_eq!(parse_acked("ack 1\nack 1\n"), vec![1, 1]);
    }
}
