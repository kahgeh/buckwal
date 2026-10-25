//! Hand-written command-line parsing.
use buckwal::{Batching, JournalMode};
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Checkpoint {
    Passive,
    Full,
    Restart,
    Truncate,
    Reopen,
}

/// How `soak` wraps each write.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Concurrent {
    /// Autocommit, as always.
    #[default]
    Off,
    /// Every write in `BEGIN CONCURRENT ... COMMIT`.
    All,
    /// Even-numbered threads write in plain `BEGIN ... COMMIT`, odd-numbered ones in
    /// `BEGIN CONCURRENT ... COMMIT`.
    Mixed,
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
    pub concurrent: Concurrent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BenchArgs {
    pub dir: PathBuf,
    pub commits: u64,
    /// Threads sharing the commits, one connection each (at least 1).
    pub connections: usize,
    pub batching: Option<Batching>,
}

/// What `scan --expect` asks of the bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expect {
    /// No needle in any object.
    Absent,
    /// `CREATE TABLE` and every marker found, the key not.
    Present,
}

impl std::fmt::Display for Expect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Expect::Absent => "absent",
            Expect::Present => "present",
        })
    }
}

/// `scan`, a test command (not in the usage): plaintext needles in the bucket's objects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanArgs {
    pub expect: Expect,
    /// The whole bucket, not just `<prefix>/`.
    pub all: bool,
    pub markers: Vec<String>,
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
    /// Serve the database over tursodb's SQL over HTTP v3 until SIGTERM or SIGINT.
    Serve {
        dir: PathBuf,
        listen: Option<String>,
        claim: bool,
        batching: Option<Batching>,
        /// `--pack on|off`: the packer thread of the writer; overrides `BUCKWAL_PACK`.
        pack: Option<bool>,
    },
    /// One pack run now; `dir` is the pack directory (`BUCKWAL_PACK_DIR` overrides it).
    Pack {
        dir: PathBuf,
    },
    /// The packer loop until SIGTERM or SIGINT; `dir` is the pack directory
    /// (`BUCKWAL_PACK_DIR` overrides it).
    Packer {
        dir: PathBuf,
    },
    /// Follow the bucket's database as a read replica until SIGTERM or SIGINT.
    Follow {
        dir: PathBuf,
        /// Overrides `BUCKWAL_REPLICA_POLL_MS`.
        poll_ms: Option<u64>,
    },
    /// Print what to set up on the bucket for replica notifications; runs nothing.
    NotifySetup {
        bucket: String,
        replicas: u32,
        region: Option<String>,
        topic: Option<String>,
    },
    Soak(SoakArgs),
    Bench(BenchArgs),
    Scan(ScanArgs),
}

pub const USAGE: &str = "usage: buckwal <command> [options]
  restore --dir DIR
  gc
  verify  --dir DIR [--acked FILE]
  sql     --dir DIR [--claim] [--batching SPEC] [--journal-mode wal|mvcc] SQL...
  stats   --dir DIR [--claim] [--batching SPEC] [--journal-mode wal|mvcc] SQL...
  soak    --dir DIR [--checkpoint passive|full|restart|truncate|reopen] [--every N]
          [--start ID] [--count N] [--page-put-delay-ms N] [--connections N]
          [--batching SPEC] [--batching-file PATH] [--stats-every N]
          [--journal-mode wal|mvcc] [--concurrent [all|mixed]]
  bench   --dir DIR --commits N [--connections N] [--batching SPEC] [--journal-mode wal|mvcc]
  serve   --dir DIR [--listen IP:PORT] [--claim] [--batching SPEC] [--journal-mode wal|mvcc]
          [--pack on|off]
  takeover --dir DIR [--journal-mode wal|mvcc]
  follow  --dir DIR [--poll-ms N]
  notify-setup --bucket B --replicas N [--region R] [--topic NAME]
  pack    --dir DIR
  packer  --dir DIR
--journal-mode is the mode a new database is created in (an existing one keeps its own; asking
for the other is refused); without it, BUCKWAL_JOURNAL_MODE applies; with neither, an existing
database's own mode is used and a new one is wal. restore and verify follow the bucket head's mode.
--concurrent (alone, or `all`) runs every soak write in BEGIN CONCURRENT ... COMMIT (MVCC);
`mixed` has even-numbered threads use plain BEGIN and odd-numbered ones BEGIN CONCURRENT.
SPEC is `off` or `window <max_wait> <max_commits> <max_bytes>` (e.g. 'window 10ms 64 1MiB');
without --batching, BUCKWAL_BATCHING applies.
follow runs a read replica until SIGTERM or SIGINT and prints a status line per event; --poll-ms
overrides BUCKWAL_REPLICA_POLL_MS, and BUCKWAL_REPLICA_NOTIFY_* start the notification listener.
notify-setup only prints the AWS CLI commands and bucket notification JSON (and RustFS
equivalents, untested) that route bucket events to N replicas; it runs nothing.
pack runs one pack run now and exits: 0 when it packed, found nothing to do or was superseded,
1 when it failed. packer runs the packer loop until SIGTERM or SIGINT. Both need the key file
of an encrypted bucket. DIR is their pack directory (the kept copy, its lock and the working
files); BUCKWAL_PACK_DIR replaces it. When a packer runs on another machine, set BUCKWAL_PACK=0
on the writer to stop its packer thread; serve --pack on|off replaces BUCKWAL_PACK for serve.
The packer settings: BUCKWAL_PACK, BUCKWAL_PACK_DIR, BUCKWAL_PACK_LOG_PERCENT,
BUCKWAL_PACK_MAX_AGE_SECS, BUCKWAL_PACK_ORPHAN_GRACE_SECS and BUCKWAL_PACK_KEEP_SECS.
S3 settings come from the BUCKWAL_* environment variables; serve also reads BUCKWAL_SERVE_*.";

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

/// An on or off value, as `BUCKWAL_PACK` takes it: `1`, `true`, `on`, `0`, `false` or `off`.
fn on_off(flag: &str, v: &str) -> Result<bool, String> {
    match v.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "on" => Ok(true),
        "0" | "false" | "off" => Ok(false),
        _ => Err(format!("{flag} is on or off, not {v:?}")),
    }
}

/// A batching setting in the PRAGMA's text, valid.
pub fn batching(flag: &str, v: &str) -> Result<Batching, String> {
    let b: Batching = v.parse().map_err(|e| format!("{flag}: {e}"))?;
    b.validate().map_err(|e| format!("{flag}: {e}"))?;
    Ok(b)
}

/// Parse the arguments after the program name.
#[cfg(test)]
pub fn parse(args: &[String]) -> Result<Command, String> {
    parse_with_mode(args).map(|(c, _)| c)
}

/// [`parse`], and the `--journal-mode` given, if any (only `sql`, `stats`, `soak`, `bench`,
/// `serve` and `takeover` take it).
pub fn parse_with_mode(args: &[String]) -> Result<(Command, Option<JournalMode>), String> {
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
    let mut listen = None;
    let mut connections = 1usize;
    let mut batching_spec = None;
    let mut batching_file = None;
    let mut stats_every = None;
    let mut journal_mode = None;
    let mut concurrent = Concurrent::Off;
    let mut expect = None;
    let mut all = false;
    let mut markers = Vec::new();
    let mut poll_ms = None;
    let mut bucket: Option<String> = None;
    let mut replicas: Option<u32> = None;
    let mut region = None;
    let mut topic = None;
    let mut pack = None;

    let mut it = rest.iter().peekable();
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
            (c, "--dir") if !matches!(c, "gc" | "scan" | "notify-setup") => {
                dir = Some(PathBuf::from(value(a)?));
                true
            }
            ("sql" | "stats" | "serve", "--claim") => {
                claim = true;
                true
            }
            ("sql" | "stats" | "soak" | "bench" | "serve", "--batching") => {
                batching_spec = Some(batching(a, &value(a)?)?);
                true
            }
            ("sql" | "stats" | "soak" | "bench" | "serve" | "takeover", "--journal-mode") => {
                let v = value(a)?;
                journal_mode = Some(match v.as_str() {
                    "wal" => JournalMode::Wal,
                    "mvcc" => JournalMode::Mvcc,
                    _ => return Err(format!("{a} is wal or mvcc, not {v:?}")),
                });
                true
            }
            ("soak", "--concurrent") => {
                concurrent = match it.peek().map(|s| s.as_str()) {
                    Some("mixed") => {
                        it.next();
                        Concurrent::Mixed
                    }
                    Some("all") => {
                        it.next();
                        Concurrent::All
                    }
                    _ => Concurrent::All,
                };
                true
            }
            ("soak" | "bench", "--connections") => {
                connections = positive(a, &value(a)?)?;
                true
            }
            ("serve", "--listen") => {
                listen = Some(value(a)?);
                true
            }
            ("serve", "--pack") => {
                pack = Some(on_off(a, &value(a)?)?);
                true
            }
            ("follow", "--poll-ms") => {
                poll_ms = Some(positive(a, &value(a)?)?);
                true
            }
            ("notify-setup", "--bucket") => {
                bucket = Some(value(a)?);
                true
            }
            ("notify-setup", "--replicas") => {
                replicas = Some(positive(a, &value(a)?)?);
                true
            }
            ("notify-setup", "--region") => {
                region = Some(value(a)?);
                true
            }
            ("notify-setup", "--topic") => {
                topic = Some(value(a)?);
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
            ("scan", "--expect") => {
                let v = value(a)?;
                expect = Some(match v.as_str() {
                    "absent" => Expect::Absent,
                    "present" => Expect::Present,
                    _ => return Err(format!("--expect is absent or present, not {v:?}")),
                });
                true
            }
            ("scan", "--all") => {
                all = true;
                true
            }
            ("scan", "--marker") => {
                markers.push(value(a)?);
                true
            }
            _ => false,
        };
        if !known {
            return Err(format!("unknown flag {a} for {cmd}"));
        }
    }

    let parsed = |c: Command| Ok((c, journal_mode));
    if cmd == "gc" || cmd == "scan" {
        if let Some(a) = positional.first() {
            return Err(format!("unexpected argument {a:?}"));
        }
        if cmd == "gc" {
            return parsed(Command::Gc);
        }
        return parsed(Command::Scan(ScanArgs {
            expect: expect.ok_or("--expect is required")?,
            all,
            markers,
        }));
    }
    if cmd == "notify-setup" {
        if let Some(a) = positional.first() {
            return Err(format!("unexpected argument {a:?}"));
        }
        return parsed(Command::NotifySetup {
            bucket: bucket.ok_or("--bucket is required")?,
            replicas: replicas.ok_or("--replicas is required")?,
            region,
            topic,
        });
    }
    let dir = dir.ok_or("--dir is required")?;
    if !matches!(cmd.as_str(), "sql" | "stats") && !positional.is_empty() {
        return Err(format!("unexpected argument {:?}", positional[0]));
    }
    let command = match cmd.as_str() {
        "restore" => Command::Restore { dir },
        "takeover" => Command::Takeover { dir },
        "verify" => Command::Verify { dir, acked },
        "sql" | "stats" if positional.is_empty() => {
            return Err(format!("{cmd} needs at least one SQL argument"));
        }
        "sql" => Command::Sql {
            dir,
            statements: positional,
            claim,
            batching: batching_spec,
        },
        "stats" => Command::Stats {
            dir,
            sql: positional,
            claim,
            batching: batching_spec,
        },
        "follow" => Command::Follow { dir, poll_ms },
        "serve" => Command::Serve {
            dir,
            listen,
            claim,
            batching: batching_spec,
            pack,
        },
        "pack" => Command::Pack { dir },
        "packer" => Command::Packer { dir },
        "soak" => Command::Soak(SoakArgs {
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
            concurrent,
        }),
        "bench" => Command::Bench(BenchArgs {
            dir,
            commits: commits.ok_or("--commits is required")?,
            connections,
            batching: batching_spec,
        }),
        _ => return Err(format!("unknown command {cmd:?}")),
    };
    parsed(command)
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
                concurrent: Concurrent::Off,
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
    fn parses_serve() {
        assert_eq!(
            p(&[
                "serve",
                "--dir",
                "d",
                "--listen",
                "127.0.0.1:0",
                "--claim",
                "--batching",
                "off"
            ]),
            Ok(Command::Serve {
                dir: "d".into(),
                listen: Some("127.0.0.1:0".into()),
                claim: true,
                batching: Some(Batching::Off),
                pack: None,
            })
        );
        assert_eq!(
            p(&["serve", "--dir", "d"]),
            Ok(Command::Serve {
                dir: "d".into(),
                listen: None,
                claim: false,
                batching: None,
                pack: None,
            })
        );
        assert!(p(&["serve"]).is_err());
        assert!(p(&["serve", "--dir", "d", "extra"]).is_err());
        assert!(p(&["serve", "--dir", "d", "--listen"]).is_err());
        for c in ["sql", "stats", "verify", "restore", "soak", "bench"] {
            assert!(
                p(&[c, "--dir", "d", "--listen", "127.0.0.1:0", "SELECT 1"]).is_err(),
                "{c}"
            );
        }
    }

    #[test]
    fn args_parse_follow_and_notify_setup() {
        assert_eq!(
            p(&["follow", "--dir", "d"]),
            Ok(Command::Follow {
                dir: "d".into(),
                poll_ms: None
            })
        );
        assert_eq!(
            p(&["follow", "--dir", "d", "--poll-ms", "250"]),
            Ok(Command::Follow {
                dir: "d".into(),
                poll_ms: Some(250)
            })
        );
        assert!(p(&["follow", "--dir", "d", "--poll-ms", "0"]).is_err());
        assert!(p(&["follow", "--dir", "d", "--poll-ms", "x"]).is_err());
        assert!(p(&["follow", "--dir", "d", "--claim"]).is_err());
        assert!(p(&["follow", "--dir", "d", "extra"]).is_err());
        assert!(p(&["sql", "--dir", "d", "--poll-ms", "5", "SELECT 1"]).is_err());
        assert_eq!(
            p(&["notify-setup", "--bucket", "b-1", "--replicas", "2"]),
            Ok(Command::NotifySetup {
                bucket: "b-1".into(),
                replicas: 2,
                region: None,
                topic: None
            })
        );
        assert_eq!(
            p(&[
                "notify-setup",
                "--replicas",
                "1",
                "--bucket",
                "b-1",
                "--region",
                "eu-west-1",
                "--topic",
                "t"
            ]),
            Ok(Command::NotifySetup {
                bucket: "b-1".into(),
                replicas: 1,
                region: Some("eu-west-1".into()),
                topic: Some("t".into())
            })
        );
        assert!(p(&[
            "notify-setup",
            "--bucket",
            "b",
            "--replicas",
            "1",
            "--dir",
            "d"
        ])
        .is_err());
        assert!(p(&["notify-setup", "--bucket", "b", "--replicas", "1", "x"]).is_err());
        assert!(USAGE.contains("follow") && USAGE.contains("notify-setup"));
    }

    #[test]
    fn follow_requires_dir() {
        let e = p(&["follow"]).unwrap_err();
        assert!(e.contains("--dir"), "{e}");
        assert!(p(&["follow", "--poll-ms", "5"]).is_err());
    }

    #[test]
    fn notify_setup_requires_bucket_and_replicas() {
        assert!(p(&["notify-setup"]).unwrap_err().contains("--bucket"));
        assert!(p(&["notify-setup", "--bucket", "b"])
            .unwrap_err()
            .contains("--replicas"));
        assert!(p(&["notify-setup", "--replicas", "2"])
            .unwrap_err()
            .contains("--bucket"));
        assert!(p(&["notify-setup", "--bucket", "b", "--replicas", "0"]).is_err());
        assert!(p(&["notify-setup", "--bucket", "b", "--replicas", "-1"]).is_err());
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

    /// `pack --dir DIR`: one pack run, with `DIR` as the pack directory.
    #[test]
    fn pack_subcommand_parses() {
        assert_eq!(
            p(&["pack", "--dir", "d"]),
            Ok(Command::Pack { dir: "d".into() })
        );
        assert!(p(&["pack"]).unwrap_err().contains("--dir"));
        for bad in [
            &["pack", "--dir", "d", "extra"][..],
            &["pack", "--dir", "d", "--claim"],
            &["pack", "--dir", "d", "--pack", "on"],
            &["pack", "--dir", "d", "--journal-mode", "wal"],
            &["pack", "--dir"],
        ] {
            assert!(p(bad).is_err(), "{bad:?}");
        }
        assert!(USAGE.contains("pack    --dir DIR"), "{USAGE}");
    }

    /// `packer --dir DIR`: the packer loop, with `DIR` as the pack directory.
    #[test]
    fn packer_subcommand_parses() {
        assert_eq!(
            p(&["packer", "--dir", "d"]),
            Ok(Command::Packer { dir: "d".into() })
        );
        assert!(p(&["packer"]).unwrap_err().contains("--dir"));
        for bad in [
            &["packer", "--dir", "d", "extra"][..],
            &["packer", "--dir", "d", "--poll-ms", "5"],
            &["packer", "--dir", "d", "--pack", "off"],
        ] {
            assert!(p(bad).is_err(), "{bad:?}");
        }
        assert!(USAGE.contains("packer  --dir DIR"), "{USAGE}");
        for var in ["BUCKWAL_PACK_DIR", "BUCKWAL_PACK=0"] {
            assert!(USAGE.contains(var), "{var}: {USAGE}");
        }
    }

    /// `serve --pack on|off` sets the packer thread; it takes the values `BUCKWAL_PACK` takes,
    /// and only `serve` takes it.
    #[test]
    fn serve_pack_flag_parses() {
        let pack = |a: &[&str]| match p(a).unwrap() {
            Command::Serve { pack, .. } => pack,
            c => panic!("{c:?}"),
        };
        assert_eq!(pack(&["serve", "--dir", "d"]), None);
        for (v, want) in [
            ("on", true),
            ("1", true),
            ("true", true),
            ("off", false),
            ("0", false),
            ("false", false),
        ] {
            assert_eq!(
                pack(&["serve", "--dir", "d", "--pack", v]),
                Some(want),
                "{v}"
            );
        }
        let e = p(&["serve", "--dir", "d", "--pack", "maybe"]).unwrap_err();
        assert!(e.contains("--pack"), "{e}");
        assert!(p(&["serve", "--dir", "d", "--pack"]).is_err());
        assert!(p(&["sql", "--dir", "d", "--pack", "on", "SELECT 1"]).is_err());
        assert!(USAGE.contains("[--pack on|off]"), "{USAGE}");
    }

    /// `gc` takes no directory: it works on the bucket alone.
    #[test]
    fn parses_gc() {
        assert_eq!(p(&["gc"]), Ok(Command::Gc));
        assert!(p(&["gc", "--dir", "x"]).is_err(), "--dir is a usage error");
        assert!(p(&["gc", "extra"]).is_err());
        assert!(p(&["gc", "--claim"]).is_err());
    }

    /// `scan` takes no directory; `--expect` is required and `--marker` repeats.
    #[test]
    fn parses_scan() {
        assert_eq!(
            p(&["scan", "--expect", "present"]),
            Ok(Command::Scan(ScanArgs {
                expect: Expect::Present,
                all: false,
                markers: vec![],
            }))
        );
        assert_eq!(
            p(&["scan", "--all", "--marker", "m1", "--expect", "absent", "--marker", "m2"]),
            Ok(Command::Scan(ScanArgs {
                expect: Expect::Absent,
                all: true,
                markers: vec!["m1".into(), "m2".into()],
            }))
        );
        assert!(p(&["scan"]).is_err(), "--expect is required");
        assert!(p(&["scan", "--expect", "maybe"]).is_err());
        assert!(p(&["scan", "--expect", "absent", "--dir", "d"]).is_err());
        assert!(p(&["scan", "--expect", "absent", "extra"]).is_err());
        assert!(p(&["scan", "--expect", "absent", "--marker"]).is_err());
        assert!(p(&["gc", "--all"]).is_err());
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

    fn pm(a: &[&str]) -> Result<(Command, Option<JournalMode>), String> {
        parse_with_mode(&a.iter().map(|s| s.to_string()).collect::<Vec<_>>())
    }

    #[test]
    fn journal_mode_flag_on_the_commands_that_open_a_database() {
        for argv in [
            &["sql", "--dir", "d", "--journal-mode", "mvcc", "SELECT 1"][..],
            &["stats", "--dir", "d", "--journal-mode", "mvcc", "SELECT 1"],
            &["soak", "--dir", "d", "--journal-mode", "mvcc"],
            &[
                "bench",
                "--dir",
                "d",
                "--commits",
                "1",
                "--journal-mode",
                "mvcc",
            ],
            &["serve", "--dir", "d", "--journal-mode", "mvcc"],
            &["takeover", "--dir", "d", "--journal-mode", "mvcc"],
        ] {
            assert_eq!(pm(argv).unwrap().1, Some(JournalMode::Mvcc), "{argv:?}");
        }
        let (_, m) = pm(&["sql", "--dir", "d", "--journal-mode", "wal", "SELECT 1"]).unwrap();
        assert_eq!(m, Some(JournalMode::Wal));
        // Without the flag nothing is chosen: BUCKWAL_JOURNAL_MODE applies.
        assert_eq!(pm(&["soak", "--dir", "d"]).unwrap().1, None);
        assert_eq!(pm(&["gc"]).unwrap().1, None);
        // A bad or missing value, and a command that has no use for it.
        let e = pm(&["sql", "--dir", "d", "--journal-mode", "delete", "SELECT 1"]).unwrap_err();
        assert!(
            e.contains("--journal-mode") && e.contains("wal or mvcc"),
            "{e}"
        );
        assert!(pm(&["soak", "--dir", "d", "--journal-mode"]).is_err());
        for c in ["restore", "verify", "gc", "scan"] {
            assert!(
                pm(&[c, "--dir", "d", "--journal-mode", "mvcc"]).is_err(),
                "{c}"
            );
        }
        assert!(
            USAGE.contains("--journal-mode wal|mvcc") && USAGE.contains("BUCKWAL_JOURNAL_MODE")
        );
    }

    #[test]
    fn soak_concurrent_flag() {
        let c = |a: &[&str]| match p(a).unwrap() {
            Command::Soak(s) => s.concurrent,
            c => panic!("{c:?}"),
        };
        assert_eq!(c(&["soak", "--dir", "d"]), Concurrent::Off);
        assert_eq!(c(&["soak", "--dir", "d", "--concurrent"]), Concurrent::All);
        assert_eq!(c(&["soak", "--concurrent", "--dir", "d"]), Concurrent::All);
        assert_eq!(
            c(&["soak", "--dir", "d", "--concurrent", "all"]),
            Concurrent::All
        );
        assert_eq!(
            c(&["soak", "--dir", "d", "--concurrent", "mixed"]),
            Concurrent::Mixed
        );
        assert_eq!(
            c(&["soak", "--dir", "d", "--concurrent", "--connections", "8"]),
            Concurrent::All
        );
        assert!(
            p(&["soak", "--dir", "d", "--concurrent", "bogus"]).is_err(),
            "positional"
        );
        for cmd in ["sql", "bench", "serve", "restore"] {
            assert!(
                p(&[cmd, "--dir", "d", "--concurrent", "SELECT 1"]).is_err(),
                "{cmd}"
            );
        }
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
