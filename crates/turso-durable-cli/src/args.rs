//! Hand-written command-line parsing.
use std::path::PathBuf;

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
    },
    Soak(SoakArgs),
    Bench {
        dir: PathBuf,
        commits: u64,
    },
}

pub const USAGE: &str = "usage: turso-durable <command> [options]
  restore --dir DIR
  verify  --dir DIR [--acked FILE]
  sql     --dir DIR SQL...
  soak    --dir DIR [--checkpoint passive|full|restart|truncate|reopen] [--every N]
          [--start ID] [--count N] [--page-put-delay-ms N]
  bench   --dir DIR --commits N
S3 settings come from the DURABLE_* environment variables.";

fn num<T: std::str::FromStr>(flag: &str, v: &str) -> Result<T, String> {
    v.parse()
        .map_err(|_| format!("{flag} needs a number, not {v:?}"))
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
            (_, "--dir") => {
                dir = Some(PathBuf::from(value(a)?));
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
                every = num(a, &value(a)?)?;
                if every == 0 {
                    return Err("--every must be at least 1".into());
                }
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

    let dir = dir.ok_or("--dir is required")?;
    if cmd != "sql" && !positional.is_empty() {
        return Err(format!("unexpected argument {:?}", positional[0]));
    }
    match cmd.as_str() {
        "restore" => Ok(Command::Restore { dir }),
        "verify" => Ok(Command::Verify { dir, acked }),
        "sql" => {
            if positional.is_empty() {
                return Err("sql needs at least one SQL argument".into());
            }
            Ok(Command::Sql {
                dir,
                statements: positional,
            })
        }
        "soak" => Ok(Command::Soak(SoakArgs {
            dir,
            checkpoint,
            every,
            start,
            count,
            page_put_delay_ms: delay,
        })),
        "bench" => Ok(Command::Bench {
            dir,
            commits: commits.ok_or("--commits is required")?,
        }),
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
                statements: vec!["SELECT 1".into(), "SELECT 2".into()]
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
            Ok(Command::Bench {
                dir: d,
                commits: 100
            })
        );
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
