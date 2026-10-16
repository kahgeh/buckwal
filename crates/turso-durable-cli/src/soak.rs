//! `soak`: commit one 1 KB row at a time, printing `ack <id>` after each durable commit.
use crate::args::{Checkpoint, SoakArgs};
use crate::{config, err, say, store, CliResult};
use std::sync::Arc;
use std::time::Duration;
use turso_durable::{CheckpointMode, Config, Connection, Database, Event, Value};

fn open_db(args: &SoakArgs) -> CliResult<Database> {
    let mut cfg: Config = config(&args.dir)?;
    cfg.on_event = Some(Arc::new(|e: &Event| match e {
        Event::SealStarted { .. } => say("event seal-started"),
        Event::SealFinished { .. } => say("event seal-finished"),
        _ => {}
    }));
    match args.page_put_delay_ms {
        Some(ms) => {
            let s = store(&cfg, Some(ms))?;
            turso_durable::open_with_store(cfg, s).map_err(err)
        }
        None => turso_durable::open(cfg).map_err(err),
    }
}

/// Open the database and its connection, retrying until it works.
fn open_all(args: &SoakArgs) -> CliResult<(Database, Connection)> {
    loop {
        let attempt = open_db(args).and_then(|db| {
            let conn = db.connect().map_err(err)?;
            conn.execute(
                "CREATE TABLE IF NOT EXISTS soak(id INTEGER PRIMARY KEY, payload BLOB)",
                &[],
            )
            .map_err(err)?;
            Ok((db, conn))
        });
        match attempt {
            Ok(x) => return Ok(x),
            Err(m) => {
                say(&format!("error {m}"));
                std::thread::sleep(Duration::from_millis(500));
            }
        }
    }
}

fn mode(c: Checkpoint) -> Option<CheckpointMode> {
    match c {
        Checkpoint::Passive => Some(CheckpointMode::Passive),
        Checkpoint::Full => Some(CheckpointMode::Full),
        Checkpoint::Restart => Some(CheckpointMode::Restart),
        Checkpoint::Truncate => Some(CheckpointMode::Truncate),
        Checkpoint::Reopen => None,
    }
}

pub fn run(args: &SoakArgs) -> CliResult<()> {
    let (mut db, mut conn) = open_all(args)?;
    let mut id = args.start;
    let mut done = 0u64;
    while args.count.is_none_or(|c| done < c) {
        let payload = vec![(id & 0xff) as u8; 1024];
        let blob = Value::from_slice(&payload).map_err(err)?;
        let r = conn.execute(
            "INSERT OR REPLACE INTO soak(id, payload) VALUES (?, ?)",
            &[Value::from_i64(id), blob],
        );
        match r {
            Ok(_) => say(&format!("ack {id}")),
            Err(e) => {
                say(&format!("error {e}"));
                std::thread::sleep(Duration::from_millis(500));
                continue;
            }
        }
        id += 1;
        done += 1;
        let Some(cp) = args.checkpoint else { continue };
        if !done.is_multiple_of(args.every) {
            continue;
        }
        match mode(cp) {
            Some(m) => {
                if let Err(e) = conn.checkpoint(m) {
                    say(&format!("error {e}"));
                }
            }
            None => {
                if let Err(e) = conn.close() {
                    say(&format!("error {e}"));
                }
                if let Err(e) = db.close() {
                    say(&format!("error {e}"));
                }
                (db, conn) = open_all(args)?;
            }
        }
    }
    conn.close().map_err(err)?;
    db.close().map_err(err)
}
