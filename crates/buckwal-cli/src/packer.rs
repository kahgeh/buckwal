//! `packer`: the packer loop (`buckwal::run_packer`) until SIGTERM or SIGINT, with a line
//! for each pack it makes.
use crate::say;
use crate::serve::Failure;
use buckwal::Event;
use buckwal_server::watch_signals;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::Arc;
use std::time::Duration;

/// How often the signal is looked at.
const POLL: Duration = Duration::from_millis(200);

pub fn run(dir: &Path) -> Result<(), Failure> {
    // Every setting is read before the directory or the bucket is touched.
    let mut cfg = crate::config(dir).map_err(Failure::Usage)?;
    let pack_dir = cfg.pack_dir.clone().unwrap_or_else(|| dir.to_path_buf());
    cfg.on_event = Some(Arc::new(|e: &Event| {
        if let Some(line) = event_line(e) {
            say(&line);
        }
    }));
    crate::make_pack_dir(&cfg, dir).map_err(Failure::Failed)?;
    let signal = watch_signals().map_err(Failure::Failed)?;
    let stop = Arc::new(AtomicBool::new(false));
    say(&format!("packer started pack_dir={}", pack_dir.display()));
    let loop_stop = stop.clone();
    let packer = std::thread::Builder::new()
        .name("buckwal-packer".to_string())
        .spawn(move || buckwal::run_packer(&cfg, loop_stop))
        .map_err(|e| Failure::Failed(format!("cannot start the packer thread: {e}")))?;
    while !packer.is_finished() {
        if signal.is_set() {
            stop.store(true, SeqCst);
        }
        std::thread::sleep(POLL);
    }
    let result = packer
        .join()
        .map_err(|_| Failure::Failed("the packer thread panicked".to_string()))?;
    result.map_err(|e| Failure::Failed(e.to_string()))?;
    say("packer stopped");
    Ok(())
}

/// The line for an event of the packer loop: one for each pack it made.
fn event_line(e: &Event) -> Option<String> {
    match e {
        Event::Packed {
            pack_generation,
            bytes,
            elapsed,
        } => Some(format!(
            "packed pack_generation={pack_generation} bytes={bytes} ms={}",
            elapsed.as_millis()
        )),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packer_prints_a_line_for_each_pack() {
        let e = Event::Packed {
            pack_generation: 7,
            bytes: 8192,
            elapsed: Duration::from_millis(1500),
        };
        assert_eq!(
            event_line(&e).as_deref(),
            Some("packed pack_generation=7 bytes=8192 ms=1500")
        );
        assert_eq!(event_line(&Event::Trimmed { deleted: 1 }), None);
    }
}
