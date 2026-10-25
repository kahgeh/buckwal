//! The kept copy of a packer (spec "The kept copy"): the file a pack run made, kept after the
//! run as `<pack dir>/pack-copy/app.db`, so the next run starts from it and downloads only the
//! pack's codes and the log. `<pack dir>/pack-copy/state.json` names the pack that the file
//! equals (its key, pack generation, size and `database_id`) and holds the `applying` flag.
//!
//! A run takes the exclusive lock on `<pack dir>/pack-copy/lock` ([`CopyLock`]) before it
//! touches the copy, and holds it until it ends. A run that cannot take the lock at once does
//! not touch the copy. The lock is the operating system's file lock (`fcntl`, the lock of
//! `durable.lock`), so a crashed run releases it. An `fcntl` lock is per process, so a
//! registry of the copies this process holds keeps two runs of one process apart too.
//!
//! A run uses the copy only when the state file exists with the flag off, the pointer names
//! the state's pack (same key, pack generation, size and `database_id`), and the file has that
//! size with each 1 MiB block matching the pack's codes ([`KeptCopy::source_for`]). Else it
//! deletes the copy and downloads the pack. It sets the flag and fsyncs the state file before
//! it changes the file ([`KeptCopy::set_applying`]); when its pack is the pointer's, it writes
//! the new pack into the state and clears the flag ([`KeptCopy::keep`]). A run that ends in any
//! other way deletes the copy (a drop of a dirty `KeptCopy` included).
use super::codes::{PackCodes, BLOCK_SIZE};
use super::PackPointer;
use crate::error::{Error, Result};
use crate::files::{io_err, DB_FILE};
use crate::format::json::{escape, Obj};
use crate::format::Keys;
use crate::restore::{fsync_dir, write_synced};
use crate::store::{with_retry, ObjectStore, RetryPolicy};
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::{Arc, LazyLock, Mutex};
use turso_core::{OpenFlags, PlatformIO, IO};

/// The kept copy's directory in the pack directory.
pub(crate) const COPY_DIR: &str = "pack-copy";
/// What the copy holds (`CopyState`).
pub(crate) const STATE_FILE: &str = "state.json";
/// The file a run locks.
pub(crate) const LOCK_FILE: &str = "lock";
/// The state file before its rename.
const STATE_TMP: &str = "state.json.tmp";
/// The format of the state file.
const STATE_FORMAT: u64 = 1;

/// Where a pack run got the database file that it applied the log to
/// (`buckwal.pack.source`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PackSource {
    /// From its kept copy: it downloaded only the pack's codes.
    Kept,
    /// It downloaded the pack (or, with no pack yet, built the first one), for this reason.
    Downloaded(DownloadReason),
}

/// Why a pack run did not use its kept copy (`buckwal.pack.reason`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownloadReason {
    /// There was no kept copy (no state file, for example a new pack directory), or no pack.
    None,
    /// The pointer names another pack than the one the copy equals: another packer moved it.
    Moved,
    /// The `applying` flag was on: a run stopped while it applied the log to the copy.
    Applying,
    /// The copy's size or a block's code was not the pack's, or the state file was not valid.
    CheckFailed,
    /// Another run held the lock on the kept copy: this run worked in its own file.
    Locked,
}

impl PackSource {
    /// `kept` or `downloaded`.
    pub fn as_str(&self) -> &'static str {
        match self {
            PackSource::Kept => "kept",
            PackSource::Downloaded(_) => "downloaded",
        }
    }

    /// `none`, `moved`, `applying`, `check_failed` or `locked` (`none` for `kept`).
    pub fn reason(&self) -> &'static str {
        match self {
            PackSource::Kept | PackSource::Downloaded(DownloadReason::None) => "none",
            PackSource::Downloaded(DownloadReason::Moved) => "moved",
            PackSource::Downloaded(DownloadReason::Applying) => "applying",
            PackSource::Downloaded(DownloadReason::CheckFailed) => "check_failed",
            PackSource::Downloaded(DownloadReason::Locked) => "locked",
        }
    }
}

/// The pack that the kept copy equals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CopyPack {
    pub pack_key: String,
    pub pack_generation: u64,
    pub size: u64,
    pub database_id: Option<String>,
}

impl CopyPack {
    pub(crate) fn of(p: &PackPointer) -> CopyPack {
        CopyPack {
            pack_key: p.pack_key.clone(),
            pack_generation: p.pack_generation,
            size: p.size,
            database_id: p.database_id.clone(),
        }
    }
}

/// What `state.json` holds. `pack` is `None` while a run builds a first pack.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CopyState {
    pub pack: Option<CopyPack>,
    pub applying: bool,
}

impl CopyState {
    fn encode(&self) -> Vec<u8> {
        let mut out = format!("{{\"format\":{STATE_FORMAT},\"applying\":{}", self.applying);
        if let Some(p) = &self.pack {
            out.push_str(&format!(
                ",\"pack_key\":{},\"pack_generation\":{},\"size\":{}",
                escape(&p.pack_key),
                p.pack_generation,
                p.size
            ));
            if let Some(id) = &p.database_id {
                out.push_str(&format!(",\"database_id\":{}", escape(id)));
            }
        }
        out.push_str("}\n");
        out.into_bytes()
    }

    fn decode(b: &[u8]) -> Result<CopyState> {
        let o = Obj::parse(b)?;
        if o.num("format")? != STATE_FORMAT {
            return Err(Error::Corrupt(
                "pack-copy/state.json: unknown format".into(),
            ));
        }
        let pack = if o.has("pack_key") {
            Some(CopyPack {
                pack_key: o.str("pack_key")?.to_string(),
                pack_generation: o.num("pack_generation")?,
                size: o.num("size")?,
                database_id: if o.has("database_id") {
                    Some(o.str("database_id")?.to_string())
                } else {
                    None
                },
            })
        } else {
            None
        };
        Ok(CopyState {
            pack,
            applying: o.bool("applying")?,
        })
    }
}

/// The state file of the copy in `copy_dir`: `None` when there is none, `Err` when it cannot
/// be read or is not valid.
pub(crate) fn read_state(copy_dir: &Path) -> Result<Option<CopyState>> {
    match fs::read(copy_dir.join(STATE_FILE)) {
        Ok(b) => CopyState::decode(&b).map(Some),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(io_err("read pack-copy/state.json", e)),
    }
}

/// The kept copies this process holds the lock of (canonical paths of `pack-copy/`).
static HELD: LazyLock<Mutex<HashSet<PathBuf>>> = LazyLock::new(Default::default);

/// The exclusive lock on `<pack dir>/pack-copy/lock`, held until drop.
pub(crate) struct CopyLock {
    dir: PathBuf,
    file: Option<Arc<dyn turso_core::File>>,
}

impl CopyLock {
    /// Take the lock of the kept copy in `pack_dir` at once, or `None` when another run (of
    /// this process or of another one) holds it. Makes `pack-copy/` when it is missing.
    pub(crate) fn try_take(pack_dir: &Path) -> Result<Option<CopyLock>> {
        let dir = pack_dir.join(COPY_DIR);
        fs::create_dir_all(&dir).map_err(|e| io_err(&format!("create {}", dir.display()), e))?;
        let dir =
            fs::canonicalize(&dir).map_err(|e| io_err(&format!("resolve {}", dir.display()), e))?;
        if !crate::db::lock(&HELD).insert(dir.clone()) {
            return Ok(None);
        }
        // From here on, dropping `lock` leaves the registry.
        let mut lock = CopyLock { dir, file: None };
        let path = lock.dir.join(LOCK_FILE);
        let path = path
            .to_str()
            .ok_or_else(|| Error::Config(format!("{} is not UTF-8", path.display())))?;
        let io = PlatformIO::new().map_err(|e| io_err("platform IO", e))?;
        // NoLock, then an explicit lock, as `durable.lock` (`db::DirGuard`).
        let file = io
            .open_file(path, OpenFlags::Create | OpenFlags::NoLock, false)
            .map_err(|e| io_err(&format!("open {path}"), e))?;
        if file.lock_file(true).is_err() {
            return Ok(None);
        }
        lock.file = Some(file);
        Ok(Some(lock))
    }
}

impl Drop for CopyLock {
    fn drop(&mut self) {
        if let Some(f) = self.file.take() {
            let _ = f.unlock_file();
        }
        crate::db::lock(&HELD).remove(&self.dir);
    }
}

/// The kept copy of a run that holds its lock. While `app.db` is not the pack the state file
/// names (a download in progress, or the flag on), the copy is *dirty*: a drop of a dirty copy
/// (an error, a close or a panic of the run) deletes it, so no partial file stays on the disk.
/// Only a crash of the process leaves one, and then the `applying` flag, or a missing state
/// file, makes the next run delete it and download.
pub(crate) struct KeptCopy {
    lock: CopyLock,
    dirty: AtomicBool,
}

impl Drop for KeptCopy {
    fn drop(&mut self) {
        if !self.dirty.load(SeqCst) || super::run::crash_simulated() {
            return;
        }
        if let Err(e) = self.clear() {
            tracing::warn!(target: "buckwal", error = %e, "pack run: cannot delete its unfinished kept copy");
        }
    }
}

impl KeptCopy {
    pub(crate) fn new(lock: CopyLock) -> KeptCopy {
        KeptCopy {
            lock,
            dirty: AtomicBool::new(false),
        }
    }

    /// A download into `app.db` starts: the copy is dirty until `keep` or `clear`.
    pub(crate) fn mark_dirty(&self) {
        self.dirty.store(true, SeqCst);
    }

    /// `pack-copy/`: the run applies the log to `app.db` in it.
    pub(crate) fn dir(&self) -> &Path {
        &self.lock.dir
    }

    /// Whether the copy equals the pack `p` names, by the three rules of the spec. `Kept`:
    /// the run can use it. A `Downloaded` reason: the run deletes it and downloads. `codes` is
    /// the pack's codes (from the bucket); `None` when they are gone.
    pub(crate) fn source_for(
        &self,
        p: &PackPointer,
        keys: &Keys,
        codes: Option<&PackCodes>,
    ) -> PackSource {
        let state = match read_state(self.dir()) {
            Ok(Some(s)) => s,
            Ok(None) => return PackSource::Downloaded(DownloadReason::None),
            Err(e) => {
                tracing::warn!(target: "buckwal", error = %e, "pack run: the kept copy's state file is not valid");
                return PackSource::Downloaded(DownloadReason::CheckFailed);
            }
        };
        if state.applying {
            return PackSource::Downloaded(DownloadReason::Applying);
        }
        if state.pack.as_ref() != Some(&CopyPack::of(p)) {
            return PackSource::Downloaded(DownloadReason::Moved);
        }
        let Some(codes) = codes else {
            return PackSource::Downloaded(DownloadReason::CheckFailed);
        };
        match check_file(&self.dir().join(DB_FILE), p, keys, codes) {
            Ok(()) => PackSource::Kept,
            Err(e) => {
                tracing::info!(target: "buckwal", error = %e, "pack run: the kept copy is not the pack; it downloads the pack");
                PackSource::Downloaded(DownloadReason::CheckFailed)
            }
        }
    }

    /// Delete the copy: the state file first, then every other entry but the lock.
    pub(crate) fn clear(&self) -> Result<()> {
        self.dirty.store(false, SeqCst);
        let dir = self.dir();
        match fs::remove_file(dir.join(STATE_FILE)) {
            Ok(()) => fsync_dir(dir)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(io_err("remove pack-copy/state.json", e)),
        }
        self.remove_entries(&[LOCK_FILE])
    }

    /// Remove the files next to `app.db` that an apply leaves (an empty `-wal`, say), before
    /// the run applies the log to a copy it uses.
    pub(crate) fn tidy(&self) -> Result<()> {
        self.remove_entries(&[LOCK_FILE, STATE_FILE, DB_FILE])
    }

    fn remove_entries(&self, keep: &[&str]) -> Result<()> {
        let dir = self.dir();
        let entries = fs::read_dir(dir).map_err(|e| io_err("list pack-copy", e))?;
        for e in entries {
            let e = e.map_err(|e| io_err("list pack-copy", e))?;
            if keep.iter().any(|k| e.file_name() == *k) {
                continue;
            }
            let path = e.path();
            let r = if path.is_dir() {
                fs::remove_dir_all(&path)
            } else {
                fs::remove_file(&path)
            };
            match r {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(io_err("remove a kept copy file", e)),
            }
        }
        fsync_dir(dir)
    }

    /// Set the `applying` flag, and fsync the state file, before the run changes `app.db`.
    /// `base` is the pack the file equals now (`None` for a first pack).
    pub(crate) fn set_applying(&self, base: Option<&PackPointer>) -> Result<()> {
        self.mark_dirty();
        self.write_state(&CopyState {
            pack: base.map(CopyPack::of),
            applying: true,
        })
    }

    /// The pointer names the run's new pack, which `app.db` equals: write it into the state
    /// file and clear the flag.
    pub(crate) fn keep(&self, pack: CopyPack) -> Result<()> {
        self.write_state(&CopyState {
            pack: Some(pack),
            applying: false,
        })?;
        self.dirty.store(false, SeqCst);
        Ok(())
    }

    fn write_state(&self, s: &CopyState) -> Result<()> {
        let dir = self.dir();
        write_synced(&dir.join(STATE_TMP), &s.encode())?;
        fs::rename(dir.join(STATE_TMP), dir.join(STATE_FILE))
            .map_err(|e| io_err("rename pack-copy/state.json", e))?;
        fsync_dir(dir)
    }
}

/// The local file `path` has the size of the pack `p` names, and each 1 MiB block matches its
/// code: the check a downloaded block gets.
fn check_file(path: &Path, p: &PackPointer, keys: &Keys, codes: &PackCodes) -> Result<()> {
    let mut f = File::open(path).map_err(|e| io_err("open the kept copy", e))?;
    let len = f
        .metadata()
        .map_err(|e| io_err("stat the kept copy", e))?
        .len();
    if len != p.size {
        return Err(Error::Corrupt(format!(
            "the kept copy holds {len} bytes, the pack {}",
            p.size
        )));
    }
    let mut buf = vec![0u8; BLOCK_SIZE as usize];
    let mut left = p.size;
    let mut n = 0u64;
    while left > 0 {
        let len = left.min(BLOCK_SIZE) as usize;
        f.read_exact(&mut buf[..len])
            .map_err(|e| io_err("read the kept copy", e))?;
        codes.verify_block(keys, &p.pack_key, p.pack_generation, p.size, n, &buf[..len])?;
        left -= len as u64;
        n += 1;
    }
    Ok(())
}

/// The pack's codes, decoded; `None` when they are gone (the pack is gone too).
pub(crate) fn fetch_codes(
    store: &dyn ObjectStore,
    keys: &Keys,
    retry: &RetryPolicy,
    p: &PackPointer,
) -> Result<Option<PackCodes>> {
    let Some(body) = with_retry(retry, &p.codes_key, |_| store.get(&p.codes_key))? else {
        return Ok(None);
    };
    PackCodes::decode(keys, &body.body, &p.pack_key, p.pack_generation, p.size).map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    /// Set only in the child process of the test below: the pack directory whose lock it holds.
    const CHILD_DIR: &str = "KEPT_COPY_LOCK_CHILD_DIR";
    const SELF: &str = "pack::kept::tests::lock_held_by_another_process_is_not_taken";

    fn wait_for(path: &Path) -> bool {
        let end = Instant::now() + Duration::from_secs(30);
        while Instant::now() < end {
            if path.exists() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        false
    }

    /// The lock is the operating system's: while another process holds it, a run of this
    /// process does not take it; once that process ends, it does.
    #[test]
    fn lock_held_by_another_process_is_not_taken() {
        if let Ok(dir) = std::env::var(CHILD_DIR) {
            let dir = PathBuf::from(dir);
            let _held = CopyLock::try_take(&dir)
                .unwrap()
                .expect("the child takes the lock");
            fs::write(dir.join("child-ready"), b"").unwrap();
            assert!(
                wait_for(&dir.join("child-stop")),
                "the parent never said stop"
            );
            return;
        }
        let t = tempfile::tempdir().unwrap();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", SELF, "--test-threads=1"])
            .env(CHILD_DIR, t.path())
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap();
        // The child goes on its own way when this test fails half-way.
        struct Stop<'a>(&'a Path);
        impl Drop for Stop<'_> {
            fn drop(&mut self) {
                let _ = fs::write(self.0.join("child-stop"), b"");
            }
        }
        let stop = Stop(t.path());
        assert!(
            wait_for(&t.path().join("child-ready")),
            "the child never took the lock"
        );
        assert!(
            CopyLock::try_take(t.path()).unwrap().is_none(),
            "a lock another process holds was taken"
        );
        drop(stop);
        assert!(child.wait().unwrap().success());
        assert!(CopyLock::try_take(t.path()).unwrap().is_some());
    }

    #[test]
    fn state_file_round_trips() {
        for s in [
            CopyState {
                pack: None,
                applying: true,
            },
            CopyState {
                pack: Some(CopyPack {
                    pack_key: "t/pack/0000000003/ab\"c".into(),
                    pack_generation: 3,
                    size: 8192,
                    database_id: Some("00ff".into()),
                }),
                applying: false,
            },
            CopyState {
                pack: Some(CopyPack {
                    pack_key: "pack/0000000001/x".into(),
                    pack_generation: 1,
                    size: 4096,
                    database_id: None,
                }),
                applying: false,
            },
        ] {
            assert_eq!(CopyState::decode(&s.encode()).unwrap(), s);
        }
        assert!(CopyState::decode(b"{\"format\":2,\"applying\":false}").is_err());
        assert!(CopyState::decode(b"not json").is_err());
    }
}
