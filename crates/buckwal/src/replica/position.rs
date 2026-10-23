//! The replica's position file `app.db-replica`: which generation it follows, where its local
//! frame numbering stands, and whether a checkpoint is half done. A small line-based text file,
//! replaced atomically (temp file, fsync, rename, fsync the directory).
use std::{fs, path::Path};

use crate::{
    error::{Error, Result},
    files::{io_err, REPLICA_FILE},
    reconcile::replace_file,
};

const MAGIC: &str = "buckwal-replica 1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stage {
    Applying,
    CkptPending,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Position {
    pub generation: u64,
    pub stage: Stage,
    /// Local frame number = generation frame number + offset.
    pub offset: i64,
    /// The offset once the pending checkpoint finishes; used only in `CkptPending`.
    pub new_offset: i64,
    pub replica_id: String,
}

fn corrupt(what: &str) -> Error {
    Error::Corrupt(format!("{REPLICA_FILE}: {what}"))
}

impl Position {
    pub(crate) fn write(&self, dir: &Path) -> Result<()> {
        let stage = match self.stage {
            Stage::Applying => "applying",
            Stage::CkptPending => "ckpt_pending",
        };
        let text = format!(
            "{MAGIC}\ngeneration={}\nstage={stage}\noffset={}\nnew_offset={}\nreplica_id={}\nend\n",
            self.generation, self.offset, self.new_offset, self.replica_id
        );
        replace_file(dir, REPLICA_FILE, text.as_bytes())
    }

    /// `Ok(None)` when the file is absent; a malformed or truncated file is `Corrupt`.
    pub(crate) fn read(dir: &Path) -> Result<Option<Position>> {
        let bytes = match fs::read(dir.join(REPLICA_FILE)) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(io_err("read replica position", e)),
        };
        let text = String::from_utf8(bytes).map_err(|_| corrupt("not UTF-8"))?;
        let mut lines = text.lines();
        if lines.next() != Some(MAGIC) {
            return Err(corrupt("bad header"));
        }
        let mut field = |key: &str| -> Result<String> {
            let line = lines.next().ok_or_else(|| corrupt("truncated"))?;
            line.strip_prefix(key)
                .and_then(|r| r.strip_prefix('='))
                .map(str::to_string)
                .ok_or_else(|| corrupt(&format!("expected {key}")))
        };
        let generation = field("generation")?
            .parse()
            .map_err(|_| corrupt("bad generation"))?;
        let stage = match field("stage")?.as_str() {
            "applying" => Stage::Applying,
            "ckpt_pending" => Stage::CkptPending,
            _ => return Err(corrupt("bad stage")),
        };
        let offset = field("offset")?
            .parse()
            .map_err(|_| corrupt("bad offset"))?;
        let new_offset = field("new_offset")?
            .parse()
            .map_err(|_| corrupt("bad new_offset"))?;
        let replica_id = field("replica_id")?;
        if lines.next() != Some("end") || lines.next().is_some() {
            return Err(corrupt("truncated"));
        }
        Ok(Some(Position {
            generation,
            stage,
            offset,
            new_offset,
            replica_id,
        }))
    }

    /// Where to resume: `(generation, offset, run the checkpoint again)`. With `CkptPending`, an
    /// empty local WAL means the checkpoint finished, so the new offset applies; otherwise it
    /// didn't, so the old offset stands; a seal's checkpoint (new offset 0) runs again, a deferred
    /// one (new offset below 0) is treated as `applying` because its `-k` can be stale.
    pub(crate) fn resume(&self, local_wal_empty: bool) -> (u64, i64, bool) {
        match self.stage {
            Stage::Applying => (self.generation, self.offset, false),
            Stage::CkptPending if local_wal_empty => (self.generation, self.new_offset, false),
            // A seal's pending checkpoint (new offset 0, written before any frame of the new
            // generation was applied) is re-run at once.
            Stage::CkptPending if self.new_offset == 0 => (self.generation, self.offset, true),
            // A deferred one (new offset -k): the file may be stale, since more frames can have
            // been applied after it was written. Treat it as `applying` at the old offset; the
            // follower works k out again from the WAL before the next checkpoint.
            Stage::CkptPending => (self.generation, self.offset, false),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pos(stage: Stage) -> Position {
        Position {
            generation: 7,
            stage,
            offset: 12,
            new_offset: -5,
            replica_id: "r-abc123".into(),
        }
    }

    #[test]
    fn roundtrip() {
        let d = tempfile::tempdir().unwrap();
        for s in [Stage::Applying, Stage::CkptPending] {
            let p = pos(s);
            p.write(d.path()).unwrap();
            assert_eq!(Position::read(d.path()).unwrap(), Some(p));
        }
    }

    #[test]
    fn absent_file_is_none() {
        let d = tempfile::tempdir().unwrap();
        assert_eq!(Position::read(d.path()).unwrap(), None);
    }

    #[test]
    fn truncated_file_is_corrupt() {
        let d = tempfile::tempdir().unwrap();
        pos(Stage::Applying).write(d.path()).unwrap();
        let path = d.path().join(REPLICA_FILE);
        let full = fs::read(&path).unwrap();
        for cut in [0, 5, full.len() / 2, full.len() - 2, full.len() - 4] {
            fs::write(&path, &full[..cut]).unwrap();
            assert!(
                matches!(Position::read(d.path()), Err(Error::Corrupt(_))),
                "cut {cut}"
            );
        }
    }

    #[test]
    fn write_leaves_no_temp_file() {
        let d = tempfile::tempdir().unwrap();
        pos(Stage::Applying).write(d.path()).unwrap();
        pos(Stage::CkptPending).write(d.path()).unwrap();
        let names: Vec<_> = fs::read_dir(d.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(names, vec![REPLICA_FILE.to_string()]);
    }

    #[test]
    fn resume_ckpt_pending_empty_wal_uses_new_offset() {
        assert_eq!(pos(Stage::CkptPending).resume(true), (7, -5, false));
    }

    #[test]
    fn resume_seal_ckpt_pending_nonempty_wal_uses_old_offset_and_reruns_checkpoint() {
        let mut p = pos(Stage::CkptPending);
        p.new_offset = 0;
        assert_eq!(p.resume(false), (7, 12, true));
    }

    #[test]
    fn resume_deferred_ckpt_pending_nonempty_wal_is_applying_at_the_old_offset() {
        // new_offset -5 may be stale (more frames were applied after it was written): the
        // follower recomputes k from the WAL instead of running the checkpoint on it.
        assert_eq!(pos(Stage::CkptPending).resume(false), (7, 12, false));
    }

    #[test]
    fn resume_applying_returns_offset() {
        assert_eq!(pos(Stage::Applying).resume(true), (7, 12, false));
        assert_eq!(pos(Stage::Applying).resume(false), (7, 12, false));
    }
}
