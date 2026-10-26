//! The bucket check of `buckwal verify` ([`check_bucket`]): the head, the pack pointer, each
//! block of the pack against its codes, and the chain of generations and their log from the
//! pack generation to the head's generation. It reads the bucket and writes nothing.
use super::codes::BLOCK_SIZE;
use super::kept::fetch_codes;
use super::run::{scan_generations, LogReader, PART_CONCURRENCY, PART_SIZE};
use super::PackPointer;
use crate::config::Config;
use crate::encryption;
use crate::error::{Error, Result};
use crate::format::{Keys, FORMAT_V5};
use crate::io::block_store::fetch_verified;
use crate::restore::fetch_generation;
use crate::store::{with_retry, ObjectStore, RetryPolicy};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

/// How many times the check reads the pointer again when the pack or its codes are gone (GC
/// deleted them after a newer pack replaced them), as a restore does.
const MAX_RESTARTS: u32 = 3;

/// What [`check_bucket`] read and found correct. Not part of the supported API: `buckwal
/// verify` is its only user.
#[doc(hidden)]
#[non_exhaustive]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BucketCheck {
    /// The head's format and generation.
    pub format: u32,
    pub generation: u64,
    /// The pack the pointer names, relative to the prefix, when there is one.
    pub pack_key: Option<String>,
    pub pack_generation: u64,
    pub pack_size: u64,
    /// The blocks of the pack that matched their codes.
    pub blocks: u64,
    /// The generations from the pack generation to the head's, the head's included, and
    /// their log objects. Skipped generations (no log) are not counted.
    pub log_generations: u64,
    pub log_objects: u64,
}

/// Check the bucket `config` names. A bucket of format 1 to 4 is not checked here (the
/// report has only its format and generation): its restore checks it. Of a format 5 bucket:
///
/// - the head and the pack pointer decode, with their integrity codes in an encrypted bucket;
/// - the pack has the pointer's size, and each 1 MiB block matches its code (a changed byte is
///   `Corrupt`, the integrity error and record in an encrypted bucket);
/// - from the pack generation, each ended generation ends with its terminal record and names
///   the next one up to the head's generation, as a restore follows it (else `Corrupt` with
///   `log_incomplete` and the key);
/// - every record of those generations and of the head's generation decodes, with no gap.
///
/// Not part of the supported API: `buckwal verify` is its only user.
#[doc(hidden)]
pub fn check_bucket(config: &Config) -> Result<BucketCheck> {
    let store = config.s3_store()?;
    check_bucket_with_store(config, store)
}

#[doc(hidden)]
pub fn check_bucket_with_store(
    config: &Config,
    store: Arc<dyn ObjectStore>,
) -> Result<BucketCheck> {
    check_bucket_in(config, store.as_ref(), &mut || {})
}

/// What one attempt of [`check_bucket_in`] found.
enum Attempt {
    Checked(BucketCheck),
    /// The pack or its codes are gone, or a log object that the attempt needs is gone below
    /// a newer pack: GC deleted it after a newer pack replaced the one the attempt read.
    Gone(String),
}

/// [`check_bucket_with_store`] with `after_pack` called in each attempt after the pack
/// check, before the log is read (tests move the pointer and delete the log there).
///
/// An attempt reads the head and the pointer, checks the pack, then the log. It starts again
/// from the head, at most `MAX_RESTARTS` times, when the pack or its codes are gone, or by the
/// restore's `log_missing` rule (`restore::restart_reason`): a log object is gone, or the log
/// is not complete (or not found) while the pointer names another pack than the one read.
/// One more such case is `Timeout`.
pub(crate) fn check_bucket_in(
    config: &Config,
    store: &dyn ObjectStore,
    after_pack: &mut dyn FnMut(),
) -> Result<BucketCheck> {
    config.validate()?;
    let enc = encryption::load(config)?;
    let keys = config.keys_for(enc.as_deref());
    let retry = config.engine(crate::io::IoMode::ReadOnly).retry;
    let mut restarts = 0;
    loop {
        let read = std::cell::RefCell::new(None);
        let gone = match attempt(store, &keys, &retry, &read, after_pack) {
            Ok(Attempt::Checked(out)) => return Ok(out),
            Ok(Attempt::Gone(key)) => key,
            Err(e) => {
                let read = read.borrow().clone();
                let log_gone = matches!(&e, Error::NotFound(k) if k.starts_with(&keys.log_root()));
                let cut = matches!(&e, Error::NotFound(_) | Error::Corrupt(_))
                    && pointer_moved(store, &keys, &retry, read.as_deref());
                if !(log_gone || cut) {
                    return Err(e);
                }
                e.to_string()
            }
        };
        if restarts >= MAX_RESTARTS {
            return Err(Error::Timeout(format!(
                "verify: the pack changed {MAX_RESTARTS} times while it was read ({gone}); \
                 raise BUCKWAL_PACK_KEEP_SECS"
            )));
        }
        restarts += 1;
        tracing::info!(target: "buckwal", attempt = restarts, reason = %gone, "verify: the pack changed; checking again");
    }
}

/// One attempt of [`check_bucket_in`]; `read` gets the key of the pack it read.
fn attempt(
    store: &dyn ObjectStore,
    keys: &Keys,
    retry: &RetryPolicy,
    read: &std::cell::RefCell<Option<String>>,
    after_pack: &mut dyn FnMut(),
) -> Result<Attempt> {
    let head_key = keys.head();
    let head_obj = with_retry(retry, &head_key, |_| store.get(&head_key))?
        .ok_or_else(|| Error::NotFound(head_key.clone()))?;
    // The keys learn the head's database id here, before the pointer is decoded.
    let head = keys.decode_head_open(&head_obj.body)?;
    let mut out = BucketCheck {
        format: head.format,
        generation: head.generation,
        ..BucketCheck::default()
    };
    if head.format != FORMAT_V5 {
        return Ok(Attempt::Checked(out));
    }
    let pointer = read_pointer(store, keys, retry)?;
    if let Some(p) = &pointer {
        *read.borrow_mut() = Some(p.pack_key.clone());
        let Some(blocks) = check_pack(store, keys, retry, p)? else {
            return Ok(Attempt::Gone(p.pack_key.clone()));
        };
        out.blocks = blocks;
        out.pack_key = Some(keys.relative(&p.pack_key).to_string());
        out.pack_generation = p.pack_generation;
        out.pack_size = p.size;
    }
    after_pack();
    let from = pointer.as_ref().map_or(1, |p| p.pack_generation);
    let reader = LogReader {
        store,
        keys,
        retry,
        concurrency: PART_CONCURRENCY,
    };
    let mut ended = |_, records: &[crate::format::LogRecord]| {
        out.log_generations += 1;
        out.log_objects += records.len() as u64;
        Ok(())
    };
    let scanned = scan_generations(&reader, from, &head, &mut ended)?;
    if let Some(key) = scanned.incomplete {
        return Err(Error::Corrupt(format!("log_incomplete: {key}")));
    }
    let current = fetch_generation(
        store,
        keys,
        head.generation,
        PART_CONCURRENCY,
        retry,
        &AtomicU64::new(0),
    )?;
    if !current.is_empty() {
        out.log_generations += 1;
        out.log_objects += current.len() as u64;
    }
    Ok(Attempt::Checked(out))
}

/// Whether the pointer now names another pack than `read` (the restore's `pointer_moved`).
fn pointer_moved(
    store: &dyn ObjectStore,
    keys: &Keys,
    retry: &RetryPolicy,
    read: Option<&str>,
) -> bool {
    read_pointer(store, keys, retry)
        .is_ok_and(|p| p.is_some_and(|p| Some(p.pack_key.as_str()) != read))
}

/// This database's pack pointer, decoded; `None` without one (a pointer of another database
/// is no pointer).
fn read_pointer(
    store: &dyn ObjectStore,
    keys: &Keys,
    retry: &RetryPolicy,
) -> Result<Option<PackPointer>> {
    let key = keys.pack_pointer();
    let Some(obj) = with_retry(retry, &key, |_| store.get(&key))? else {
        return Ok(None);
    };
    keys.read_pointer(&obj.body)
}

/// Check the size of the pack `p` names and each of its blocks against its codes, in ranges
/// of one part. The number of blocks; `None` when the pack or its codes are gone.
fn check_pack(
    store: &dyn ObjectStore,
    keys: &Keys,
    retry: &RetryPolicy,
    p: &PackPointer,
) -> Result<Option<u64>> {
    let Some(codes) = fetch_codes(store, keys, retry, p)? else {
        return Ok(None);
    };
    let Some(stat) = with_retry(retry, &p.pack_key, |_| store.stat(&p.pack_key))? else {
        return Ok(None);
    };
    if stat.size != p.size {
        return Err(Error::Corrupt(format!(
            "{} holds {} bytes, its pointer names {}",
            p.pack_key, stat.size, p.size
        )));
    }
    let mut offset = 0;
    while offset < p.size {
        let range = PART_SIZE as u64;
        let Some(bytes) = fetch_verified(store, keys, retry, p, &codes, offset, range)? else {
            return Ok(None);
        };
        offset += bytes.len() as u64;
    }
    Ok(Some(p.size.div_ceil(BLOCK_SIZE)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::open_with_store;
    use crate::store::MemStore;
    use crate::{CheckpointMode, Value};
    use std::path::Path;
    use std::time::Duration;

    fn config(dir: &Path) -> Config {
        let mut c = Config::new(dir, "http://127.0.0.1:1", "bucket", "key", "secret");
        c.prefix = "t".to_string();
        c.retry_deadline = Duration::from_secs(2);
        c
    }

    /// A pack run publishes a newer pack, and GC deletes the log below it, after the check
    /// read the older pack and before it reads the log. The check reads the pointer again
    /// (the restore's `log_missing` rule) and checks the newer pack: no false `log_incomplete`.
    #[test]
    fn check_restarts_when_the_log_below_a_newer_pack_is_gone() {
        let mem = Arc::new(MemStore::new());
        let store: Arc<dyn ObjectStore> = mem.clone();
        let t = tempfile::tempdir().unwrap();
        let db = open_with_store(config(&t.path().join("w")), store.clone()).unwrap();
        let c = db.connect().unwrap();
        c.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)", &[])
            .unwrap();
        c.execute("INSERT INTO t VALUES (1)", &[]).unwrap();
        c.checkpoint(CheckpointMode::Truncate).unwrap();
        let first = crate::pack_with_store(&config(&t.path().join("p")), store.clone()).unwrap();
        assert_eq!(first.outcome, crate::PackOutcome::Packed, "{first:?}");
        c.execute("INSERT INTO t VALUES (?)", &[Value::from_i64(2)])
            .unwrap();
        c.checkpoint(CheckpointMode::Truncate).unwrap();
        c.execute("INSERT INTO t VALUES (3)", &[]).unwrap();
        c.close().unwrap();
        db.close().unwrap();

        let keys = Keys::new("t");
        let below = first.pack_generation;
        let mut calls = 0;
        let mut newer = None;
        let mut after_pack = || {
            calls += 1;
            if calls > 1 {
                return;
            }
            let r = crate::pack_with_store(&config(&t.path().join("p")), store.clone()).unwrap();
            assert_eq!(r.outcome, crate::PackOutcome::Packed, "{r:?}");
            assert!(r.pack_generation > below, "{r:?}");
            // GC after the pack keep time: the log below the newer pack goes.
            for k in mem.list(&keys.log_root()).unwrap() {
                if keys
                    .parse_log(&k)
                    .is_some_and(|(g, _)| g < r.pack_generation)
                {
                    mem.delete(&k).unwrap();
                }
            }
            newer = Some(r);
        };
        let out =
            check_bucket_in(&config(&t.path().join("v")), mem.as_ref(), &mut after_pack).unwrap();
        assert_eq!(calls, 2, "one restart");
        let newer = newer.unwrap();
        assert_eq!(out.pack_generation, newer.pack_generation);
        assert_eq!(
            out.pack_key.as_deref(),
            newer.pack_key.as_deref().and_then(|k| k.strip_prefix("t/"))
        );
        assert!(out.blocks >= 1 && out.log_objects >= 1, "{out:?}");
    }
}
