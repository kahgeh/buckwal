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

/// What [`check_bucket`] read and found correct.
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
pub fn check_bucket(config: &Config) -> Result<BucketCheck> {
    let store = config.s3_store()?;
    check_bucket_with_store(config, store)
}

#[doc(hidden)]
pub fn check_bucket_with_store(
    config: &Config,
    store: Arc<dyn ObjectStore>,
) -> Result<BucketCheck> {
    config.validate()?;
    let enc = encryption::load(config)?;
    let keys = config.keys_for(enc.as_deref());
    let retry = config.engine(crate::io::IoMode::ReadOnly).retry;
    let store = store.as_ref();
    let head_key = keys.head();
    let head_obj = with_retry(&retry, &head_key, |_| store.get(&head_key))?
        .ok_or_else(|| Error::NotFound(head_key.clone()))?;
    // The keys learn the head's database id here, before the pointer is decoded.
    let head = keys.decode_head_open(&head_obj.body)?;
    let mut out = BucketCheck {
        format: head.format,
        generation: head.generation,
        ..BucketCheck::default()
    };
    if head.format != FORMAT_V5 {
        return Ok(out);
    }
    let mut restarts = 0;
    let pointer = loop {
        let Some(p) = read_pointer(store, &keys, &retry)? else {
            break None;
        };
        match check_pack(store, &keys, &retry, &p)? {
            Some(blocks) => {
                out.blocks = blocks;
                break Some(p);
            }
            None if restarts < MAX_RESTARTS => restarts += 1,
            None => {
                return Err(Error::Timeout(format!(
                    "verify: the pack changed {MAX_RESTARTS} times while it was read ({})",
                    p.pack_key
                )))
            }
        }
    };
    let from = pointer.as_ref().map_or(1, |p| p.pack_generation);
    if let Some(p) = &pointer {
        out.pack_key = Some(keys.relative(&p.pack_key).to_string());
        out.pack_generation = p.pack_generation;
        out.pack_size = p.size;
    }
    let reader = LogReader {
        store,
        keys: &keys,
        retry: &retry,
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
        &keys,
        head.generation,
        PART_CONCURRENCY,
        &retry,
        &AtomicU64::new(0),
    )?;
    if !current.is_empty() {
        out.log_generations += 1;
        out.log_objects += current.len() as u64;
    }
    Ok(out)
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
