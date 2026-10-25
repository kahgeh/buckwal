//! The first pack of a new MVCC database (spec "A new MVCC database is the one case that
//! publishes a pack at once", ADR 0004 as changed by format 5). The creation built the empty
//! database in its staging directory; this publishes that file before any head exists:
//!
//! 1. the pack, `pack/<G>/<run id>`, as a multipart upload in 16 MiB parts, its size (HEAD)
//!    and the SHA-256 of each part checked as a pack run checks them;
//! 2. its codes, `pack/<G>/<run id>.codes`;
//! 3. the first pointer, with `If-None-Match`.
//!
//! The caller then writes the head with `If-None-Match` (`Bucket::create_packed_mvcc_with`). A
//! crash between the steps leaves no head, so the next create starts again with a new run id,
//! and GC deletes the orphan pack. A pointer that such a crash left (step 3 done, no head) gets
//! a 412: the next create reads it and, while no head exists, replaces it with `If-Match`.
use super::codes::BLOCK_SIZE;
use super::pointer::{PackPointer, POINTER_FORMAT};
use super::run::{digest, FileSource, PART_CONCURRENCY, PART_SIZE};
use crate::error::{Error, Result};
use crate::files::io_err;
use crate::format::Keys;
use crate::store::{part_size_for, with_retry, CondPut, ObjectStore, RetryPolicy};
use std::path::Path;
use std::time::Duration;

/// The most pointer writes a creation sends when each gets a 412 from a pointer that changes
/// between its reads (racing creators).
const MAX_POINTER_WRITES: u32 = 10;

/// What [`publish`] left in the bucket.
pub(crate) struct FirstPack {
    /// The pointer object's bytes and the ETag of this creation's write of it.
    pub body: Vec<u8>,
    pub etag: String,
    /// The pointer object this creation replaced (one that no head named), if any.
    pub replaced: Option<Vec<u8>>,
}

fn exists() -> Error {
    Error::Config("a database already exists at this prefix".into())
}

/// Steps 1 to 3 (module doc) for the database file `db` of `page_size`-byte pages, as the
/// pack of `generation`. The keys must know the database id of an encrypted database
/// (`Keys::init_database_id`): the codes and the pointer are coded under it.
pub(crate) fn publish(
    store: &dyn ObjectStore,
    keys: &Keys,
    retry: &RetryPolicy,
    db: &Path,
    page_size: u32,
    generation: u64,
) -> Result<FirstPack> {
    let run_id = crate::db::new_writer_id()?;
    let size = std::fs::metadata(db)
        .map_err(|e| io_err("stat the first pack", e))?
        .len();
    let part_size = part_size_for(size, PART_SIZE);
    let pointer = PackPointer {
        format: POINTER_FORMAT,
        pack_key: keys.pack(generation, &run_id),
        codes_key: keys.pack_codes(generation, &run_id),
        pack_generation: generation,
        size,
        page_size,
        part_size: part_size as u64,
        block_size: BLOCK_SIZE,
        database_id: keys.known_database_id(),
    };
    let (pack_key, codes_key) = (pointer.pack_key.clone(), pointer.codes_key.clone());
    let (part_sha256, codes) = digest(keys, &pointer, db)?;
    let codes_body = codes.encode(keys, &pack_key, generation, size)?;
    // Step 1. An attempt that stored the pack but lost its reply is sent again: the key is
    // this creation's own.
    let mut source = FileSource::open(db)?;
    let done = match with_retry(retry, &pack_key, |_| {
        store.put_multipart(&pack_key, &mut source, part_size, PART_CONCURRENCY)
    }) {
        Ok(d) => d,
        Err(e) => {
            // The parts of a failed upload stay hidden until an abort; GC aborts them after
            // the orphan grace in any case.
            let _ = store.abort_incomplete(&pack_key, Duration::ZERO);
            return Err(e);
        }
    };
    let stat = with_retry(retry, &pack_key, |_| store.stat(&pack_key))?;
    if !matches!(&stat, Some(s) if s.size == size) {
        return Err(Error::UploadFailed(format!(
            "{pack_key}: the store holds {:?} bytes, not {size}",
            stat.map(|s| s.size)
        )));
    }
    if done.part_sha256 != part_sha256 {
        return Err(Error::UploadFailed(format!(
            "{pack_key}: a part's SHA-256 is not the file's"
        )));
    }
    // Step 2.
    with_retry(retry, &codes_key, |_| store.put(&codes_key, &codes_body))?;
    // Step 3.
    let body = keys.encode_pointer(&pointer);
    let (etag, replaced) = put_first_pointer(store, keys, retry, &body)?;
    Ok(FirstPack {
        body,
        etag,
        replaced,
    })
}

/// The first pointer with `If-None-Match`. A 412 reads the pointer: this creation's own bytes
/// (an earlier attempt landed) are its write; another pointer is replaced with `If-Match` on
/// its ETag while no head exists (a crashed creation left it, and nothing names its pack), and
/// refused as `Config` once a head exists. Returns the ETag of the write and the pointer it
/// replaced.
fn put_first_pointer(
    store: &dyn ObjectStore,
    keys: &Keys,
    retry: &RetryPolicy,
    body: &[u8],
) -> Result<(String, Option<Vec<u8>>)> {
    let key = keys.pack_pointer();
    let head_key = keys.head();
    // The pointer to replace: its ETag and its bytes.
    let mut over: Option<(String, Vec<u8>)> = None;
    for _ in 0..MAX_POINTER_WRITES {
        let put = match &over {
            None => with_retry(retry, &key, |_| store.put_if_absent(&key, body))?,
            Some((etag, _)) => with_retry(retry, &key, |_| store.put_if_match(&key, body, etag))?,
        };
        if let CondPut::Created { etag } = put {
            return Ok((etag, over.map(|(_, old)| old)));
        }
        let Some(now) = with_retry(retry, &key, |_| store.get(&key))? else {
            over = None;
            continue;
        };
        if now.body == body {
            return Ok((now.etag, over.map(|(_, old)| old)));
        }
        if with_retry(retry, &head_key, |_| store.get(&head_key))?.is_some() {
            return Err(exists());
        }
        over = Some((now.etag, now.body));
    }
    Err(Error::UploadFailed(format!(
        "{key}: the pointer changed at each of {MAX_POINTER_WRITES} writes"
    )))
}

/// After the head landed: the pointer must name this creation's pack. A racing creator that
/// found no head yet may have replaced it; it is put back with `If-Match` on what is there.
/// Best effort: a failure is logged, and the head stays.
pub(crate) fn keep_pointer(store: &dyn ObjectStore, keys: &Keys, retry: &RetryPolicy, ours: &[u8]) {
    let key = keys.pack_pointer();
    for _ in 0..MAX_POINTER_WRITES {
        let now = match with_retry(retry, &key, |_| store.get(&key)) {
            Ok(Some(o)) if o.body == ours => return,
            Ok(Some(o)) => o,
            Ok(None) => match with_retry(retry, &key, |_| store.put_if_absent(&key, ours)) {
                Ok(_) => continue,
                Err(e) => return warn_pointer(&e),
            },
            Err(e) => return warn_pointer(&e),
        };
        if let Err(e) = with_retry(retry, &key, |_| store.put_if_match(&key, ours, &now.etag)) {
            return warn_pointer(&e);
        }
    }
}

/// The loser of a creation race (its head got a 412): put back the pointer it replaced, with
/// `If-Match` on its own write, so the winner's pointer is the one that stays. Best effort.
pub(crate) fn give_back_pointer(
    store: &dyn ObjectStore,
    keys: &Keys,
    retry: &RetryPolicy,
    first: &FirstPack,
) {
    let Some(old) = &first.replaced else {
        return;
    };
    let key = keys.pack_pointer();
    if let Err(e) = with_retry(retry, &key, |_| store.put_if_match(&key, old, &first.etag)) {
        warn_pointer(&e);
    }
}

fn warn_pointer(e: &Error) {
    tracing::warn!(target: "buckwal", error = %e, "create: cannot make sure of the first pack pointer");
}
