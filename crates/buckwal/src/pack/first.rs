//! The first pack of a new MVCC database, and the care every create takes with `pack-latest`
//! (spec rev 11, "A new MVCC database is the one case that publishes a pack at once", and its
//! three rules; ADR 0004 as format 5 changes it).
//!
//! A new MVCC database publishes the empty database file of its staging directory before any
//! head exists: the pack `pack/<G>/<run id>` and its codes (`run::upload_verified`, the steps a
//! pack run makes), then the first pointer with `If-None-Match`. The caller then writes the
//! head with `If-None-Match` (`Bucket::create_packed_mvcc_with`), then rewrites the pointer
//! (rule 2, [`rewrite_after_head`]). A crash between the steps leaves no head, so the next
//! create starts again with a new run id, and GC deletes the orphan pack.
//!
//! How the first pack differs from a pack run: a failed check deletes nothing (the pack and its
//! codes stay for GC; no pointer names them); there is no HEAD of the pack and codes just before
//! the pointer write and no time limit (no pointer names an older pack that GC could trim
//! against, and the head that makes the pointer count comes after it); a failed multipart
//! upload is sent again under the retry policy, as the key is this creation's own.
//!
//! Every create, WAL or MVCC, that finds no head removes a leftover pointer first (rule 1,
//! [`clear_leftover_pointer`]), and the WAL create deletes a pointer that appeared once its
//! head landed (rule 2, [`delete_after_head`]). Readers treat a pointer of another
//! `database_id` as no pointer (rule 3, `Keys::read_pointer`).
use super::codes::BLOCK_SIZE;
use super::pointer::{PackPointer, POINTER_FORMAT};
use super::run::{upload_verified, PART_SIZE};
use crate::error::{Error, Result};
use crate::files::io_err;
use crate::format::{Head, Keys};
use crate::store::{part_size_for, with_retry, CondPut, ObjectStore, RetryPolicy};
use std::path::Path;

/// The most rounds a create makes of a read and a conditional write of `pack-latest` before it
/// gives up, when each one finds that another creator changed it between the two (a race).
const MAX_ROUNDS: u32 = 10;

fn exists() -> Error {
    Error::Config("a database already exists at this prefix".into())
}

fn too_many(key: &str) -> Error {
    Error::UploadFailed(format!(
        "{key}: it changed at each of {MAX_ROUNDS} reads (another creator)"
    ))
}

/// Rule 1: before a create writes anything that a head would name, remove a `pack-latest`
/// that no head names. GET the pointer and keep its ETag; GET the head again: a head now is
/// `Config` ("a database already exists"), and nothing is deleted; else delete the pointer
/// with `If-Match` on that ETag. A 412 means another creator wrote a new pointer: read both
/// again. The pack it named stays for GC.
pub(crate) fn clear_leftover_pointer(
    store: &dyn ObjectStore,
    keys: &Keys,
    retry: &RetryPolicy,
) -> Result<()> {
    let key = keys.pack_pointer();
    let head_key = keys.head();
    for _ in 0..MAX_ROUNDS {
        let Some(pointer) = with_retry(retry, &key, |_| store.get(&key))? else {
            return Ok(());
        };
        if with_retry(retry, &head_key, |_| store.get(&head_key))?.is_some() {
            return Err(exists());
        }
        if with_retry(retry, &key, |_| store.delete_if_match(&key, &pointer.etag))? {
            tracing::info!(target: "buckwal", key = %key, "create: removed a pointer that no head names");
            return Ok(());
        }
    }
    Err(too_many(&key))
}

/// Rule 2 of a WAL create, after its head landed: a pointer that appeared (a racing MVCC
/// creator's, written after this create's rule 1) is deleted with `If-Match`. No pack run can
/// have written one yet: a pack needs a generation that this new writer ended.
pub(crate) fn delete_after_head(
    store: &dyn ObjectStore,
    keys: &Keys,
    retry: &RetryPolicy,
) -> Result<()> {
    let key = keys.pack_pointer();
    for _ in 0..MAX_ROUNDS {
        let Some(pointer) = with_retry(retry, &key, |_| store.get(&key))? else {
            return Ok(());
        };
        if with_retry(retry, &key, |_| store.delete_if_match(&key, &pointer.etag))? {
            return Ok(());
        }
    }
    Err(too_many(&key))
}

/// The first pointer this creation wrote (step 3), as rule 2 rewrites it.
pub(crate) struct FirstPack {
    pub pointer: PackPointer,
}

/// Steps 1 to 3 (module doc) for the database file `db` of `page_size`-byte pages, as the
/// pack of `generation`. The keys must know the database id (`Keys::init_packed_database_id`):
/// the pointer carries it, and the codes and the pointer are coded under it when encrypted.
/// The caller made rule 1 first.
pub(crate) fn publish(
    store: &dyn ObjectStore,
    keys: &Keys,
    retry: &RetryPolicy,
    db: &Path,
    page_size: u32,
    generation: u64,
) -> Result<FirstPack> {
    let pointer = upload_first(store, keys, retry, db, page_size, generation)?;
    // Step 3. A 412: another creator's pointer appeared since rule 1 (an earlier attempt of
    // this one that landed has these bytes); rule 1 again, then the write again.
    let key = keys.pack_pointer();
    let body = keys.encode_pointer(&pointer);
    for _ in 0..MAX_ROUNDS {
        if let CondPut::Created { .. } =
            with_retry(retry, &key, |_| store.put_if_absent(&key, &body))?
        {
            return Ok(FirstPack { pointer });
        }
        if with_retry(retry, &key, |_| store.get(&key))?.is_some_and(|o| o.body == body) {
            return Ok(FirstPack { pointer });
        }
        clear_leftover_pointer(store, keys, retry)?;
    }
    Err(too_many(&key))
}

/// Steps 1 and 2 (module doc): upload the database file `db` of `page_size`-byte pages as a new
/// pack `pack/<generation>/<run id>` and its codes, checked as a pack run checks them. Returns
/// the pointer that names it (not written). A failed check leaves the pack and its codes for
/// GC. The keys must know the database id. `buckwal upgrade` makes its first pack with it too.
pub(crate) fn upload_first(
    store: &dyn ObjectStore,
    keys: &Keys,
    retry: &RetryPolicy,
    db: &Path,
    page_size: u32,
    generation: u64,
) -> Result<PackPointer> {
    let run_id = crate::db::new_writer_id()?;
    let size = std::fs::metadata(db)
        .map_err(|e| io_err("stat the first pack", e))?
        .len();
    let part_size = part_size_for(size, PART_SIZE);
    let pointer = PackPointer {
        format: POINTER_FORMAT,
        pack_key: keys.pack(generation, &run_id),
        codes_key: keys.pack_codes(generation, &run_id),
        write_id: crate::db::new_writer_id()?,
        pack_generation: generation,
        size,
        page_size,
        part_size: part_size as u64,
        block_size: BLOCK_SIZE,
        database_id: keys.known_database_id(),
    };
    // A failed check leaves the pack and its codes for GC.
    if let Err(f) = upload_verified(
        store,
        keys,
        retry,
        &pointer,
        db,
        true,
        &std::sync::atomic::AtomicBool::new(false),
    )?
    .result
    {
        if f.step == "upload" {
            // The parts of a failed upload stay hidden until an abort; GC aborts them after
            // the orphan grace in any case.
            let _ = store.abort_incomplete(&pointer.pack_key, std::time::Duration::ZERO);
        }
        return Err(f.error);
    }
    Ok(pointer)
}

/// Rule 2 of an MVCC create, after its head landed: write the pointer again with a new
/// `write_id`, with `If-Match` on the ETag just read, or `If-None-Match` when there is none;
/// then GET it once more and check that it names this creation's pack. A slow creator that read
/// the first pointer before the head landed holds only its ETag: its late delete fails with a
/// 412 once this rewrite (other bytes, another ETag) is in, and a delete before the rewrite is
/// undone by it.
pub(crate) fn rewrite_after_head(
    store: &dyn ObjectStore,
    keys: &Keys,
    retry: &RetryPolicy,
    first: &FirstPack,
) -> Result<()> {
    let key = keys.pack_pointer();
    for _ in 0..MAX_ROUNDS {
        let body = keys.encode_pointer(&PackPointer {
            write_id: crate::db::new_writer_id()?,
            ..first.pointer.clone()
        });
        let put = match with_retry(retry, &key, |_| store.get(&key))? {
            Some(o) => with_retry(retry, &key, |_| store.put_if_match(&key, &body, &o.etag))?,
            None => with_retry(retry, &key, |_| store.put_if_absent(&key, &body))?,
        };
        if let CondPut::PreconditionFailed = put {
            continue;
        }
        let now = with_retry(retry, &key, |_| store.get(&key))?;
        let ours = now
            .map(|o| keys.read_pointer(&o.body))
            .transpose()?
            .flatten()
            .is_some_and(|p| p.pack_key == first.pointer.pack_key);
        if ours {
            return Ok(());
        }
    }
    Err(too_many(&key))
}

/// The error for an MVCC bucket of format 5 whose pack pointer (`pointer_key`) is missing or
/// another database's. Only a head that is still the create state gets the way out: the first
/// pack generation, epoch 1, and no object under the log of that generation (no commit was ever
/// acked). Any other bucket may hold committed data, so its text gives no destructive advice.
pub(crate) fn pointer_gap_error(
    store: &dyn ObjectStore,
    keys: &Keys,
    retry: &RetryPolicy,
    head: &Head,
    pointer_key: &str,
) -> Error {
    let prefix = keys.log_prefix(head.generation);
    let create_state = head.generation == crate::bucket::first_pack::FIRST_GENERATION
        && head.epoch == 1
        && match with_retry(retry, &prefix, |_| store.list(&prefix)) {
            Ok(objects) => objects.is_empty(),
            Err(e) => return e,
        };
    let advice = if create_state {
        "The create of this database did not finish. Delete the object 'head' under the prefix \
         and create the database again."
    } else {
        "The bucket may hold committed data. Keep every object."
    };
    Error::Corrupt(format!(
        "{pointer_key} is missing or another database's. An MVCC bucket of format 5 starts with \
         its first pack. {advice}"
    ))
}
