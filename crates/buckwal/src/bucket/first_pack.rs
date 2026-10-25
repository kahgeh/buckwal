//! A new MVCC database of format 5 starts with its first pack (spec "A new MVCC database is
//! the one case that publishes a pack at once"; ADR 0004 as format 5 changes it): the pack of
//! the empty database, its codes and the first pointer (`pack::first::publish`), then the
//! head, last, with `If-None-Match`.

use super::*;
use crate::pack::first;

/// The generation a new MVCC database starts at, and the pack generation of its first pack.
/// Equal, so the chain of generations from the first pack ends at once at the head's (a
/// restore and a pack run read no log before the head's generation). It stays 2, as ADR 0004
/// set it for the seal of formats 2 and 4, so the generation numbers of MVCC buckets do not
/// change meaning.
pub(crate) const FIRST_GENERATION: u64 = 2;

impl Bucket {
    /// Start a new MVCC database of format 5 from `db`, the empty database file the creation
    /// built (`create::build_mvcc_image`), of `page_size`-byte pages (ciphertext when
    /// encrypted). In this order: the database id (the pack's codes and the pointer are coded
    /// under it), `pack/2/<run id>`, its codes, the first pointer with `If-None-Match`, then
    /// the head with `If-None-Match` (`create_head`): generation 2, no manifest, epoch 1,
    /// format 5, journal mode MVCC.
    ///
    /// A failure or crash before the head leaves no head: the next create starts again, and
    /// GC deletes the orphan pack. Two racing creators: the head decides, and the loser returns
    /// `Config` as `create` does, after it puts back a pointer it replaced. The winner then
    /// makes sure that the pointer names its own pack (`first::keep_pointer`).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create_packed_mvcc_with(
        store: Arc<dyn ObjectStore>,
        keys: Keys,
        retry: RetryPolicy,
        concurrency: usize,
        writer_id: String,
        db: &std::path::Path,
        page_size: u32,
        own: Timeouts,
    ) -> Result<Bucket> {
        check_writer_id(&writer_id)?;
        let database_id = keys.init_database_id()?;
        let pack = first::publish(&*store, &keys, &retry, db, page_size, FIRST_GENERATION)?;
        let head = own.stamp(Head {
            format: keys.new_format_packed(),
            generation: FIRST_GENERATION,
            manifest: None,
            writer_id,
            epoch: 1,
            journal_mode: JournalMode::Mvcc,
            lease_ms: None,
            request_timeout_ms: None,
            next_generation_floor: None,
            database_id,
        });
        let mut b = match Bucket::create_head(
            store.clone(),
            keys.clone(),
            retry.clone(),
            concurrency,
            head,
        ) {
            Ok(b) => b,
            Err(e) => {
                if matches!(e, Error::Config(_)) {
                    first::give_back_pointer(&*store, &keys, &retry, &pack);
                }
                return Err(e);
            }
        };
        first::keep_pointer(&*store, &keys, &retry, &pack.body);
        b.timeouts = own;
        Ok(b)
    }

    /// `create_packed_mvcc_with` and the default timeouts.
    #[cfg(test)]
    pub(crate) fn create_packed_mvcc(
        store: Arc<dyn ObjectStore>,
        keys: Keys,
        retry: RetryPolicy,
        concurrency: usize,
        writer_id: String,
        db: &std::path::Path,
        page_size: u32,
    ) -> Result<Bucket> {
        Bucket::create_packed_mvcc_with(
            store,
            keys,
            retry,
            concurrency,
            writer_id,
            db,
            page_size,
            Timeouts::default(),
        )
    }
}
