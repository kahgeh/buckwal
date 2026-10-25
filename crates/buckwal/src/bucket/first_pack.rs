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
    /// encrypted). In this order (spec rev 11): rule 1 (a leftover pointer no head names is
    /// deleted with `If-Match`), the database id (the pointer carries it; the codes and the
    /// pointer are coded under it when encrypted), `pack/2/<run id>`, its codes, the first
    /// pointer with `If-None-Match`, the head with `If-None-Match` (`create_head`: generation
    /// 2, no manifest, epoch 1, format 5, journal mode MVCC), then rule 2 (the pointer written
    /// again with a new `write_id`, and checked).
    ///
    /// A failure or crash before the head leaves no head: the next create removes the pointer
    /// (rule 1) and starts again, and GC deletes the orphan pack. Two racing creators: the head
    /// decides, and the loser returns `Config` as `create` does; a pointer it leaves later has
    /// its own `database_id`, which every reader of the winner treats as no pointer (rule 3).
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
        first::clear_leftover_pointer(&*store, &keys, &retry)?;
        let database_id = keys.init_packed_database_id()?;
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
        let mut b = Bucket::create_head(
            store.clone(),
            keys.clone(),
            retry.clone(),
            concurrency,
            head,
        )?;
        first::rewrite_after_head(&*store, &keys, &retry, &pack)?;
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
