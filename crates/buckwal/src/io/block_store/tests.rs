//! The block store over synthetic packs: a switch to a newer pack across a block boundary, a
//! block past the end of the pack, and the newer pack's page size.
use super::*;
use crate::pack::pointer::POINTER_FORMAT;
use crate::store::MemStore;
use std::time::Duration;

const BS: u64 = BLOCK_SIZE;

fn keys() -> Keys {
    Keys::new("t")
}

fn retry() -> RetryPolicy {
    RetryPolicy {
        deadline: Duration::from_secs(1),
        first_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(10),
    }
}

/// `len` bytes of a database file of `page_size`-byte pages: the SQLite header, then a pattern
/// made from `seed`.
fn body(len: u64, seed: u8, page_size: u32) -> Vec<u8> {
    let mut b: Vec<u8> = (0..len).map(|i| (i as u8).wrapping_mul(7) ^ seed).collect();
    b[..16].copy_from_slice(b"SQLite format 3\0");
    let field = if page_size == 65536 {
        1
    } else {
        page_size as u16
    };
    b[16..18].copy_from_slice(&field.to_be_bytes());
    b
}

/// A pack of `b` at `gen` in `store`, its codes, and the pointer to it (written as the pointer).
/// Returns the pointer, its bytes and the codes' bytes.
fn put_pack(
    store: &MemStore,
    gen: u64,
    run: &str,
    b: &[u8],
    page_size: u32,
) -> (PackPointer, Vec<u8>, Vec<u8>) {
    let k = keys();
    let pack_key = k.pack(gen, run);
    let size = b.len() as u64;
    let codes = PackCodes::build(&k, &pack_key, gen, size, b.chunks(BS as usize)).unwrap();
    let codes_body = codes.encode(&k, &pack_key, gen, size).unwrap();
    let p = PackPointer {
        format: POINTER_FORMAT,
        write_id: "w1".into(),
        codes_key: k.pack_codes(gen, run),
        pack_key: pack_key.clone(),
        pack_generation: gen,
        size,
        page_size,
        part_size: 16 << 20,
        block_size: BS,
        database_id: None,
    };
    store.put(&pack_key, b).unwrap();
    store.put(&p.codes_key, &codes_body).unwrap();
    let pointer_body = k.encode_pointer(&p);
    store.put(&k.pack_pointer(), &pointer_body).unwrap();
    (p, pointer_body, codes_body)
}

/// A restore's block store over pack `a` in `dir`, with block 0 local, dropped; then the store an
/// open builds over the same directory, at local generation 10.
fn restored(store: &Arc<MemStore>, dir: &Path, a: (PackPointer, Vec<u8>, Vec<u8>)) -> BlockStore {
    let (p, pb, cb) = a;
    let codes = PackCodes::decode(&keys(), &cb, &p.pack_key, p.pack_generation, p.size).unwrap();
    let bs = BlockStore::create(
        dir,
        store.clone(),
        &keys(),
        &retry(),
        p,
        &pb,
        codes,
        &cb,
        Arc::default(),
    )
    .unwrap();
    bs.ensure_block(0).unwrap();
    drop(bs);
    reopen(store, dir).expect("the store of the open")
}

fn reopen(store: &Arc<MemStore>, dir: &Path) -> Result<BlockStore> {
    BlockStore::open(dir, store.clone(), &keys(), &retry(), false, 10).map(|b| b.unwrap())
}

fn file_bytes(dir: &Path, from: u64, len: u64) -> Vec<u8> {
    let b = fs::read(dir.join(DB_FILE)).unwrap();
    b[from as usize..(from + len) as usize].to_vec()
}

/// The pack grows by a block, then the old one is gone: a read takes the newer pack, and after a
/// reopen the store still opens, reads and completes.
#[test]
fn switch_to_a_larger_pack_then_reopen_reads_and_completes() {
    let store = Arc::new(MemStore::new());
    let t = tempfile::tempdir().unwrap();
    let a = body(2 * BS + BS / 2, 1, 4096);
    let old = put_pack(&store, 2, "a", &a, 4096);
    let bs = restored(&store, t.path(), old.clone());
    let b = body(3 * BS + BS / 2, 2, 4096);
    put_pack(&store, 3, "b", &b, 4096);
    store.delete(&old.0.pack_key).unwrap();
    bs.ensure_block(1).unwrap();
    assert_eq!(
        file_bytes(t.path(), BS, BS),
        b[BS as usize..2 * BS as usize]
    );
    drop(bs);
    let bs = reopen(&store, t.path()).unwrap();
    assert!(bs.fetch_all(&AtomicBool::new(false)).unwrap());
    assert!(bs.is_complete());
    let end = 2 * BS + BS / 2;
    assert_eq!(
        file_bytes(t.path(), 2 * BS, BS / 2),
        b[2 * BS as usize..end as usize]
    );
}

/// The file was truncated below a block boundary (as the engine does), the pack shrinks by a
/// block, then the old one is gone: a read takes the newer pack, and after a reopen the store
/// still opens and completes without fetching past the newer pack's end.
#[test]
fn switch_to_a_smaller_pack_then_reopen_reads_and_completes() {
    let store = Arc::new(MemStore::new());
    let t = tempfile::tempdir().unwrap();
    let a = body(2 * BS + BS / 2, 1, 4096);
    let old = put_pack(&store, 2, "a", &a, 4096);
    let bs = restored(&store, t.path(), old.clone());
    bs.truncate(BS + BS / 2).unwrap();
    let c = body(BS + BS / 2, 3, 4096);
    put_pack(&store, 3, "c", &c, 4096);
    store.delete(&old.0.pack_key).unwrap();
    bs.ensure_block(1).unwrap();
    assert_eq!(file_bytes(t.path(), BS, BS / 2), c[BS as usize..]);
    drop(bs);
    let bs = reopen(&store, t.path()).unwrap();
    assert!(bs.fetch_all(&AtomicBool::new(false)).unwrap());
}

/// A block at or after the end of the pack it comes from is an error, not an empty block.
#[test]
fn block_past_the_end_of_the_pack_is_an_error() {
    let store = Arc::new(MemStore::new());
    let t = tempfile::tempdir().unwrap();
    let a = body(2 * BS + BS / 2, 1, 4096);
    let old = put_pack(&store, 2, "a", &a, 4096);
    let bs = restored(&store, t.path(), old.clone());
    // A newer pack of two blocks, with no truncate of the local file below block 2.
    let c = body(2 * BS, 3, 4096);
    put_pack(&store, 3, "c", &c, 4096);
    store.delete(&old.0.pack_key).unwrap();
    let e = bs.ensure_block(2).unwrap_err();
    assert!(
        matches!(&e, Error::Corrupt(m) if m.contains("past the end")),
        "{e:?}"
    );
    assert!(!bs.is_complete());
}

/// A newer pack whose page size is not the local header's is not taken.
#[test]
fn newer_pack_with_another_page_size_is_refused() {
    let store = Arc::new(MemStore::new());
    let t = tempfile::tempdir().unwrap();
    let a = body(2 * BS + BS / 2, 1, 4096);
    let old = put_pack(&store, 2, "a", &a, 4096);
    let bs = restored(&store, t.path(), old.clone());
    let d = body(2 * BS + BS / 2, 4, 8192);
    put_pack(&store, 3, "d", &d, 8192);
    store.delete(&old.0.pack_key).unwrap();
    let e = bs.ensure_block(1).unwrap_err();
    assert!(
        matches!(&e, Error::Corrupt(m) if m.contains("page_size")),
        "{e:?}"
    );
    assert_eq!(file_bytes(t.path(), BS, 16), vec![0u8; 16]);
}
