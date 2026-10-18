//! Q5: the log of an encrypted database holds ciphertext frames. Does wal_insert_frame accept them as
//! stored, with the replica opened with the same key? If not, what does it take?
//!   A. insert the log's frames as stored
//!   B. decrypt each frame's page with the key (EncryptionContext), insert the plaintext frame
use probe::*;
use turso_core::{CipherMode, EncryptionContext, EncryptionKey};
use turso_durable::{Batching, Value};

const KEY: [u8; 32] = [0x5a; 32];

fn main() {
    let keydir = fresh_dir("q5key");
    let key_file = write_key_file(&keydir, KEY);
    let b = baseline_with("q5", Batching::Off, Some(key_file));
    b.w.execute("INSERT INTO t(id, v) VALUES (11, zeroblob(100))", &[]).unwrap();
    b.w.transaction(|t| {
        for i in 100..120 {
            t.execute("INSERT INTO t(id, v) VALUES (?, zeroblob(3000))", &[Value::from_i64(i)])?;
        }
        Ok(())
    })
    .unwrap();
    let writer_rows = b.w.query("SELECT count(*) FROM t", &[]).unwrap();
    println!("writer rows {writer_rows:?}, baseline app.db starts with {:?}", &std::fs::read(&b.replica_db_path).unwrap()[..6]);

    // the log's frames
    let mut rb = Rebuild::default();
    let mut frames: Vec<(u32, u32, Vec<u8>)> = vec![];
    for (_s, ops) in gen_syncs(b.store.as_ref(), b.gen) {
        if let Some(ops) = ops {
            frames.extend(rb.feed(&ops));
        }
    }
    println!("log frames: {}", frames.len());
    let commit_frames = frames.iter().filter(|f| f.1 != 0).count();
    println!("frame headers readable in the log as stored: page numbers {:?}..., commit frames (db_size != 0) {commit_frames}", frames.iter().take(6).map(|f| f.0).collect::<Vec<_>>());
    let page_of_commit = frames.iter().find(|f| f.1 != 0).unwrap();
    println!("a commit frame: page {} db_size {} body starts {:02x?} (plain zero pages would be 00..)", page_of_commit.0, page_of_commit.1, &page_of_commit.2[24..32]);

    // A: as stored
    let copy_a = fresh_dir("q5a").join("app.db");
    std::fs::copy(&b.replica_db_path, &copy_a).unwrap();
    let a_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let (_db, c) = open_core_enc(&copy_a, KEY);
        println!("A replica before: rows={} integrity={}", count(&c), integrity(&c));
        c.wal_insert_begin().unwrap();
        let mut n = 0;
        for (i, f) in frames.iter().enumerate() {
            match c.wal_insert_frame(i as u64 + 1, &f.2) {
                Ok(_) => n += 1,
                Err(e) => {
                    println!("A frame {} returned an error: {e:?}", i + 1);
                    break;
                }
            }
        }
        println!("A inserted {n}/{} frames as stored", frames.len());
    }));
    println!("A outcome: {}", if a_result.is_err() { "PANIC inside wal_insert_frame (message above)" } else { "no panic" });

    // B: decrypt first
    let copy_b = fresh_dir("q5b").join("app.db");
    std::fs::copy(&b.replica_db_path, &copy_b).unwrap();
    let (_db, c) = open_core_enc(&copy_b, KEY);
    println!("B replica before: rows={} integrity={}", count(&c), integrity(&c));
    let ctx = EncryptionContext::new(CipherMode::Aegis256, &EncryptionKey::new_256(KEY), 4096).unwrap();
    c.wal_insert_begin().unwrap();
    let mut shown = 0;
    for (i, f) in frames.iter().enumerate() {
        let (hdr, body) = f.2.split_at(24);
        let plain = ctx.decrypt_page(body, f.0 as usize).unwrap();
        if shown < 2 {
            println!("B frame {} page {}: ciphertext {:02x?} -> plaintext {:02x?} (len {} -> {})", i + 1, f.0, &body[..8], &plain[..8], body.len(), plain.len());
            shown += 1;
        }
        let mut out = hdr.to_vec();
        out.extend_from_slice(&plain);
        c.wal_insert_frame(i as u64 + 1, &out).unwrap();
        if i + 1 == frames.len() {
            println!("B last frame commit marker db_size={}", f.1);
        }
    }
    c.wal_insert_end(true).unwrap();
    println!("B replica after: rows={} integrity={}", count(&c), integrity(&c));
    let st = c.wal_state().unwrap();
    println!("B wal_state {st:?}");
    // stored bytes now differ from the writer's (fresh nonce per encode)
    let wal = std::fs::read(copy_b.with_file_name("app.db-wal")).unwrap();
    println!("B local wal length {} (writer log image length {})", wal.len(), rb.img.len());
    println!("B first stored frame body equals the log's: {}", wal.len() > 56 && wal[32 + 24..32 + 24 + 64] == frames[0].2[24..24 + 64]);
    // a wrong key is refused at open
    use turso_core::{DatabaseOpts, EncryptionOpts, OpenOptions};
    let io: std::sync::Arc<dyn turso_core::IO> = std::sync::Arc::new(turso_core::PlatformIO::new().unwrap());
    let opts = OpenOptions::new(std::sync::Arc::new(turso_core::SqliteDialect))
        .db_opts(DatabaseOpts::new().with_encryption(true))
        .encryption(EncryptionOpts { cipher: "aegis256".to_string(), hexkey: "11".repeat(32) });
    match turso_core::Database::open(io, copy_b.to_str().unwrap(), opts) {
        Err(e) => println!("B open with a wrong key: refused: {e:?}"),
        Ok(db) => match db.connect_with_encryption(Some(EncryptionKey::new_256([0x11; 32]))) {
            Err(e) => println!("B connect with a wrong key: refused: {e:?}"),
            Ok(c) => println!("B wrong key read: {:?}", c.query("SELECT count(*) FROM t").map(|s| s.map(|mut s| s.run_collect_rows()))),
        },
    }
}
