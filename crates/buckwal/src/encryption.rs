//! Encryption at rest (D16), through turso_core's own page codec: a key read from a local
//! file and a cipher. The engine encrypts every page before it writes `app.db`
//! (storage/database.rs:279-330) and every WAL frame (storage/wal.rs:3246-3256), so a seal's
//! pages and the log's extents are ciphertext, and the key never leaves the machine.
//!
//! Page 1 is checked against the setting before anything else touches an existing database
//! ([`check_page_one`]). turso takes an existing database's reserved space from its header
//! (database.rs:3119-3180), so a key on a plaintext database would corrupt it, and a missing,
//! wrong or other-cipher key is a clear `Encryption` error instead of an engine failure.
use crate::config::Config;
use crate::error::{Error, Result};
use crate::files::{io_err, DB_FILE, WAL_FILE};
use crate::io::BucketDbStorage;
use crate::restore::wal_page_one;
use std::fmt;
use std::fs;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use turso_core::{CipherMode, EncryptionContext, EncryptionKey};

const SQLITE_MAGIC: &[u8] = b"SQLite format 3\0";
/// An encrypted page 1 starts with `Turso`, a version byte and the cipher id
/// (storage/encryption.rs:70-75, 668-682).
const TURSO_MAGIC: &[u8] = b"Turso";
const CIPHER_ID_AT: usize = 6;
/// The largest page size; header bytes 16-17 hold 1 for it.
const MAX_PAGE: usize = 65536;

/// The cipher and key a database is opened with (`EngineConfig::encryption`).
pub(crate) struct Encryption {
    pub cipher: CipherMode,
    /// Debug-redacted and zeroed on drop by turso (storage/encryption.rs:143-170).
    pub key: EncryptionKey,
}

/// The cipher only, never the key.
impl fmt::Debug for Encryption {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Encryption")
            .field("cipher", &self.cipher)
            .finish_non_exhaustive()
    }
}

/// `BUCKWAL_ENCRYPTION_CIPHER`: any name `CipherMode::try_from` accepts
/// (storage/encryption.rs:425-444), else `Config`.
pub(crate) fn parse_cipher(name: &str) -> Result<CipherMode> {
    CipherMode::try_from(name).map_err(|_| {
        Error::Config(format!(
            "BUCKWAL_ENCRYPTION_CIPHER {name} is not a cipher turso supports"
        ))
    })
}

/// The encryption `cfg` sets: none without a key file. The file holds the key as hex, either
/// case, with ASCII whitespace around it, `cipher.required_key_size()` bytes long. Errors
/// name the file, never its content.
pub(crate) fn load(cfg: &Config) -> Result<Option<Arc<Encryption>>> {
    let Some(path) = &cfg.encryption_key_file else {
        return Ok(None);
    };
    let cipher = parse_cipher(&cfg.encryption_cipher)?;
    let mut text = fs::read(path)
        .map_err(|e| Error::Config(format!("encryption key file {}: {e}", path.display())))?;
    let key = parse_key(&text, cipher);
    wipe(&mut text);
    let key = key.ok_or_else(|| {
        let n = cipher.required_key_size();
        Error::Config(format!(
            "encryption key file {}: must hold {n} bytes as {} hex digits",
            path.display(),
            2 * n
        ))
    })?;
    Ok(Some(Arc::new(Encryption { cipher, key })))
}

/// `text` without its surrounding ASCII whitespace, as hex of exactly the cipher's key size.
fn parse_key(text: &[u8], cipher: CipherMode) -> Option<EncryptionKey> {
    // The decode error names the bad character: never passed on.
    let mut bytes = hex::decode(text.trim_ascii()).ok()?;
    let key = match (cipher.required_key_size(), bytes.len()) {
        (16, 16) => bytes.as_slice().try_into().ok().map(EncryptionKey::new_128),
        (32, 32) => bytes.as_slice().try_into().ok().map(EncryptionKey::new_256),
        _ => None,
    };
    wipe(&mut bytes);
    key
}

/// Zero key material before its buffer is freed (best effort: `black_box` keeps the writes).
fn wipe(bytes: &mut [u8]) {
    bytes.fill(0);
    std::hint::black_box(bytes);
}

/// The page size header bytes 16-17 give (big-endian; 1 means 65536), if it is one SQLite
/// allows: a power of two from 512 to 65536.
fn page_size(page1: &[u8]) -> Option<usize> {
    let field = u16::from_be_bytes(page1.get(16..18)?.try_into().ok()?);
    let ps = if field == 1 { MAX_PAGE } else { field as usize };
    (ps >= 512 && ps.is_power_of_two()).then_some(ps)
}

const WRONG_KEY: &str = "the key in BUCKWAL_ENCRYPTION_KEY_FILE does not decrypt this database \
                         (wrong key, or page 1 is damaged)";

/// Page 1 (any version of it: every one decrypts with the right key) against the setting.
/// None is a new database. A plaintext page 1 with a key, an encrypted one without a key or
/// under another cipher, or one the key does not decrypt (`EncryptionContext::decrypt_page`,
/// which authenticates it) is an `Encryption` error; neither header is `Corrupt`.
pub(crate) fn check_page_one(page1: Option<&[u8]>, enc: Option<&Encryption>) -> Result<()> {
    let Some(page1) = page1 else {
        return Ok(());
    };
    if page1.starts_with(SQLITE_MAGIC) {
        return match enc {
            None => Ok(()),
            Some(_) => Err(Error::Encryption(
                "the database is not encrypted and cannot be encrypted in place: unset \
                 BUCKWAL_ENCRYPTION_KEY_FILE or use a new prefix"
                    .into(),
            )),
        };
    }
    if !page1.starts_with(TURSO_MAGIC) {
        return Err(Error::Corrupt(
            "page 1 starts with neither the SQLite nor the Turso header".into(),
        ));
    }
    let id = page1.get(CIPHER_ID_AT).copied().unwrap_or(0);
    let name =
        CipherMode::from_cipher_id(id).map_or_else(|_| format!("id {id}"), |c| c.to_string());
    let Some(enc) = enc else {
        return Err(Error::Encryption(format!(
            "the database is encrypted (cipher {name}): set BUCKWAL_ENCRYPTION_KEY_FILE"
        )));
    };
    if id != enc.cipher.cipher_id() {
        return Err(Error::Encryption(format!(
            "the database uses cipher {name}, not BUCKWAL_ENCRYPTION_CIPHER {}",
            enc.cipher
        )));
    }
    // `decrypt_page` asserts a whole page (storage/encryption.rs:896-901).
    let ps = page_size(page1)
        .filter(|&ps| ps == page1.len())
        .ok_or_else(|| {
            Error::Corrupt(format!(
                "page 1 is {} bytes, not the page size its header gives",
                page1.len()
            ))
        })?;
    let ctx = EncryptionContext::new(enc.cipher, &enc.key, ps)
        .map_err(|e| Error::Encryption(e.to_string()))?;
    ctx.decrypt_page(page1, 1)
        .map(drop)
        .map_err(|_| Error::Encryption(WRONG_KEY.into()))
}

/// Page 1 from the local files alone: `app.db`'s first page when the file holds a whole one
/// (by its header's page size), else the local `-wal`'s first page-1 frame, else None.
pub(crate) fn file_page_one(dir: &Path) -> Result<Option<Vec<u8>>> {
    if let Some(p) = db_file_page_one(&dir.join(DB_FILE))? {
        return Ok(Some(p));
    }
    let wal = match fs::read(dir.join(WAL_FILE)) {
        Ok(w) => w,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(io_err("read app.db-wal", e)),
    };
    Ok(wal_page_one(&wal).map(<[u8]>::to_vec))
}

fn db_file_page_one(path: &Path) -> Result<Option<Vec<u8>>> {
    let f = match fs::File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(io_err("open app.db", e)),
    };
    let mut page = Vec::new();
    f.take(MAX_PAGE as u64)
        .read_to_end(&mut page)
        .map_err(|e| io_err("read app.db page 1", e))?;
    match page_size(&page) {
        Some(ps) if page.len() >= ps => {
            page.truncate(ps);
            Ok(Some(page))
        }
        _ => Ok(None),
    }
}

/// Page 1 as the engine will read it: fetched into `app.db` first when only the bucket holds
/// it (`BucketDbStorage::ensure_local`), then [`file_page_one`].
pub(crate) fn local_page_one(dir: &Path, storage: &BucketDbStorage) -> Result<Option<Vec<u8>>> {
    storage.ensure_local(1)?;
    file_page_one(dir)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use turso_core::{CipherMode, EncryptionContext, EncryptionKey};

    fn with_key_file(text: &[u8]) -> (tempfile::TempDir, Config) {
        let t = tempfile::tempdir().unwrap();
        let path = t.path().join("key");
        std::fs::write(&path, text).unwrap();
        let mut c = Config::new(t.path(), "http://e", "b", "ak", "sk");
        c.encryption_key_file = Some(path);
        (t, c)
    }

    fn config_err<T: std::fmt::Debug>(r: Result<T>) -> String {
        match r {
            Err(Error::Config(m)) => m,
            r => panic!("expected Config, got {r:?}"),
        }
    }

    #[test]
    fn key_file_hex_round_trip() {
        for text in [
            format!("{}\n", "ab".repeat(32)),
            format!(" \t{}\r\n", "AB".repeat(32)),
        ] {
            let (_t, c) = with_key_file(text.as_bytes());
            let enc = load(&c).unwrap().expect("encryption on");
            assert_eq!(enc.cipher, CipherMode::Aegis256);
            assert!(
                matches!(enc.key, EncryptionKey::Key256(k) if k == [0xab; 32]),
                "{text:?}"
            );
            let debug = format!("{enc:?}");
            assert!(debug.contains("Aegis256"), "{debug}");
            assert!(!debug.to_ascii_lowercase().contains("abab"), "{debug}");
            assert!(!debug.contains("171"), "{debug}"); // 0xab as a decimal byte
        }
        // A 16-byte cipher takes 32 hex digits.
        let (_t, mut c) = with_key_file("0f".repeat(16).as_bytes());
        c.encryption_cipher = "aes128gcm".into();
        let enc = load(&c).unwrap().unwrap();
        assert!(matches!(enc.key, EncryptionKey::Key128(k) if k == [0x0f; 16]));
        // No key file: off.
        let c = Config::new("/d", "http://e", "b", "ak", "sk");
        assert!(load(&c).unwrap().is_none());
    }

    #[test]
    fn key_file_wrong_length_is_config_error_without_content() {
        for text in [
            "cd".repeat(31),
            "cd".repeat(33),
            format!("{}zz", "cd".repeat(31)),
        ] {
            let (_t, c) = with_key_file(text.as_bytes());
            let path = c.encryption_key_file.clone().unwrap();
            let m = config_err(load(&c));
            assert!(m.contains(&path.display().to_string()), "{m}");
            assert!(m.contains("64 hex digits"), "{m}");
            assert!(!m.contains("cdcd") && !m.contains("zz"), "{m}");
        }
        // 64 digits for a 16-byte cipher.
        let (_t, mut c) = with_key_file("cd".repeat(32).as_bytes());
        c.encryption_cipher = "aegis128l".into();
        let m = config_err(load(&c));
        assert!(
            m.contains("16 bytes as 32 hex digits") && !m.contains("cdcd"),
            "{m}"
        );
    }

    #[test]
    fn key_file_missing_is_config_error() {
        let t = tempfile::tempdir().unwrap();
        let path = t.path().join("no-such-key");
        let mut c = Config::new(t.path(), "http://e", "b", "ak", "sk");
        c.encryption_key_file = Some(path.clone());
        let m = config_err(load(&c));
        assert!(
            m.starts_with(&format!("encryption key file {}: ", path.display())),
            "{m}"
        );
    }

    /// A plaintext page 1 of `ps` bytes (header page size field set), and its encryption
    /// under `cipher` and `key`.
    fn pages(ps: usize, cipher: CipherMode, key: &EncryptionKey) -> (Vec<u8>, Vec<u8>) {
        let mut plain = vec![0u8; ps];
        plain[..16].copy_from_slice(b"SQLite format 3\0");
        let field: u16 = if ps == 65536 { 1 } else { ps as u16 };
        plain[16..18].copy_from_slice(&field.to_be_bytes());
        plain[20] = cipher.metadata_size() as u8;
        plain[100..105].copy_from_slice(b"hello");
        let ctx = EncryptionContext::new(cipher, key, ps).unwrap();
        let enc = ctx.encrypt_page(&plain, 1).unwrap();
        (plain, enc)
    }

    fn encryption_err(r: Result<()>) -> String {
        match r {
            Err(Error::Encryption(m)) => m,
            r => panic!("expected Encryption, got {r:?}"),
        }
    }

    #[test]
    fn check_page_one_follows_the_table() {
        let key = EncryptionKey::new_256([0x11; 32]);
        let on = Encryption {
            cipher: CipherMode::Aegis256,
            key: key.clone(),
        };
        let (plain, enc) = pages(4096, CipherMode::Aegis256, &key);
        assert!(enc.starts_with(b"Turso"));
        assert_eq!(check_page_one(None, None), Ok(()));
        assert_eq!(check_page_one(None, Some(&on)), Ok(()));
        assert_eq!(check_page_one(Some(&plain), None), Ok(()));
        assert_eq!(check_page_one(Some(&enc), Some(&on)), Ok(()));
        assert_eq!(
            encryption_err(check_page_one(Some(&plain), Some(&on))),
            "the database is not encrypted and cannot be encrypted in place: unset \
             BUCKWAL_ENCRYPTION_KEY_FILE or use a new prefix"
        );
        assert_eq!(
            encryption_err(check_page_one(Some(&enc), None)),
            "the database is encrypted (cipher aegis256): set BUCKWAL_ENCRYPTION_KEY_FILE"
        );
        let other = Encryption {
            cipher: CipherMode::Aes256Gcm,
            key: key.clone(),
        };
        assert_eq!(
            encryption_err(check_page_one(Some(&enc), Some(&other))),
            "the database uses cipher aegis256, not BUCKWAL_ENCRYPTION_CIPHER aes256gcm"
        );
        let wrong = Encryption {
            cipher: CipherMode::Aegis256,
            key: EncryptionKey::new_256([0x22; 32]),
        };
        let wrong_key = "the key in BUCKWAL_ENCRYPTION_KEY_FILE does not decrypt this database \
                         (wrong key, or page 1 is damaged)";
        assert_eq!(
            encryption_err(check_page_one(Some(&enc), Some(&wrong))),
            wrong_key
        );
        // A damaged page fails authentication like a wrong key.
        let mut damaged = enc.clone();
        damaged[200] ^= 1;
        assert_eq!(
            encryption_err(check_page_one(Some(&damaged), Some(&on))),
            wrong_key
        );
        for junk in [vec![0u8; 4096], b"hello".to_vec(), Vec::new()] {
            assert_eq!(
                check_page_one(Some(&junk), Some(&on)),
                Err(Error::Corrupt(
                    "page 1 starts with neither the SQLite nor the Turso header".into()
                ))
            );
        }
        // A Turso page 1 that is not one whole page cannot be decrypted: Corrupt, no panic.
        assert!(matches!(
            check_page_one(Some(&enc[..4000]), Some(&on)),
            Err(Error::Corrupt(_))
        ));
        // The page size field's 1 means 65536.
        let (_, big) = pages(65536, CipherMode::Aegis256, &key);
        assert_eq!(check_page_one(Some(&big), Some(&on)), Ok(()));
        assert_eq!(
            encryption_err(check_page_one(Some(&big), Some(&wrong))),
            wrong_key
        );
    }

    #[test]
    fn file_page_one_reads_the_db_file_then_the_wal() {
        let t = tempfile::tempdir().unwrap();
        assert_eq!(file_page_one(t.path()), Ok(None));
        let key = EncryptionKey::new_256([0x11; 32]);
        let (_, enc) = pages(4096, CipherMode::Aegis256, &key);
        // A WAL with a page-2 frame, then a page-1 frame.
        let mut wal = vec![0u8; 32];
        wal[8..12].copy_from_slice(&4096u32.to_be_bytes());
        for (pgno, body) in [(2u32, vec![7u8; 4096]), (1, enc.clone())] {
            let mut header = [0u8; 24];
            header[..4].copy_from_slice(&pgno.to_be_bytes());
            wal.extend_from_slice(&header);
            wal.extend_from_slice(&body);
        }
        std::fs::write(t.path().join(WAL_FILE), &wal).unwrap();
        // An empty or partial app.db: the WAL's page 1.
        std::fs::write(t.path().join(DB_FILE), b"").unwrap();
        assert_eq!(file_page_one(t.path()), Ok(Some(enc.clone())));
        std::fs::write(t.path().join(DB_FILE), &enc[..2000]).unwrap();
        assert_eq!(file_page_one(t.path()), Ok(Some(enc.clone())));
        // A whole page in app.db wins, and only page 1 is read.
        let mut db = vec![9u8; 3 * 4096];
        db[..4096].copy_from_slice(&enc);
        std::fs::write(t.path().join(DB_FILE), &db).unwrap();
        std::fs::write(t.path().join(WAL_FILE), b"").unwrap();
        assert_eq!(file_page_one(t.path()), Ok(Some(enc)));
    }
}
