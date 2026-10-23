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
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;
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
    integrity: IntegrityKey,
}

impl Encryption {
    /// The integrity key is derived here, once, from the page key.
    pub(crate) fn new(cipher: CipherMode, key: EncryptionKey) -> Self {
        let integrity = IntegrityKey::derive(key.as_slice());
        Self {
            cipher,
            key,
            integrity,
        }
    }

    /// The key integrity codes are made with: HKDF of the page key, never the page key itself.
    /// Unused outside tests until the codes are wired in (Task 5).
    #[allow(dead_code)]
    pub(crate) fn integrity_key(&self) -> IntegrityKey {
        self.integrity.clone()
    }
}

/// HKDF info label of the integrity key.
const INTEGRITY_LABEL: &[u8] = b"buckwal integrity v1";

/// A 32-byte HMAC-SHA256 key for integrity codes. Wiped on drop; `Debug` prints no bytes.
#[derive(Clone)]
pub(crate) struct IntegrityKey([u8; 32]);

// Unused outside tests until the codes are wired in (Task 5).
#[allow(dead_code)]
impl IntegrityKey {
    fn derive(page_key: &[u8]) -> Self {
        let mut okm = hkdf_sha256(&[], page_key, INTEGRITY_LABEL, 32);
        let mut k = [0u8; 32];
        k.copy_from_slice(&okm);
        wipe(&mut okm);
        Self(k)
    }

    fn mac(&self, parts: &[&[u8]]) -> Hmac<Sha256> {
        let mut m =
            <Hmac<Sha256> as KeyInit>::new_from_slice(&self.0).expect("hmac takes any key length");
        // Each part is length-prefixed, so re-splitting the same bytes gives another code.
        for p in parts {
            m.update(&(p.len() as u64).to_be_bytes());
            m.update(p);
        }
        m
    }

    /// The integrity code over `parts` in order, each fed as its length (u64, big-endian) and
    /// then its bytes. A call site's first part is its kind (`page`, `record`, `manifest`,
    /// `head`), so a code of one kind never verifies as another.
    pub(crate) fn code(&self, parts: &[&[u8]]) -> [u8; 32] {
        self.mac(parts).finalize().into_bytes().into()
    }

    /// Whether `code` is the integrity code over `parts`; constant time, false for any
    /// length but 32.
    pub(crate) fn verify(&self, parts: &[&[u8]], code: &[u8]) -> bool {
        self.mac(parts).verify_slice(code).is_ok()
    }

    #[cfg(test)]
    fn bytes(&self) -> &[u8] {
        &self.0
    }
}

impl Drop for IntegrityKey {
    fn drop(&mut self) {
        wipe(&mut self.0);
    }
}

impl fmt::Debug for IntegrityKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

/// HKDF-SHA256 (RFC 5869): extract with `salt` (empty means 32 zero bytes), expand `info`
/// to `len` bytes. `len` is at most 255 * 32.
pub(crate) fn hkdf_sha256(salt: &[u8], ikm: &[u8], info: &[u8], len: usize) -> Vec<u8> {
    assert!(len <= 255 * 32, "hkdf output is at most 255 blocks");
    let zero = [0u8; 32];
    let salt = if salt.is_empty() { &zero[..] } else { salt };
    let mut ext = <Hmac<Sha256> as KeyInit>::new_from_slice(salt).expect("any key length");
    ext.update(ikm);
    let mut prk: [u8; 32] = ext.finalize().into_bytes().into();
    let mut okm = Vec::with_capacity(len + 32);
    let mut t: Vec<u8> = Vec::new();
    let mut counter = 1u8;
    while okm.len() < len {
        let mut m = <Hmac<Sha256> as KeyInit>::new_from_slice(&prk).expect("any key length");
        m.update(&t);
        m.update(info);
        m.update(&[counter]);
        t = m.finalize().into_bytes().to_vec();
        okm.extend_from_slice(&t);
        counter = counter.wrapping_add(1);
    }
    wipe(&mut prk);
    wipe(&mut t);
    okm.truncate(len);
    okm
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
    Ok(Some(Arc::new(Encryption::new(cipher, key))))
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

    // RFC 5869 appendix A, test cases 1-3 (SHA-256).
    fn rfc_bytes(from: u8, n: usize) -> Vec<u8> {
        (0..n).map(|i| from.wrapping_add(i as u8)).collect()
    }

    #[test]
    fn hkdf_rfc5869_case_1() {
        let okm = hkdf_sha256(&rfc_bytes(0, 13), &[0x0b; 22], &rfc_bytes(0xf0, 10), 42);
        assert_eq!(
            hex::encode(okm),
            "3cb25f25faacd57a90434f64d0362f2a2d2d0a90cf1a5a4c5db02d56ecc4c5bf34007208d5b887185865"
        );
    }

    #[test]
    fn hkdf_rfc5869_case_2() {
        let okm = hkdf_sha256(
            &rfc_bytes(0x60, 80),
            &rfc_bytes(0, 80),
            &rfc_bytes(0xb0, 80),
            82,
        );
        assert_eq!(
            hex::encode(okm),
            "b11e398dc80327a1c8e7f78c596a49344f012eda2d4efad8a050cc4c19afa97c59045a99cac7827271cb41c65e590e09da3275600c2f09b8367793a9aca3db71cc30c58179ec3e87c14c01d5c1f3434f1d87"
        );
    }

    #[test]
    fn hkdf_rfc5869_case_3() {
        let okm = hkdf_sha256(&[], &[0x0b; 22], &[], 42);
        assert_eq!(
            hex::encode(okm),
            "8da4e775a563c18f715f802a063c5a31b8a11f5c5ee1879ec3454e5f3c738d2d9d201395faa4b61a96c8"
        );
    }

    #[test]
    fn hkdf_max_length_and_zero() {
        assert_eq!(hkdf_sha256(b"s", b"k", b"i", 255 * 32).len(), 255 * 32);
        assert!(hkdf_sha256(b"s", b"k", b"i", 0).is_empty());
    }

    #[test]
    fn integrity_key_differs_from_page_key_and_ignores_newline() {
        let hex_key = "ab".repeat(32);
        let (_t, c) = with_key_file(hex_key.as_bytes());
        let a = load(&c).unwrap().unwrap();
        let (_t2, c2) = with_key_file(format!(" {hex_key}\r\n").as_bytes());
        let b = load(&c2).unwrap().unwrap();
        assert_eq!(
            a.integrity_key().code(&[b"x"]),
            b.integrity_key().code(&[b"x"])
        );
        assert_ne!(a.integrity_key().bytes(), a.key.as_slice());
        let (_t3, c3) = with_key_file("cd".repeat(32).as_bytes());
        let other = load(&c3).unwrap().unwrap();
        assert_ne!(
            a.integrity_key().code(&[b"x"]),
            other.integrity_key().code(&[b"x"])
        );
    }

    #[test]
    fn integrity_code_covers_parts_and_verifies() {
        let (_t, c) = with_key_file("ab".repeat(32).as_bytes());
        let ik = load(&c).unwrap().unwrap().integrity_key();
        let code = ik.code(&[&b"ab"[..], &b"c"[..]]);
        assert!(ik.verify(&[&b"ab"[..], &b"c"[..]], &code));
        // Parts are length-prefixed: the same bytes split elsewhere are another message.
        assert!(!ik.verify(&[&b"a"[..], &b"bc"[..]], &code));
        assert!(!ik.verify(&[&b"abc"[..]], &code));
        assert!(!ik.verify(&[b"abd"], &code));
        assert!(!ik.verify(&[b"abc"], &code[..31]));
        assert!(!ik.verify(&[b"abc"], &[]));
        let mut bad = code;
        bad[0] ^= 1;
        assert!(!ik.verify(&[b"abc"], &bad));
    }

    #[test]
    fn integrity_key_debug_is_redacted() {
        let (_t, c) = with_key_file("ab".repeat(32).as_bytes());
        let ik = load(&c).unwrap().unwrap().integrity_key();
        assert_eq!(format!("{ik:?}"), "<redacted>");
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
        let on = Encryption::new(CipherMode::Aegis256, key.clone());
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
        let other = Encryption::new(CipherMode::Aes256Gcm, key.clone());
        assert_eq!(
            encryption_err(check_page_one(Some(&enc), Some(&other))),
            "the database uses cipher aegis256, not BUCKWAL_ENCRYPTION_CIPHER aes256gcm"
        );
        let wrong = Encryption::new(CipherMode::Aegis256, EncryptionKey::new_256([0x22; 32]));
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
