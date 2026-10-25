//! Integrity codes of an encrypted bucket's objects (spec item 4): a page object ends with
//! one, a log record carries one in place of its SHA-256 trailer, and a manifest and the head
//! carry one. Each code is an HMAC-SHA256 under the integrity key (`IntegrityKey`); its first
//! part is the object's kind, so a code of one kind never verifies as another, and the
//! generation, seq or page number it covers ties the object to its place.
use crate::encryption::IntegrityKey;
use crate::error::{Error, Result};
use crate::pack::PackPointer;

/// The length of an integrity code.
pub(crate) const CODE_LEN: usize = 32;

/// Which kind of object failed its check (the `kind` field of the ERROR record).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    Page,
    Record,
    Manifest,
    Head,
    Pointer,
    /// A block of a pack.
    Pack,
    /// The pack codes object.
    PackCodes,
}

impl Kind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Kind::Page => "page",
            Kind::Record => "record",
            Kind::Manifest => "manifest",
            Kind::Head => "head",
            Kind::Pointer => "pointer",
            Kind::Pack => "pack",
            Kind::PackCodes => "pack_codes",
        }
    }
}

/// The failure of every integrity check, from one place: `Corrupt("integrity: <object key>")`
/// (the key only, never data or code bytes) and the ERROR record `integrity check failed`
/// (target `buckwal`) with the object key, its kind, generation and seq (0 for an object
/// without one). It also records `buckwal.outcome = integrity` on the innermost enclosing span
/// that declares the field (`telemetry::record_enclosing_outcome`: the engine's own spans
/// around a lazy page read have none); `restore`, `gc` and `takeover` also set it from the
/// error when they end (`Error::outcome`).
pub(crate) fn integrity_failed(key: &str, kind: Kind, generation: u64, seq: u64) -> Error {
    integrity_failed_as(key, "", kind, generation, seq)
}

/// `integrity_failed` with `detail` (for example ` format downgraded`) after the key in the
/// error only; the record still carries just the key.
pub(crate) fn integrity_failed_as(
    key: &str,
    detail: &str,
    kind: Kind,
    generation: u64,
    seq: u64,
) -> Error {
    tracing::error!(
        target: "buckwal",
        key,
        kind = kind.as_str(),
        generation,
        seq,
        "integrity check failed"
    );
    crate::telemetry::record_enclosing_outcome("integrity");
    Error::Corrupt(format!("integrity: {key}{detail}"))
}

/// What a coded decode needs: the integrity key, the object's key (for the error) and the
/// generation and seq it must be bound to (the seq is 0 for a manifest).
#[derive(Clone, Copy)]
pub(crate) struct Check<'a> {
    pub ik: &'a IntegrityKey,
    pub key: &'a str,
    pub generation: u64,
    pub seq: u64,
}

/// `ciphertext` followed by its integrity code over the page number, the generation and the
/// ciphertext.
pub(crate) fn seal_page(
    ik: &IntegrityKey,
    page_no: u32,
    generation: u64,
    ciphertext: &[u8],
) -> Vec<u8> {
    let code = ik.code(&[
        b"page",
        &page_no.to_be_bytes(),
        &generation.to_be_bytes(),
        ciphertext,
    ]);
    let mut out = Vec::with_capacity(ciphertext.len() + CODE_LEN);
    out.extend_from_slice(ciphertext);
    out.extend_from_slice(&code);
    out
}

/// The ciphertext of page object `obj` (at `key`), when its integrity code is right for
/// `page_no` and `generation`. Shorter than a code, or a wrong code: `Corrupt("integrity: key")`.
pub(crate) fn open_page<'a>(
    ik: &IntegrityKey,
    page_no: u32,
    generation: u64,
    obj: &'a [u8],
    key: &str,
) -> Result<&'a [u8]> {
    let fail = || integrity_failed(key, Kind::Page, generation, 0);
    let split = obj.len().checked_sub(CODE_LEN).ok_or_else(fail)?;
    let (ciphertext, code) = obj.split_at(split);
    let parts: [&[u8]; 4] = [
        b"page",
        &page_no.to_be_bytes(),
        &generation.to_be_bytes(),
        ciphertext,
    ];
    if ik.verify(&parts, code) {
        Ok(ciphertext)
    } else {
        Err(fail())
    }
}

/// The code of a log record's bytes at `(generation, seq)`.
pub(crate) fn record_code(ik: &IntegrityKey, generation: u64, seq: u64, body: &[u8]) -> [u8; 32] {
    ik.code(&[
        b"record",
        &generation.to_be_bytes(),
        &seq.to_be_bytes(),
        body,
    ])
}

/// Whether `code` is the code of a log record's bytes at `(generation, seq)`.
pub(crate) fn record_verifies(
    ik: &IntegrityKey,
    generation: u64,
    seq: u64,
    body: &[u8],
    code: &[u8],
) -> bool {
    ik.verify(
        &[
            b"record",
            &generation.to_be_bytes(),
            &seq.to_be_bytes(),
            body,
        ],
        code,
    )
}

/// The code of a manifest's bytes at generation `generation`.
pub(crate) fn manifest_code(ik: &IntegrityKey, generation: u64, body: &[u8]) -> [u8; 32] {
    ik.code(&[b"manifest", &generation.to_be_bytes(), body])
}

pub(crate) fn manifest_verifies(
    ik: &IntegrityKey,
    generation: u64,
    body: &[u8],
    code: &[u8],
) -> bool {
    ik.verify(&[b"manifest", &generation.to_be_bytes(), body], code)
}

/// The code of a head's canonical fields (`Head::encode` without the code).
pub(crate) fn head_code(ik: &IntegrityKey, fields: &[u8]) -> [u8; 32] {
    ik.code(&[b"head", fields])
}

pub(crate) fn head_verifies(ik: &IntegrityKey, fields: &[u8], code: &[u8]) -> bool {
    ik.verify(&[b"head", fields], code)
}

/// The parts of a pack pointer's code: every field of the pointer, so none can change without
/// the code failing. The database id has a presence byte, so "none" differs from "empty".
fn pointer_parts(p: &PackPointer) -> [Vec<u8>; 10] {
    let id = p.database_id.as_deref();
    [
        b"pack-latest".to_vec(),
        p.pack_key.as_bytes().to_vec(),
        p.codes_key.as_bytes().to_vec(),
        p.size.to_be_bytes().to_vec(),
        p.pack_generation.to_be_bytes().to_vec(),
        p.page_size.to_be_bytes().to_vec(),
        p.part_size.to_be_bytes().to_vec(),
        p.block_size.to_be_bytes().to_vec(),
        vec![u8::from(id.is_some())],
        id.unwrap_or_default().as_bytes().to_vec(),
    ]
}

/// The code of a pack pointer. The key is the database integrity key, so the code is also tied
/// to the database.
pub(crate) fn pointer_code(ik: &IntegrityKey, p: &PackPointer) -> [u8; 32] {
    let parts = pointer_parts(p);
    let refs: Vec<&[u8]> = parts.iter().map(Vec::as_slice).collect();
    ik.code(&refs)
}

pub(crate) fn pointer_verifies(ik: &IntegrityKey, p: &PackPointer, code: &[u8]) -> bool {
    let parts = pointer_parts(p);
    let refs: Vec<&[u8]> = parts.iter().map(Vec::as_slice).collect();
    ik.verify(&refs, code)
}

/// The length of a head's `key_check`.
pub(crate) const KEY_CHECK_LEN: usize = 16;

/// The `Encryption` error of a head whose `key_check` is not this key's: a wrong key, not
/// tampering (no `integrity check failed` record). Names the setting, never key material.
pub(crate) fn wrong_key() -> Error {
    Error::Encryption(
        "the key in BUCKWAL_ENCRYPTION_KEY_FILE is not the key this bucket was written with".into(),
    )
}

/// The `Encryption` error of an encrypted open of a head that has no integrity codes.
pub(crate) fn no_codes() -> Error {
    Error::Encryption(
        "the bucket has no integrity codes: it is plaintext, or was encrypted before this version"
            .into(),
    )
}

/// A head's `key_check`: the first 16 bytes of the integrity code over the fixed kind
/// `key_check`. It says which key the bucket was written with and reveals nothing of the key.
pub(crate) fn key_check(ik: &IntegrityKey) -> [u8; KEY_CHECK_LEN] {
    let mut out = [0u8; KEY_CHECK_LEN];
    out.copy_from_slice(&ik.code(&[b"key_check"])[..KEY_CHECK_LEN]);
    out
}

/// Whether `got` is this key's `key_check`; constant time.
pub(crate) fn key_check_matches(ik: &IntegrityKey, got: &[u8]) -> bool {
    got.len() == KEY_CHECK_LEN && ik.verify_truncated(&[b"key_check"], got)
}
