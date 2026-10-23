//! Integrity codes of an encrypted bucket's objects (spec item 4): a page object ends with
//! one, a log record carries one in place of its SHA-256 trailer, and a manifest and the head
//! carry one. Each code is an HMAC-SHA256 under the integrity key (`IntegrityKey`); its first
//! part is the object's kind, so a code of one kind never verifies as another, and the
//! generation, seq or page number it covers ties the object to its place.
use crate::encryption::IntegrityKey;
use crate::error::{Error, Result};

/// The length of an integrity code.
pub(crate) const CODE_LEN: usize = 32;

/// Which kind of object failed its check (the `kind` field of the ERROR record).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    Page,
    Record,
    Manifest,
    Head,
}

impl Kind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Kind::Page => "page",
            Kind::Record => "record",
            Kind::Manifest => "manifest",
            Kind::Head => "head",
        }
    }
}

/// The failure of every integrity check, from one place: `Corrupt("integrity: <object key>")`
/// (the key only, never data or code bytes) and the ERROR record `integrity check failed`
/// (target `buckwal`) with the object key, its kind, generation and seq (0 for an object
/// without one). The telemetry task attaches `buckwal.outcome = integrity` here.
pub(crate) fn integrity_failed(key: &str, kind: Kind, generation: u64, seq: u64) -> Error {
    tracing::error!(
        target: "buckwal",
        key,
        kind = kind.as_str(),
        generation,
        seq,
        "integrity check failed"
    );
    Error::Corrupt(format!("integrity: {key}"))
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
