//! The pack codes object: one 32-byte code per 1 MiB block of a pack, then a code over all of
//! it. In a plaintext bucket a code is a SHA-256. In an encrypted bucket it is an integrity code
//! under the database integrity key. Each block code covers the database id, the pack key, the
//! pack generation, the pack's total size and the block number. The codes object's own code
//! covers the same, and all block codes.
use crate::error::{Error, Result};
use crate::format::integrity::{integrity_failed, Kind, CODE_LEN};
use crate::format::Keys;
use sha2::{Digest, Sha256};

/// The size of a block that one code covers.
pub(crate) const BLOCK_SIZE: u64 = 1 << 20;

/// The codes of a pack, one per block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PackCodes {
    codes: Vec<[u8; CODE_LEN]>,
}

/// What every code covers, besides its own kind and data.
struct Binding<'a> {
    keys: &'a Keys,
    pack_key: &'a str,
    pack_generation: u64,
    total: u64,
}

impl Binding<'_> {
    /// The code of `kind` over the binding, `extra` and `data`. Each part is fed as its length
    /// and then its bytes, as `IntegrityKey::code` does, in both kinds of bucket.
    fn code(&self, kind: &[u8], extra: &[u8], data: &[u8]) -> [u8; CODE_LEN] {
        let id = self.keys.known_database_id().unwrap_or_default();
        let parts: [&[u8]; 7] = [
            kind,
            id.as_bytes(),
            self.pack_key.as_bytes(),
            &self.pack_generation.to_be_bytes(),
            &self.total.to_be_bytes(),
            extra,
            data,
        ];
        match self.keys.object_key() {
            Some(ik) => ik.code(&parts),
            None => {
                let mut h = Sha256::new();
                for p in parts {
                    h.update((p.len() as u64).to_be_bytes());
                    h.update(p);
                }
                h.finalize().into()
            }
        }
    }

    fn block_code(&self, n: u64, bytes: &[u8]) -> [u8; CODE_LEN] {
        self.code(b"pack-block", &n.to_be_bytes(), bytes)
    }

    fn codes_code(&self, codes: &[u8]) -> [u8; CODE_LEN] {
        self.code(b"pack-codes", &[], codes)
    }

    /// The failure of a check: the integrity error and record in an encrypted bucket, else the
    /// checksum error.
    fn fail(&self, key: &str, kind: Kind, seq: u64) -> Error {
        if self.keys.coded() {
            return integrity_failed(key, kind, self.pack_generation, seq);
        }
        Error::Corrupt(format!("checksum mismatch: {key}"))
    }
}

/// The number of blocks of a pack of `total` bytes.
fn block_count(total: u64) -> u64 {
    total.div_ceil(BLOCK_SIZE)
}

impl PackCodes {
    /// The length of the encoded codes object of a pack of `total` bytes.
    pub(crate) fn encoded_len(total: u64) -> usize {
        (block_count(total) as usize + 1) * CODE_LEN
    }

    /// The codes of `blocks`, in order. They must be the whole pack: all of `BLOCK_SIZE` bytes
    /// but the last, and `total` bytes together.
    pub(crate) fn build<'a>(
        keys: &Keys,
        pack_key: &str,
        pack_generation: u64,
        total: u64,
        blocks: impl Iterator<Item = &'a [u8]>,
    ) -> PackCodes {
        let b = Binding {
            keys,
            pack_key,
            pack_generation,
            total,
        };
        let mut seen = 0u64;
        let codes: Vec<_> = blocks
            .enumerate()
            .map(|(n, bytes)| {
                seen += bytes.len() as u64;
                b.block_code(n as u64, bytes)
            })
            .collect();
        debug_assert_eq!(seen, total, "the blocks are not the whole pack");
        debug_assert_eq!(codes.len() as u64, block_count(total));
        PackCodes { codes }
    }

    /// The block codes, then the code over all of them.
    pub(crate) fn encode(
        &self,
        keys: &Keys,
        pack_key: &str,
        pack_generation: u64,
        total: u64,
    ) -> Vec<u8> {
        let b = Binding {
            keys,
            pack_key,
            pack_generation,
            total,
        };
        let mut out = Vec::with_capacity((self.codes.len() + 1) * CODE_LEN);
        for c in &self.codes {
            out.extend_from_slice(c);
        }
        let own = b.codes_code(&out);
        out.extend_from_slice(&own);
        out
    }

    /// The codes in `body`, when its own code is right for this pack and it holds one code per
    /// block. Else an integrity error naming the codes key (`<pack key>.codes`) in an encrypted
    /// bucket, a checksum error in a plaintext one.
    pub(crate) fn decode(
        keys: &Keys,
        body: &[u8],
        pack_key: &str,
        pack_generation: u64,
        total: u64,
    ) -> Result<PackCodes> {
        let b = Binding {
            keys,
            pack_key,
            pack_generation,
            total,
        };
        let codes_key = format!("{pack_key}.codes");
        let fail = || b.fail(&codes_key, Kind::PackCodes, 0);
        let split = body.len().checked_sub(CODE_LEN).ok_or_else(fail)?;
        let (codes, own) = body.split_at(split);
        if !constant_time_eq(&b.codes_code(codes), own) {
            return Err(fail());
        }
        if codes.len() as u64 != block_count(total) * CODE_LEN as u64 {
            return Err(fail());
        }
        let codes = codes
            .chunks_exact(CODE_LEN)
            .map(|c| c.try_into().expect("chunks of CODE_LEN"))
            .collect();
        Ok(PackCodes { codes })
    }

    /// Check block `n` (`bytes`) of the pack. The block must have the length its place needs:
    /// `BLOCK_SIZE`, or what is left of `total` for the last block.
    pub(crate) fn verify_block(
        &self,
        keys: &Keys,
        pack_key: &str,
        pack_generation: u64,
        total: u64,
        n: u64,
        bytes: &[u8],
    ) -> Result<()> {
        let b = Binding {
            keys,
            pack_key,
            pack_generation,
            total,
        };
        let fail = || b.fail(pack_key, Kind::Pack, n);
        let want = total
            .saturating_sub(n.saturating_mul(BLOCK_SIZE))
            .min(BLOCK_SIZE);
        if n >= block_count(total) || bytes.len() as u64 != want {
            return Err(fail());
        }
        let code = self.codes.get(n as usize).ok_or_else(fail)?;
        if constant_time_eq(&b.block_code(n, bytes), code) {
            Ok(())
        } else {
            Err(fail())
        }
    }
}

/// Whether two codes are equal; the time does not depend on where they differ.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |d, (x, y)| d | (x ^ y)) == 0
}
