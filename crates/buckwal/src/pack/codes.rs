//! The pack codes object: one 32-byte code per 1 MiB block of a pack, then a code over all of
//! it. In a plaintext bucket a code is a SHA-256. In an encrypted bucket it is an integrity code
//! under the database integrity key. Each block code covers the database id, the pack key
//! (relative to the prefix), the
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
    /// The database id. A format 5 head has one in a plaintext bucket too.
    id: String,
    pack_key: &'a str,
    pack_generation: u64,
    total: u64,
}

impl<'a> Binding<'a> {
    /// The binding of a pack. In an encrypted bucket the keys must know the database id (a
    /// format 5 head was read or made), else `Corrupt`: a code without it would not bind the
    /// pack to its database.
    fn new(keys: &'a Keys, pack_key: &'a str, pack_generation: u64, total: u64) -> Result<Self> {
        let id = match keys.known_database_id() {
            Some(id) => id,
            None if keys.coded() => {
                return Err(Error::Corrupt(format!(
                    "pack codes of {pack_key}: the database id is not known"
                )))
            }
            None => String::new(),
        };
        Ok(Binding {
            keys,
            id,
            // Relative to the prefix (spec rev 8): a database copied with its head to another
            // prefix still verifies its packs.
            pack_key: keys.relative(pack_key),
            pack_generation,
            total,
        })
    }

    /// The code of `kind` over the binding, `extra` and `data`. Each part is fed as its length
    /// and then its bytes, as `IntegrityKey::code` does, in both kinds of bucket.
    fn code(&self, kind: &[u8], extra: &[u8], data: &[u8]) -> [u8; CODE_LEN] {
        let parts: [&[u8]; 7] = [
            kind,
            self.id.as_bytes(),
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
    #[cfg(test)]
    pub(crate) fn encoded_len(total: u64) -> usize {
        (block_count(total) as usize + 1) * CODE_LEN
    }

    /// The codes of `blocks`, in order. They must be the whole pack: all of `BLOCK_SIZE` bytes
    /// but the last, and `total` bytes together (`Corrupt` otherwise).
    #[cfg(test)]
    pub(crate) fn build<'a, B: AsRef<[u8]> + 'a>(
        keys: &Keys,
        pack_key: &str,
        pack_generation: u64,
        total: u64,
        blocks: impl Iterator<Item = B>,
    ) -> Result<PackCodes> {
        let mut b = CodesBuilder::new(keys, pack_key, pack_generation, total)?;
        for block in blocks {
            b.block(block.as_ref())?;
        }
        b.finish()
    }

    /// The block codes, then the code over all of them.
    pub(crate) fn encode(
        &self,
        keys: &Keys,
        pack_key: &str,
        pack_generation: u64,
        total: u64,
    ) -> Result<Vec<u8>> {
        let b = Binding::new(keys, pack_key, pack_generation, total)?;
        let mut out = Vec::with_capacity((self.codes.len() + 1) * CODE_LEN);
        for c in &self.codes {
            out.extend_from_slice(c);
        }
        let own = b.codes_code(&out);
        out.extend_from_slice(&own);
        Ok(out)
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
        let b = Binding::new(keys, pack_key, pack_generation, total)?;
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
        let b = Binding::new(keys, pack_key, pack_generation, total)?;
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

/// The codes of a pack, made one block at a time, in order, as the pack is read.
pub(crate) struct CodesBuilder<'a> {
    binding: Binding<'a>,
    codes: Vec<[u8; CODE_LEN]>,
    /// The bytes of the blocks so far.
    seen: u64,
}

impl<'a> CodesBuilder<'a> {
    pub(crate) fn new(
        keys: &'a Keys,
        pack_key: &'a str,
        pack_generation: u64,
        total: u64,
    ) -> Result<Self> {
        Ok(CodesBuilder {
            binding: Binding::new(keys, pack_key, pack_generation, total)?,
            codes: Vec::with_capacity(block_count(total) as usize),
            seen: 0,
        })
    }

    fn layout(&self, m: &str) -> Error {
        Error::Corrupt(format!("pack codes of {}: {m}", self.binding.pack_key))
    }

    /// The next block. It must be `BLOCK_SIZE` bytes, or what is left of the pack for the
    /// last block (`Corrupt` otherwise).
    pub(crate) fn block(&mut self, bytes: &[u8]) -> Result<()> {
        let want = (self.binding.total - self.seen).min(BLOCK_SIZE);
        if want == 0 || bytes.len() as u64 != want {
            return Err(self.layout(&format!(
                "block {} is {} bytes, not {want}",
                self.codes.len(),
                bytes.len()
            )));
        }
        let n = self.codes.len() as u64;
        self.codes.push(self.binding.block_code(n, bytes));
        self.seen += want;
        Ok(())
    }

    /// The codes, once the blocks were the whole pack.
    pub(crate) fn finish(self) -> Result<PackCodes> {
        if self.seen != self.binding.total {
            return Err(self.layout(&format!(
                "the blocks hold {} of {} bytes",
                self.seen, self.binding.total
            )));
        }
        Ok(PackCodes { codes: self.codes })
    }
}

/// Whether two codes are equal; the time does not depend on where they differ.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |d, (x, y)| d | (x ^ y)) == 0
}
