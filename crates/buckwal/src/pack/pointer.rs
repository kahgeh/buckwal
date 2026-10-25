//! `pack-latest`: flat JSON that names the newest pack. The pack key and the codes key are
//! stored relative to the database's prefix (`pack/0000000015/<run id>`, spec rev 8), so a
//! database copied with its head to another prefix still names its pack;
//! `Keys::encode_pointer` and `Keys::decode_pointer` convert. In an encrypted bucket it also
//! carries an integrity code over every field.
use crate::error::{Error, Result};
use crate::format::json::{escape, Obj};

/// The format number of a pointer.
pub(crate) const POINTER_FORMAT: u32 = 5;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PackPointer {
    pub format: u32,
    /// The key of the pack, for example `pack/0000000015/<run id>`.
    pub pack_key: String,
    /// The key of the pack's codes object (`<pack key>.codes`).
    pub codes_key: String,
    pub pack_generation: u64,
    /// The pack's total size, in bytes.
    pub size: u64,
    pub page_size: u32,
    /// The size of each part of the multipart upload that made the pack.
    pub part_size: u64,
    /// The size of a block that one code covers.
    pub block_size: u64,
    /// The database id of an encrypted bucket; `None` in a plaintext one.
    pub database_id: Option<String>,
}

fn corrupt(m: &str) -> Error {
    Error::Corrupt(format!("pack pointer: {m}"))
}

impl PackPointer {
    /// The fields as JSON, without the closing brace (a coded pointer adds its code).
    pub(crate) fn encode_fields(&self) -> String {
        let mut out = format!(
            "{{\"format\":{},\"pack_key\":{},\"codes_key\":{},\"pack_generation\":{},\"size\":{},\"page_size\":{},\"part_size\":{},\"block_size\":{}",
            self.format,
            escape(&self.pack_key),
            escape(&self.codes_key),
            self.pack_generation,
            self.size,
            self.page_size,
            self.part_size,
            self.block_size
        );
        if let Some(id) = &self.database_id {
            out.push_str(&format!(",\"database_id\":{}", escape(id)));
        }
        out
    }

    /// Decode the fields; also returns the `integrity` field (hex) when there is one.
    pub(crate) fn decode_fields(b: &[u8]) -> Result<(PackPointer, Option<String>)> {
        let o = Obj::parse(b)?;
        let format = o.num("format")?;
        if format != u64::from(POINTER_FORMAT) {
            return Err(corrupt(&format!("unsupported format {format}")));
        }
        let narrow = |k: &str| -> Result<u32> {
            u32::try_from(o.num(k)?).map_err(|_| corrupt(&format!("{k} is too large")))
        };
        let opt_str = |k: &str| -> Result<Option<String>> {
            if o.has(k) {
                Ok(Some(o.str(k)?.to_string()))
            } else {
                Ok(None)
            }
        };
        let p = PackPointer {
            format: POINTER_FORMAT,
            pack_key: o.str("pack_key")?.to_string(),
            codes_key: o.str("codes_key")?.to_string(),
            pack_generation: o.num("pack_generation")?,
            size: o.num("size")?,
            page_size: narrow("page_size")?,
            part_size: o.num("part_size")?,
            block_size: o.num("block_size")?,
            database_id: opt_str("database_id")?,
        };
        if p.block_size == 0 || p.part_size == 0 || p.page_size == 0 {
            return Err(corrupt("a size is 0"));
        }
        Ok((p, opt_str("integrity")?))
    }
}
