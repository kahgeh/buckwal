use super::codes::{PackCodes, BLOCK_SIZE};
use crate::encryption::Encryption;
use crate::error::Error;
use crate::format::Keys;
use turso_core::{CipherMode, EncryptionKey};

const MIB: usize = 1 << 20;

fn plain() -> Keys {
    Keys::new("p")
}

fn coded(seed: u8) -> Keys {
    let ik =
        Encryption::new(CipherMode::Aegis256, EncryptionKey::new_256([seed; 32])).integrity_key();
    let k = Keys::new("p").with_integrity(Some(ik));
    k.init_database_id().unwrap();
    k
}

/// 2.5 MiB of data with a pattern: three blocks, the last one half a block.
fn data() -> Vec<u8> {
    (0..(5 * MIB / 2)).map(|i| (i % 251) as u8).collect()
}

fn blocks(d: &[u8]) -> Vec<&[u8]> {
    d.chunks(MIB).collect()
}

fn built(k: &Keys, d: &[u8]) -> PackCodes {
    PackCodes::build(
        k,
        "p/pack/0000000015/r1",
        15,
        d.len() as u64,
        blocks(d).into_iter(),
    )
}

fn each(f: impl Fn(&Keys)) {
    f(&plain());
    f(&coded(1));
}

fn integrity_err(r: crate::Result<()>, key: &str) -> bool {
    matches!(r, Err(Error::Corrupt(m)) if m == format!("integrity: {key}"))
}

#[test]
fn a_block_of_one_mib_verifies() {
    each(|k| {
        let d = data();
        let c = built(k, &d);
        let total = d.len() as u64;
        for (n, b) in blocks(&d).into_iter().enumerate() {
            c.verify_block(k, "p/pack/0000000015/r1", 15, total, n as u64, b)
                .unwrap();
        }
        assert_eq!(BLOCK_SIZE, MIB as u64);
    });
}

#[test]
fn a_changed_byte_fails() {
    each(|k| {
        let d = data();
        let c = built(k, &d);
        let mut b = blocks(&d)[1].to_vec();
        b[777] ^= 1;
        let r = c.verify_block(k, "p/pack/0000000015/r1", 15, d.len() as u64, 1, &b);
        if k.coded() {
            assert!(integrity_err(r, "p/pack/0000000015/r1"));
        } else {
            assert!(matches!(r, Err(Error::Corrupt(m)) if m.contains("checksum mismatch")));
        }
    });
}

#[test]
fn a_block_at_another_number_fails() {
    each(|k| {
        let d = data();
        let c = built(k, &d);
        let b = blocks(&d)[0];
        assert!(c
            .verify_block(k, "p/pack/0000000015/r1", 15, d.len() as u64, 1, b)
            .is_err());
        assert!(c
            .verify_block(k, "p/pack/0000000015/r1", 15, d.len() as u64, 7, b)
            .is_err());
    });
}

#[test]
fn a_block_of_another_pack_key_or_generation_fails() {
    each(|k| {
        let d = data();
        let c = built(k, &d);
        let b = blocks(&d)[0];
        let t = d.len() as u64;
        assert!(c
            .verify_block(k, "p/pack/0000000015/r2", 15, t, 0, b)
            .is_err());
        assert!(c
            .verify_block(k, "p/pack/0000000015/r1", 16, t, 0, b)
            .is_err());
    });
}

#[test]
fn a_pack_cut_short_fails() {
    each(|k| {
        let d = data();
        let c = built(k, &d);
        let b = blocks(&d)[0];
        let t = d.len() as u64;
        assert!(c
            .verify_block(k, "p/pack/0000000015/r1", 15, t - 1, 0, b)
            .is_err());
        // A last block cut short fails too.
        let last = blocks(&d)[2];
        assert!(c
            .verify_block(k, "p/pack/0000000015/r1", 15, t, 2, &last[..last.len() - 1])
            .is_err());
    });
}

#[test]
fn the_codes_object_round_trips_and_a_changed_byte_fails_its_own_code() {
    each(|k| {
        let d = data();
        let c = built(k, &d);
        let t = d.len() as u64;
        let key = "p/pack/0000000015/r1";
        let body = c.encode(k, key, 15, t);
        assert_eq!(PackCodes::decode(k, &body, key, 15, t).unwrap(), c);
        for at in [0, 40, body.len() - 1] {
            let mut bad = body.clone();
            bad[at] ^= 1;
            let r = PackCodes::decode(k, &bad, key, 15, t);
            assert!(matches!(r, Err(Error::Corrupt(_))), "byte {at}: {r:?}");
        }
        // Cut short, or read for another pack, another generation or another total.
        assert!(PackCodes::decode(k, &body[..body.len() - 1], key, 15, t).is_err());
        assert!(PackCodes::decode(k, &body, "p/pack/0000000015/r2", 15, t).is_err());
        assert!(PackCodes::decode(k, &body, key, 16, t).is_err());
        assert!(PackCodes::decode(k, &body, key, 15, t + 1).is_err());
    });
}

#[test]
fn a_coded_codes_object_fails_with_the_integrity_record_and_a_wrong_key() {
    let k = coded(1);
    let d = data();
    let t = d.len() as u64;
    let key = "p/pack/0000000015/r1";
    let body = built(&k, &d).encode(&k, key, 15, t);
    let mut bad = body.clone();
    bad[3] ^= 1;
    let r = PackCodes::decode(&k, &bad, key, 15, t);
    assert!(matches!(r, Err(Error::Corrupt(m)) if m == format!("integrity: {key}.codes")));
    // Codes made under another key do not verify.
    let other = coded(2);
    assert!(PackCodes::decode(&other, &body, key, 15, t).is_err());
}

#[test]
fn one_gigabyte_gives_32_kb_of_codes() {
    // The body is 32 bytes per block, then the object's own 32-byte code.
    assert_eq!(PackCodes::encoded_len(1 << 30), 32 * 1024 + 32);
    let k = plain();
    let one = vec![9u8; MIB];
    let c = PackCodes::build(
        &k,
        "p/pack/0000000001/r",
        1,
        3 * MIB as u64,
        std::iter::repeat_n(&one[..], 3),
    );
    assert_eq!(
        c.encode(&k, "p/pack/0000000001/r", 1, 3 * MIB as u64).len(),
        PackCodes::encoded_len(3 << 20)
    );
}

#[test]
fn an_empty_pack_has_no_blocks() {
    each(|k| {
        let c = PackCodes::build(k, "p/pack/0000000001/r", 1, 0, std::iter::empty());
        let body = c.encode(k, "p/pack/0000000001/r", 1, 0);
        assert_eq!(body.len(), 32);
        assert_eq!(
            PackCodes::decode(k, &body, "p/pack/0000000001/r", 1, 0).unwrap(),
            c
        );
    });
}
