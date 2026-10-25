use super::*;
use crate::config::JournalMode;
use crate::format::LogFile;
use crate::Error;
use std::collections::{BTreeMap, BTreeSet};

fn is_corrupt<T: std::fmt::Debug>(r: crate::Result<T>) -> bool {
    matches!(r, Err(Error::Corrupt(_)))
}

#[test]
fn keys_with_and_without_prefix() {
    let k = Keys::new("p");
    assert_eq!(k.log(3, 7), "p/log/0000000003/00000000000000000007");
    assert_eq!(k.log_prefix(3), "p/log/0000000003/");
    assert_eq!(k.page(2, 9), "p/pages/0000000002/0000000009");
    assert_eq!(k.manifest(2), "p/manifest/0000000002");
    assert_eq!(k.head(), "p/head");
    assert_eq!(Keys::new("").head(), "head");
    assert_eq!(Keys::new("").manifest(2), "manifest/0000000002");
}

#[test]
fn key_parsers_round_trip_and_reject_malformed() {
    for k in [Keys::new("p"), Keys::new("")] {
        assert_eq!(k.parse_log(&k.log(3, 7)), Some((3, 7)));
        assert_eq!(k.parse_page(&k.page(2, 9)), Some((2, 9)));
        assert_eq!(k.parse_manifest(&k.manifest(5)), Some(5));
        assert_eq!(k.parse_log(&k.page(2, 9)), None);
        assert_eq!(k.parse_log("log/3/7"), None);
        assert_eq!(k.parse_log(&format!("{}x", k.log(3, 7))), None);
        assert_eq!(k.parse_page("nope"), None);
        // v1 page keys live under `pages/`; the singular form is not a page key.
        assert_eq!(k.parse_page(&k.page(2, 9).replace("pages/", "page/")), None);
        assert_eq!(k.parse_manifest(&k.head()), None);
        assert_eq!(k.parse_manifest(&format!("{}/extra", k.manifest(1))), None);
    }
    assert_eq!(Keys::new("p").parse_manifest("manifest/0000000002"), None);
}

fn sample_extents() -> LogRecord {
    LogRecord::Extents {
        file: LogFile::Wal,
        writer_id: "ab12".into(),
        syncs: vec![
            vec![
                Op::Write {
                    off: 4096,
                    bytes: vec![1, 2, 3],
                },
                Op::Truncate { len: 8192 },
            ],
            vec![Op::Write {
                off: 0,
                bytes: vec![],
            }],
        ],
        next_generation: None,
    }
}

#[test]
fn record_round_trip_multi_sync() {
    let r = sample_extents();
    assert_eq!(decode_record(&encode_record(&r)).unwrap(), r);
}

#[test]
fn record_rejects_flipped_byte() {
    let enc = encode_record(&sample_extents());
    for i in 0..enc.len() {
        let mut b = enc.clone();
        b[i] ^= 0x40;
        assert!(is_corrupt(decode_record(&b)), "byte {i}");
    }
    assert!(is_corrupt(decode_record(&enc[..enc.len() - 1])));
    assert!(is_corrupt(decode_record(&[])));
}

#[test]
fn fence_record_round_trips() {
    let r = LogRecord::Fence {
        writer_id: "ff00".into(),
        epoch: 9,
    };
    assert_eq!(decode_record(&encode_record(&r)).unwrap(), r);
}

#[test]
fn apply_ops_matches_file_semantics() {
    let mut buf = Vec::new();
    apply_ops(
        &mut buf,
        &[Op::Write {
            off: 10,
            bytes: vec![7, 8, 9],
        }],
    )
    .unwrap();
    assert_eq!(buf.len(), 13);
    assert!(buf[..10].iter().all(|b| *b == 0));
    assert_eq!(&buf[10..], &[7, 8, 9]);
    apply_ops(&mut buf, &[Op::Truncate { len: 4 }]).unwrap();
    assert_eq!(buf.len(), 4);
    apply_ops(&mut buf, &[Op::Truncate { len: 6 }]).unwrap();
    assert_eq!(buf, vec![0; 6]);
}

#[test]
fn manifest_runs_compact() {
    let m = Manifest {
        page_size: 4096,
        page_count: 1000,
        pages: (1..=1000).map(|p| (p, 1)).collect(),
    };
    let enc = m.encode();
    // Fixed overhead is 52 bytes (magic, header, sha) plus 16 per run, so 68 here.
    assert_eq!(enc.len(), 68, "len {}", enc.len());
    assert_eq!(Manifest::decode(&enc).unwrap(), m);

    let dirty: BTreeSet<u32> = [5, 6, 1000, 2000].into();
    let s = m.sealed(2, &dirty, 4096, 900);
    assert_eq!(s.page_count, 900);
    assert_eq!(s.pages.len(), 900);
    assert_eq!(s.pages[&5], 2);
    assert_eq!(s.pages[&6], 2);
    assert_eq!(s.pages[&7], 1);
    assert!(!s.pages.contains_key(&1000));
    assert_eq!(Manifest::decode(&s.encode()).unwrap(), s);
}

#[test]
fn manifest_decode_validates() {
    let mk = |pages: &[(u32, u64)], count| Manifest {
        page_size: 4096,
        page_count: count,
        pages: pages.iter().copied().collect::<BTreeMap<_, _>>(),
    };
    let ok = mk(&[(1, 1), (2, 1), (3, 2)], 3);
    assert_eq!(Manifest::decode(&ok.encode()).unwrap(), ok);
    // page beyond page_count
    assert!(is_corrupt(Manifest::decode(
        &mk(&[(1, 1), (4, 1)], 3).encode()
    )));
    // page 0
    assert!(is_corrupt(Manifest::decode(&mk(&[(0, 1)], 3).encode())));
    // bad sha / truncation
    let mut b = ok.encode();
    b[10] ^= 1;
    assert!(is_corrupt(Manifest::decode(&b)));
    assert!(is_corrupt(Manifest::decode(&ok.encode()[..20])));
}

#[test]
fn head_json_round_trip_and_null_manifest() {
    let json = br#"{"format":1,"generation":3,"manifest":"manifest/0000000002","writer_id":"0123456789abcdef0123456789abcdef"}"#;
    let h = Head::decode(json).unwrap();
    assert_eq!(
        h,
        Head {
            journal_mode: JournalMode::Wal,
            format: 1,
            generation: 3,
            manifest: Some(2),
            writer_id: "0123456789abcdef0123456789abcdef".into(),
            epoch: 0,
            lease_ms: None,
            request_timeout_ms: None,
            next_generation_floor: None,
            database_id: None,
        }
    );
    // A head without `epoch` reads as 0 and is written back with it.
    let with_epoch = json.strip_suffix(b"}").unwrap().to_vec();
    assert_eq!(
        h.encode(),
        [with_epoch, br#","epoch":0}"#.to_vec()].concat()
    );

    let n = Head {
        journal_mode: JournalMode::Wal,
        format: 1,
        generation: 0,
        manifest: None,
        writer_id: "w".into(),
        epoch: 0,
        lease_ms: None,
        request_timeout_ms: None,
        next_generation_floor: None,
        database_id: None,
    };
    assert_eq!(
        String::from_utf8(n.encode()).unwrap(),
        r#"{"format":1,"generation":0,"manifest":null,"writer_id":"w","epoch":0}"#
    );
    assert_eq!(Head::decode(&n.encode()).unwrap(), n);

    // any key order, unknown keys ignored
    let h2 = Head::decode(
        br#" { "extra" : [ ] , "writer_id":"w","manifest":null,"generation":0,"format":1, "x":true } "#.as_slice(),
    );
    // arrays are not supported values: strict parser rejects
    assert!(is_corrupt(h2));
    let h3 = Head::decode(
        br#"{"writer_id":"w","x":true,"y":null,"z":"s","manifest":null,"generation":0,"format":1}"#,
    )
    .unwrap();
    assert_eq!(h3, n);

    assert!(is_corrupt(Head::decode(
        br#"{"format":2,"generation":0,"manifest":null,"writer_id":"w"}"#
    )));
}

#[test]
fn head_rejects_bad_json() {
    for bad in [
        r#"{"format":1,"format":1,"generation":0,"manifest":null,"writer_id":"w"}"#,
        r#"{"format":1,"generation":0,"manifest":"log/0000000002","writer_id":"w"}"#,
        r#"{"format":1,"generation":0,"manifest":"manifest/2","writer_id":"w"}"#,
        r#"{"format":1,"generation":-1,"manifest":null,"writer_id":"w"}"#,
        r#"{"format":1,"generation":0,"manifest":null}"#,
        r#"{"format":1,"generation":0,"manifest":null,"writer_id":"w"} x"#,
        "{\"format\":1,\"generation\":0,\"manifest\":null,\"writer_id\":\"w\n\"}",
        r#"{"format":1,"generation":0,"manifest":null,"writer_id":"w""#,
        r#"{"format":1,"generation":99999999999999999999999,"manifest":null,"writer_id":"w"}"#,
        r#"{"format":true,"generation":0,"manifest":null,"writer_id":"w"}"#,
        "",
        "[]",
    ] {
        assert!(is_corrupt(Head::decode(bad.as_bytes())), "{bad}");
    }
    let ok = Head::decode(br#"{"format":1,"generation":0,"manifest":null,"writer_id":"a\"b\\c"}"#)
        .unwrap();
    assert_eq!(ok.writer_id, "a\"b\\c");
    assert_eq!(Head::decode(&ok.encode()).unwrap(), ok);
}

#[test]
fn local_state_atomic_write() {
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(LocalState::read(dir.path()).unwrap(), None);
    let s = LocalState {
        format: 1,
        writer_id: "w1".into(),
        generation: 3,
        complete: true,
        claimed: false,
        head_etag: "\"e1\"".into(),
        epoch: 0,
        claim_id: String::new(),
        seal_not_before_ms: 0,
        seal_quiet_ms: 0,
        next_generation_floor: 0,
    };
    s.write_atomic(dir.path()).unwrap();
    assert_eq!(LocalState::read(dir.path()).unwrap(), Some(s.clone()));
    assert_eq!(
        String::from_utf8(s.encode()).unwrap(),
        r#"{"format":1,"writer_id":"w1","generation":3,"complete":true,"claimed":false,"head_etag":"\"e1\"","epoch":0,"claim_id":"","seal_not_before_ms":0,"seal_quiet_ms":0}"#
    );
    // A file written before `claimed` and `head_etag` existed: claimed, no ETag.
    let old =
        LocalState::decode(br#"{"format":1,"writer_id":"w1","generation":3,"complete":true}"#)
            .unwrap();
    assert!(old.claimed);
    assert_eq!(old.head_etag, "");
    let s2 = LocalState {
        complete: false,
        claimed: true,
        generation: 4,
        ..s
    };
    s2.write_atomic(dir.path()).unwrap();
    assert_eq!(LocalState::read(dir.path()).unwrap(), Some(s2));
    let names: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(names, vec!["app.db-durable".to_string()]);
    std::fs::write(dir.path().join("app.db-durable"), b"garbage").unwrap();
    assert!(is_corrupt(LocalState::read(dir.path())));
}

#[test]
fn local_state_refuses_absurd_seal_quiet() {
    let with = |ms: u64| LocalState {
        format: 1,
        writer_id: "w1".into(),
        generation: 3,
        complete: true,
        claimed: true,
        head_etag: String::new(),
        epoch: 0,
        claim_id: String::new(),
        seal_not_before_ms: 1,
        seal_quiet_ms: ms,
        next_generation_floor: 0,
    };
    assert!(is_corrupt(LocalState::decode(&with(u64::MAX).encode())));
    assert!(is_corrupt(LocalState::decode(&with(86_400_001).encode())));
    assert!(LocalState::decode(&with(86_400_000).encode()).is_ok());
}

/// Append the sha256 of `body` (computed here, independent of the codecs).
fn seal(mut body: Vec<u8>) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    let sha = Sha256::digest(&body);
    body.extend_from_slice(&sha);
    body
}

#[test]
fn golden_extents_record() {
    let body = hex::decode(concat!(
        "54444c4f47000001", // magic
        "01",               // kind extents
        "02",
        "7731",             // writer id "w1"
        "01000000",         // sync_count 1
        "01000000",         // op_count 1
        "01",               // write tag
        "0010000000000000", // off 0x1000
        "03000000",         // len 3
        "0a0b0c",
    ))
    .unwrap();
    let r = LogRecord::Extents {
        file: LogFile::Wal,
        writer_id: "w1".into(),
        syncs: vec![vec![Op::Write {
            off: 0x1000,
            bytes: vec![10, 11, 12],
        }]],
        next_generation: None,
    };
    assert_eq!(encode_record(&r), seal(body.clone()));
    assert_eq!(decode_record(&seal(body)).unwrap(), r);
}

/// A seal-start record of formats 1 to 4: kind 1 with no sync, byte for byte as before.
#[test]
fn golden_seal_start_without_next_generation() {
    let body = hex::decode(concat!(
        "54444c4f47000001", // magic
        "01",               // kind extents
        "02",
        "7731",     // writer id "w1"
        "00000000", // sync_count 0
    ))
    .unwrap();
    let r = LogRecord::Extents {
        file: LogFile::Wal,
        writer_id: "w1".into(),
        syncs: vec![],
        next_generation: None,
    };
    assert_eq!(encode_record(&r), seal(body.clone()));
    assert_eq!(decode_record(&seal(body)).unwrap(), r);
}

/// A format 5 seal-start record (kind 4) names the generation the switch moves the head to.
#[test]
fn golden_seal_start_with_next_generation() {
    let body = hex::decode(concat!(
        "54444c4f47000001", // magic
        "04",               // kind seal-start
        "02",
        "7731",             // writer id "w1"
        "00000000",         // sync_count 0
        "0c00000000000000", // next generation 12
    ))
    .unwrap();
    let r = LogRecord::Extents {
        file: LogFile::Wal,
        writer_id: "w1".into(),
        syncs: vec![],
        next_generation: Some(12),
    };
    assert_eq!(encode_record(&r), seal(body.clone()));
    assert_eq!(decode_record(&seal(body.clone())).unwrap(), r);
    // A kind 4 record with a sync, or cut short, is not a seal-start record.
    let mut with_sync = body.clone();
    with_sync[12] = 1;
    assert!(is_corrupt(decode_record(&seal(with_sync))));
    assert!(is_corrupt(decode_record(&seal(
        body[..body.len() - 1].to_vec()
    ))));
}

#[test]
fn golden_truncate_op() {
    let body = hex::decode(concat!(
        "54444c4f47000001",
        "01",
        "01",
        "77",
        "01000000",
        "01000000",
        "02",
        "0500000000000000",
    ))
    .unwrap();
    let r = LogRecord::Extents {
        file: LogFile::Wal,
        writer_id: "w".into(),
        syncs: vec![vec![Op::Truncate { len: 5 }]],
        next_generation: None,
    };
    assert_eq!(encode_record(&r), seal(body));
}

#[test]
fn golden_manifest() {
    let body = hex::decode(concat!(
        "54444d414e000001", // magic
        "00100000",         // page_size 4096
        "03000000",         // page_count 3
        "01000000",         // run_count 1
        "01000000",         // first 1
        "03000000",         // count 3
        "0200000000000000", // generation 2
    ))
    .unwrap();
    let m = Manifest {
        page_size: 4096,
        page_count: 3,
        pages: [(1, 2), (2, 2), (3, 2)].into(),
    };
    assert_eq!(m.encode(), seal(body.clone()));
    assert_eq!(Manifest::decode(&seal(body)).unwrap(), m);
}

fn manifest_body(page_count: u32, run_count: u32, runs: &[(u32, u32, u64)]) -> Vec<u8> {
    let mut b = b"TDMAN\0\0\x01".to_vec();
    b.extend(4096u32.to_le_bytes());
    b.extend(page_count.to_le_bytes());
    b.extend(run_count.to_le_bytes());
    for (f, c, g) in runs {
        b.extend(f.to_le_bytes());
        b.extend(c.to_le_bytes());
        b.extend(g.to_le_bytes());
    }
    b
}

#[test]
fn manifest_rejects_structural_lies_with_valid_sha() {
    let ok = manifest_body(10, 2, &[(1, 2, 1), (5, 2, 2)]);
    assert!(Manifest::decode(&seal(ok)).is_ok());
    // unsorted
    assert!(is_corrupt(Manifest::decode(&seal(manifest_body(
        10,
        2,
        &[(5, 2, 1), (1, 2, 2)]
    )))));
    // overlapping
    assert!(is_corrupt(Manifest::decode(&seal(manifest_body(
        10,
        2,
        &[(1, 3, 1), (3, 2, 2)]
    )))));
    // zero count
    assert!(is_corrupt(Manifest::decode(&seal(manifest_body(
        10,
        1,
        &[(1, 0, 1)]
    )))));
    // run_count larger / smaller than the table
    assert!(is_corrupt(Manifest::decode(&seal(manifest_body(
        10,
        3,
        &[(1, 2, 1)]
    )))));
    assert!(is_corrupt(Manifest::decode(&seal(manifest_body(
        10,
        1,
        &[(1, 2, 1), (5, 2, 2)]
    )))));
}

#[test]
fn record_rejects_lying_lengths_with_valid_sha() {
    let pre = hex::decode("54444c4f47000001010177").unwrap(); // magic, kind 1, id "w"
    let tail = |parts: &[&str]| {
        let mut b = pre.clone();
        for p in parts {
            b.extend(hex::decode(p).unwrap());
        }
        seal(b)
    };
    // sync_count lies (3, one present)
    assert!(is_corrupt(decode_record(&tail(&["03000000", "00000000"]))));
    // op_count lies
    assert!(is_corrupt(decode_record(&tail(&["01000000", "ffffffff"]))));
    // write len lies
    assert!(is_corrupt(decode_record(&tail(&[
        "01000000",
        "01000000",
        "01",
        "0000000000000000",
        "ffffffff",
        "00"
    ]))));
    // sanity: the honest version decodes
    assert!(decode_record(&tail(&["01000000", "00000000"])).is_ok());
}

#[test]
fn apply_ops_rejects_overflow() {
    let mut buf = Vec::new();
    let r = apply_ops(
        &mut buf,
        &[Op::Write {
            off: u64::MAX - 1,
            bytes: vec![1, 2, 3],
        }],
    );
    assert!(is_corrupt(r));
}

#[test]
fn json_escapes_decode_and_encode() {
    let h = Head::decode(
        br#"{"format":1,"generation":0,"manifest":null,"writer_id":"a\/\b\f\n\r\t\u0041\u00e9\ud83d\ude00"}"#,
    )
    .unwrap();
    assert_eq!(h.writer_id, "a/\u{8}\u{c}\n\r\tA\u{e9}\u{1F600}");
    for bad in [
        r#""\ud83d""#,
        r#""\ud83dx""#,
        r#""\ude00""#,
        r#""\u12""#,
        r#""\uzzzz""#,
    ] {
        let j = format!(r#"{{"format":1,"generation":0,"manifest":null,"writer_id":{bad}}}"#);
        assert!(is_corrupt(Head::decode(j.as_bytes())), "{bad}");
    }
    let nasty = Head {
        journal_mode: JournalMode::Wal,
        format: 1,
        generation: 0,
        manifest: None,
        writer_id: "q\"\\\u{0}\u{1f}\n\t\u{7f}é😀".into(),
        epoch: 0,
        lease_ms: None,
        request_timeout_ms: None,
        next_generation_floor: None,
        database_id: None,
    };
    let enc = nasty.encode();
    assert!(enc.iter().all(|b| *b >= 0x20));
    assert_eq!(Head::decode(&enc).unwrap(), nasty);
}

#[test]
fn head_encodes_epoch_last() {
    let h = Head {
        journal_mode: JournalMode::Wal,
        format: 1,
        generation: 3,
        manifest: Some(2),
        writer_id: "ab".into(),
        epoch: 7,
        lease_ms: None,
        request_timeout_ms: None,
        next_generation_floor: None,
        database_id: None,
    };
    let want =
        br#"{"format":1,"generation":3,"manifest":"manifest/0000000002","writer_id":"ab","epoch":7}"#;
    assert_eq!(h.encode(), want.to_vec());
    assert_eq!(Head::decode(want).unwrap(), h);
}

#[test]
fn stage2_head_reads_as_epoch_0() {
    let h = Head::decode(
        br#"{"format":1,"generation":3,"manifest":"manifest/0000000002","writer_id":"ab"}"#,
    )
    .unwrap();
    assert_eq!(h.epoch, 0);
}

#[test]
fn fence_record_golden_bytes() {
    let mut body = b"TDLOG\0\0\x01".to_vec();
    body.push(2);
    body.extend([2, b'a', b'b']);
    body.extend(0u32.to_le_bytes());
    body.extend(5u64.to_le_bytes());
    let r = LogRecord::Fence {
        writer_id: "ab".into(),
        epoch: 5,
    };
    assert_eq!(encode_record(&r), seal(body));
    assert_eq!(decode_record(&encode_record(&r)).unwrap(), r);
}

fn fence_body(syncs: u32, epoch: Option<u64>, trailing: &[u8]) -> Vec<u8> {
    let mut body = b"TDLOG\0\0\x01".to_vec();
    body.extend([2, 2, b'a', b'b']);
    body.extend(syncs.to_le_bytes());
    if let Some(e) = epoch {
        body.extend(e.to_le_bytes());
    }
    body.extend(trailing);
    seal(body)
}

#[test]
fn fence_record_with_syncs_is_corrupt() {
    assert!(is_corrupt(decode_record(&fence_body(1, Some(5), &[]))));
}

#[test]
fn fence_record_without_epoch_is_corrupt() {
    assert!(is_corrupt(decode_record(&fence_body(0, None, &[]))));
}

#[test]
fn fence_record_with_trailing_bytes_is_corrupt() {
    assert!(is_corrupt(decode_record(&fence_body(
        0,
        Some(5),
        &[0xde, 0xad]
    ))));
}

#[test]
fn stage2_local_state_reads_with_defaults() {
    let s = LocalState::decode(
        br#"{"format":1,"writer_id":"w","generation":3,"complete":true,"claimed":true,"head_etag":"e"}"#,
    )
    .unwrap();
    assert_eq!(
        (s.epoch, s.claim_id.as_str(), s.seal_not_before_ms),
        (0, "", 0)
    );
    let full = LocalState {
        format: 1,
        writer_id: "w".into(),
        generation: 3,
        complete: true,
        claimed: true,
        head_etag: "e".into(),
        epoch: 4,
        claim_id: "c1".into(),
        seal_not_before_ms: 99,
        seal_quiet_ms: 800,
        next_generation_floor: 0,
    };
    assert_eq!(
        String::from_utf8(full.encode()).unwrap(),
        r#"{"format":1,"writer_id":"w","generation":3,"complete":true,"claimed":true,"head_etag":"e","epoch":4,"claim_id":"c1","seal_not_before_ms":99,"seal_quiet_ms":800}"#
    );
    assert_eq!(LocalState::decode(&full.encode()).unwrap(), full);
    // A file written before `seal_quiet_ms` existed: a pending quiet of no length (0), which
    // the reopen reads as its own `lease + request_timeout`.
    let before = LocalState::decode(
        br#"{"format":1,"writer_id":"w","generation":3,"complete":true,"claimed":true,"head_etag":"e","epoch":4,"claim_id":"c1","seal_not_before_ms":99}"#,
    )
    .unwrap();
    assert_eq!((before.seal_not_before_ms, before.seal_quiet_ms), (99, 0));
}

// ---- Task 1: journal mode, format v2 and record kind 3 ----

#[test]
fn v1_head_bytes_are_unchanged() {
    let h = Head {
        format: FORMAT,
        generation: 3,
        manifest: Some(2),
        writer_id: "w1".into(),
        epoch: 5,
        journal_mode: JournalMode::Wal,
        lease_ms: None,
        request_timeout_ms: None,
        next_generation_floor: None,
        database_id: None,
    };
    // Golden: exactly today's encoding, no journal_mode key.
    assert_eq!(
        String::from_utf8(h.encode()).unwrap(),
        r#"{"format":1,"generation":3,"manifest":"manifest/0000000002","writer_id":"w1","epoch":5}"#
    );
    assert_eq!(Head::decode(&h.encode()).unwrap(), h);
    // A v1 head decodes as a WAL head.
    assert_eq!(
        Head::decode(&h.encode()).unwrap().journal_mode,
        JournalMode::Wal
    );
}

#[test]
fn v2_head_roundtrip_has_journal_mode_mvcc() {
    let h = Head {
        format: FORMAT_V2,
        generation: 1,
        manifest: Some(1),
        writer_id: "w1".into(),
        epoch: 1,
        journal_mode: JournalMode::Mvcc,
        lease_ms: None,
        request_timeout_ms: None,
        next_generation_floor: None,
        database_id: None,
    };
    assert_eq!(
        String::from_utf8(h.encode()).unwrap(),
        r#"{"format":2,"generation":1,"manifest":"manifest/0000000001","writer_id":"w1","epoch":1,"journal_mode":"mvcc"}"#
    );
    assert_eq!(Head::decode(&h.encode()).unwrap(), h);
    // A format 2 head without the key, or with an unknown mode, is corrupt.
    assert!(is_corrupt(Head::decode(
        br#"{"format":2,"generation":1,"manifest":null,"writer_id":"w1","epoch":1}"#
    )));
    assert!(is_corrupt(Head::decode(
        br#"{"format":2,"generation":1,"manifest":null,"writer_id":"w1","epoch":1,"journal_mode":"x"}"#
    )));
    // Format 5 is unknown.
    assert!(is_corrupt(Head::decode(
        br#"{"format":5,"generation":1,"manifest":null,"writer_id":"w1","journal_mode":"mvcc"}"#
    )));
}

#[test]
fn head_mode_must_match_its_format() {
    // Format 2 means MVCC: a "wal" mode is corrupt. A format 1 head carries no mode key.
    assert!(is_corrupt(Head::decode(
        br#"{"format":2,"generation":1,"manifest":null,"writer_id":"w1","epoch":1,"journal_mode":"wal"}"#
    )));
    assert!(is_corrupt(Head::decode(
        br#"{"format":1,"generation":1,"manifest":null,"writer_id":"w1","epoch":1,"journal_mode":"mvcc"}"#
    )));
}

#[test]
fn v1_decoder_style_refusal() {
    let h = Head {
        format: FORMAT_V2,
        generation: 1,
        manifest: None,
        writer_id: "w1".into(),
        epoch: 1,
        journal_mode: JournalMode::Mvcc,
        lease_ms: None,
        request_timeout_ms: None,
        next_generation_floor: None,
        database_id: None,
    };
    assert_eq!(
        Head::decode_accepting(&h.encode(), &[FORMAT]),
        Err(Error::Corrupt("head: unsupported format 2".into()))
    );
    assert_eq!(
        Head::decode_accepting(&h.encode(), &[FORMAT, FORMAT_V2]),
        Ok(h)
    );
}

#[test]
fn kind3_record_roundtrip() {
    let r = LogRecord::Extents {
        file: LogFile::Log,
        writer_id: "w1".into(),
        syncs: vec![vec![Op::Write {
            off: 0x1000,
            bytes: vec![10, 11, 12],
        }]],
        next_generation: None,
    };
    // Kind 3 body = kind 1 body with the kind byte 3.
    let body = hex::decode(concat!(
        "54444c4f47000001",
        "03",
        "02",
        "7731",
        "01000000",
        "01000000",
        "01",
        "0010000000000000",
        "03000000",
        "0a0b0c",
    ))
    .unwrap();
    assert_eq!(encode_record(&r), seal(body.clone()));
    assert_eq!(decode_record(&seal(body)).unwrap(), r);
    // The same payload as a WAL record differs only in the kind byte (and checksum).
    let wal = LogRecord::Extents {
        file: LogFile::Wal,
        writer_id: "w1".into(),
        syncs: match &r {
            LogRecord::Extents { syncs, .. } => syncs.clone(),
            _ => unreachable!(),
        },
        next_generation: None,
    };
    let (a, b) = (encode_record(&wal), encode_record(&r));
    assert_eq!(a[8], 1);
    assert_eq!(b[8], 3);
    assert_eq!(a[9..a.len() - 32], b[9..b.len() - 32]);
    assert_eq!(decode_record(&a).unwrap(), wal);
}

#[test]
fn kind3_checksum_mismatch_is_corrupt() {
    let r = LogRecord::Extents {
        file: LogFile::Log,
        writer_id: "w1".into(),
        syncs: vec![vec![Op::Truncate { len: 5 }]],
        next_generation: None,
    };
    let mut b = encode_record(&r);
    assert_eq!(decode_record(&b).unwrap(), r);
    // Flip the kind byte from 3 to 1: the checksum no longer matches.
    b[8] = 1;
    assert!(is_corrupt(decode_record(&b)));
    let mut b = encode_record(&r);
    let n = b.len();
    b[n - 1] ^= 1;
    assert!(is_corrupt(decode_record(&b)));
}

#[test]
fn local_state_v2_roundtrip() {
    let s = LocalState {
        format: FORMAT_V2,
        writer_id: "w1".into(),
        generation: 3,
        complete: true,
        claimed: true,
        head_etag: String::new(),
        epoch: 2,
        claim_id: String::new(),
        seal_not_before_ms: 0,
        seal_quiet_ms: 0,
        next_generation_floor: 0,
    };
    assert_eq!(LocalState::decode(&s.encode()).unwrap(), s);
    assert!(String::from_utf8(s.encode())
        .unwrap()
        .starts_with(r#"{"format":2,"#));
    let dir = tempfile::tempdir().unwrap();
    s.write_atomic(dir.path()).unwrap();
    assert_eq!(LocalState::read(dir.path()).unwrap(), Some(s));
    assert!(is_corrupt(LocalState::decode(
        br#"{"format":6,"writer_id":"w1","generation":3,"complete":true}"#
    )));
}

fn head_v1(lease: Option<u64>, rt: Option<u64>, floor: Option<u64>) -> Head {
    Head {
        format: FORMAT,
        generation: 3,
        manifest: Some(2),
        writer_id: "ab".into(),
        epoch: 7,
        journal_mode: JournalMode::Wal,
        lease_ms: lease,
        request_timeout_ms: rt,
        next_generation_floor: floor,
        database_id: None,
    }
}

#[test]
fn head_round_trips_lease_timeout_and_floor() {
    let h = head_v1(Some(30_000), Some(5_000), Some(9));
    assert_eq!(
        String::from_utf8(h.encode()).unwrap(),
        r#"{"format":1,"generation":3,"manifest":"manifest/0000000002","writer_id":"ab","epoch":7,"lease_ms":30000,"request_timeout_ms":5000,"next_generation_floor":9}"#
    );
    assert_eq!(Head::decode(&h.encode()).unwrap(), h);
    let mut v2 = h.clone();
    v2.format = FORMAT_V2;
    v2.journal_mode = JournalMode::Mvcc;
    assert_eq!(Head::decode(&v2.encode()).unwrap(), v2);
}

#[test]
fn head_without_the_new_fields_decodes_to_none_and_keeps_main_bytes() {
    let main = br#"{"format":1,"generation":3,"manifest":"manifest/0000000002","writer_id":"ab","epoch":7}"#;
    let h = Head::decode(main).unwrap();
    assert_eq!(h, head_v1(None, None, None));
    assert_eq!(h.encode(), main.to_vec());
    let main2 = br#"{"format":2,"generation":1,"manifest":null,"writer_id":"w1","epoch":1,"journal_mode":"mvcc"}"#;
    assert_eq!(Head::decode(main2).unwrap().encode(), main2.to_vec());
}

#[test]
fn head_with_a_huge_lease_is_corrupt() {
    let h = head_v1(Some(u64::MAX / 2), Some(5_000), None);
    assert!(is_corrupt(Head::decode(&h.encode())));
    // Each alone is under a day, the sum is over.
    let day = crate::config::defaults::MAX_SEAL_QUIET.as_millis() as u64;
    assert!(is_corrupt(Head::decode(
        &head_v1(Some(day), Some(1), None).encode()
    )));
    assert!(Head::decode(&head_v1(Some(day - 1), Some(1), None).encode()).is_ok());
}

#[test]
fn format_3_head_is_accepted_by_decode_only_for_a_v3_decoder() {
    for mode in [JournalMode::Wal, JournalMode::Mvcc] {
        let h = Head {
            format: FORMAT_V3,
            journal_mode: mode,
            ..head_v1(Some(1), Some(2), Some(5))
        };
        let enc = String::from_utf8(h.encode()).unwrap();
        assert!(
            enc.contains(&format!(",\"journal_mode\":\"{}\"", mode.as_str())),
            "{enc}"
        );
        assert_eq!(Head::decode(enc.as_bytes()).unwrap(), h);
        assert_eq!(
            Head::decode_accepting(enc.as_bytes(), &[FORMAT]),
            Err(Error::Corrupt("head: unsupported format 3".into()))
        );
        assert_eq!(
            Head::decode_accepting(enc.as_bytes(), &[FORMAT, FORMAT_V2]),
            Err(Error::Corrupt("head: unsupported format 3".into()))
        );
    }
    // A format 3 head without journal_mode is corrupt.
    assert!(is_corrupt(Head::decode(
        br#"{"format":3,"generation":1,"manifest":null,"writer_id":"w1","epoch":1}"#
    )));
}

#[test]
fn local_state_round_trips_the_floor_and_defaults_to_zero() {
    let old = br#"{"format":1,"writer_id":"w","generation":3,"complete":true}"#;
    assert_eq!(LocalState::decode(old).unwrap().next_generation_floor, 0);
    let s = LocalState {
        format: FORMAT,
        writer_id: "w".into(),
        generation: 3,
        complete: true,
        claimed: true,
        head_etag: "e".into(),
        epoch: 4,
        claim_id: "c1".into(),
        seal_not_before_ms: 0,
        seal_quiet_ms: 0,
        next_generation_floor: 12,
    };
    assert!(String::from_utf8(s.encode())
        .unwrap()
        .ends_with(r#","next_generation_floor":12}"#));
    assert_eq!(LocalState::decode(&s.encode()).unwrap(), s);
}

#[test]
fn previous_build_reads_heads_carrying_the_new_fields() {
    // A previous build accepts only its own formats and ignores keys it does not know.
    let v1 = br#"{"format":1,"generation":3,"manifest":"manifest/0000000002","writer_id":"ab","epoch":7,"lease_ms":30000,"request_timeout_ms":5000,"next_generation_floor":9}"#;
    let h = Head::decode_accepting(v1, &[FORMAT]).unwrap();
    assert_eq!(
        (
            h.format,
            h.generation,
            h.manifest,
            h.writer_id.as_str(),
            h.epoch,
            h.journal_mode
        ),
        (FORMAT, 3, Some(2), "ab", 7, JournalMode::Wal)
    );
    let v2 = br#"{"format":2,"generation":4,"manifest":null,"writer_id":"cd","epoch":8,"journal_mode":"mvcc","lease_ms":30000,"request_timeout_ms":5000,"next_generation_floor":9}"#;
    let h = Head::decode_accepting(v2, &[FORMAT, FORMAT_V2]).unwrap();
    assert_eq!(
        (
            h.format,
            h.generation,
            h.manifest,
            h.writer_id.as_str(),
            h.epoch,
            h.journal_mode
        ),
        (FORMAT_V2, 4, None, "cd", 8, JournalMode::Mvcc)
    );
    // And a decoder that does not know the keys (the pre-Task-2 shape) sees only the old ones.
    let o = super::json::Obj::parse(v1).unwrap();
    assert_eq!(o.num("generation").unwrap(), 3);
}

// Integrity codes (spec item 4).

mod integrity_codes {
    use super::super::integrity::{
        head_code, manifest_code, open_page, record_code, seal_page, CODE_LEN,
    };
    use super::*;
    use crate::encryption::{Encryption, IntegrityKey};
    use crate::format::{decode_record, encode_record, Op, FORMAT_V3, FORMAT_V4};
    use crate::telemetry::testing::Capture;
    use turso_core::{CipherMode, EncryptionKey};

    fn ik(seed: u8) -> IntegrityKey {
        Encryption::new(CipherMode::Aegis256, EncryptionKey::new_256([seed; 32])).integrity_key()
    }

    fn coded(seed: u8) -> Keys {
        Keys::new("p").with_integrity(Some(ik(seed)))
    }

    fn extents() -> LogRecord {
        LogRecord::Extents {
            file: LogFile::Wal,
            writer_id: "w".into(),
            syncs: vec![vec![Op::Write {
                off: 0,
                bytes: vec![1, 2, 3],
            }]],
            next_generation: None,
        }
    }

    fn manifest() -> Manifest {
        Manifest {
            page_size: 4096,
            page_count: 3,
            pages: BTreeMap::from([(1, 1), (2, 1), (3, 2)]),
        }
    }

    fn head3(mode: JournalMode) -> Head {
        Head {
            format: FORMAT_V3,
            generation: 7,
            manifest: Some(6),
            writer_id: "w1".into(),
            epoch: 3,
            journal_mode: mode,
            lease_ms: Some(30_000),
            request_timeout_ms: Some(10_000),
            next_generation_floor: Some(7),
            database_id: None,
        }
    }

    /// The message of an integrity failure for `key`, and nothing else.
    fn assert_failed<T: std::fmt::Debug>(r: crate::Result<T>, key: &str) {
        assert_eq!(r.unwrap_err(), Error::Corrupt(format!("integrity: {key}")));
    }

    #[test]
    fn record_code_replaces_the_checksum_and_binds_generation_and_seq() {
        let k = coded(1);
        let plain = encode_record(&extents());
        let body = k.encode_record(&extents(), 4, 9);
        assert_eq!(body.len(), plain.len(), "a code is as long as the checksum");
        assert_eq!(body[..body.len() - 32], plain[..plain.len() - 32]);
        assert_ne!(body[body.len() - 32..], plain[plain.len() - 32..]);
        assert_eq!(k.decode_record(&body, 4, 9).unwrap(), extents());
        // Another seq or generation, and another key.
        assert_failed(k.decode_record(&body, 4, 10), &k.log(4, 10));
        assert_failed(k.decode_record(&body, 5, 9), &k.log(5, 9));
        assert_failed(coded(2).decode_record(&body, 4, 9), &k.log(4, 9));
        // The plain checksum is not a code, and a code is not a checksum.
        assert_failed(k.decode_record(&plain, 4, 9), &k.log(4, 9));
        assert!(is_corrupt(decode_record(&body)));
        // Every kind: a fence and a kind 3 record.
        let fence = LogRecord::Fence {
            writer_id: "w".into(),
            epoch: 2,
        };
        let log3 = LogRecord::Extents {
            file: LogFile::Log,
            writer_id: "w".into(),
            syncs: vec![vec![Op::Truncate { len: 5 }]],
            next_generation: None,
        };
        let start = LogRecord::Extents {
            file: LogFile::Wal,
            writer_id: "w".into(),
            syncs: vec![],
            next_generation: Some(9),
        };
        // The next generation is covered by the code: an edited one fails.
        let b = k.encode_record(&start, 1, 2);
        let mut next = b.clone();
        next[b.len() - 32 - 8] ^= 1;
        assert_failed(k.decode_record(&next, 1, 2), &k.log(1, 2));
        for rec in [fence, log3, start] {
            let b = k.encode_record(&rec, 1, 2);
            assert_eq!(k.decode_record(&b, 1, 2).unwrap(), rec);
            assert_failed(k.decode_record(&b, 1, 3), &k.log(1, 3));
        }
        // A record shorter than a code.
        for len in [0, 8, 31] {
            assert_failed(k.decode_record(&body[..len], 4, 9), &k.log(4, 9));
        }
        // An edited byte.
        let mut edited = body.clone();
        edited[12] ^= 1;
        assert_failed(k.decode_record(&edited, 4, 9), &k.log(4, 9));
    }

    #[test]
    fn manifest_code_binds_the_generation() {
        let k = coded(1);
        let body = k.encode_manifest(&manifest(), 5);
        assert_eq!(k.decode_manifest(&body, 5).unwrap(), manifest());
        assert_failed(k.decode_manifest(&body, 6), &k.manifest(6));
        assert_failed(coded(2).decode_manifest(&body, 5), &k.manifest(5));
        let plain = manifest().encode();
        assert_eq!(body[..body.len() - 32], plain[..plain.len() - 32]);
        assert_failed(k.decode_manifest(&plain, 5), &k.manifest(5));
        assert!(is_corrupt(Manifest::decode(&body)));
        assert_failed(k.decode_manifest(&body[..31], 5), &k.manifest(5));
    }

    #[test]
    fn page_code_binds_page_number_and_generation() {
        let k = ik(1);
        let ct = vec![0xab; 4096];
        let obj = seal_page(&k, 7, 3, &ct);
        assert_eq!(obj.len(), ct.len() + CODE_LEN);
        assert_eq!(&obj[..ct.len()], &ct[..]);
        let key = "p/pages/0000000003/0000000007";
        assert_eq!(open_page(&k, 7, 3, &obj, key).unwrap(), &ct[..]);
        assert_failed(open_page(&k, 8, 3, &obj, key), key);
        assert_failed(open_page(&k, 7, 4, &obj, key), key);
        assert_failed(open_page(&ik(2), 7, 3, &obj, key), key);
        for len in [0, 1, 31] {
            assert_failed(open_page(&k, 7, 3, &obj[..len], key), key);
        }
        // Exactly a code: an empty page still has to verify.
        assert_failed(open_page(&k, 7, 3, &[0; 32], key), key);
        let mut edited = obj.clone();
        edited[100] ^= 1;
        assert_failed(open_page(&k, 7, 3, &edited, key), key);
    }

    #[test]
    fn code_kinds_are_domain_separated() {
        let k = ik(1);
        let bytes = b"same bytes";
        let codes = [
            record_code(&k, 1, 0, bytes),
            manifest_code(&k, 1, bytes),
            head_code(&k, bytes),
            k.code(&[b"page", &0u32.to_be_bytes(), &1u64.to_be_bytes(), bytes]),
        ];
        for (i, a) in codes.iter().enumerate() {
            for b in &codes[i + 1..] {
                assert_ne!(a, b);
            }
        }
    }

    #[test]
    fn coded_head_round_trips_and_every_field_is_covered() {
        let k = coded(1);
        for mode in [JournalMode::Wal, JournalMode::Mvcc] {
            let h = head3(mode);
            let body = k.encode_head(&h);
            let text = String::from_utf8(body.clone()).unwrap();
            assert!(text.starts_with("{\"format\":3,"), "{text}");
            assert!(text.contains(",\"integrity\":\""), "{text}");
            assert_eq!(k.decode_head(&body).unwrap(), h);
            // Deterministic: bytes compare equal for the same head.
            assert_eq!(k.encode_head(&h), body);
            // Each field edited: the code no longer verifies.
            for (from, to) in [
                ("\"generation\":7", "\"generation\":8"),
                ("\"manifest/0000000006\"", "\"manifest/0000000005\""),
                ("\"writer_id\":\"w1\"", "\"writer_id\":\"w2\""),
                ("\"epoch\":3", "\"epoch\":4"),
                ("\"lease_ms\":30000", "\"lease_ms\":30001"),
                ("\"request_timeout_ms\":10000", "\"request_timeout_ms\":1"),
                ("\"next_generation_floor\":7", "\"next_generation_floor\":6"),
                (
                    &format!("\"journal_mode\":\"{}\"", mode.as_str()),
                    &format!(
                        "\"journal_mode\":\"{}\"",
                        if mode == JournalMode::Wal {
                            "mvcc"
                        } else {
                            "wal"
                        }
                    ),
                ),
            ] {
                assert!(text.contains(from), "{from} not in {text}");
                assert_failed(k.decode_head(text.replace(from, to).as_bytes()), "p/head");
            }
            // The code removed, truncated, or from another key.
            let at = text.find(",\"integrity\"").unwrap();
            assert_failed(
                k.decode_head(format!("{}}}", &text[..at]).as_bytes()),
                "p/head",
            );
            assert_failed(coded(2).decode_head(&body), "p/head");
            assert_failed(
                k.decode_head(
                    text.replace("\"integrity\":\"", "\"integrity\":\"00")
                        .as_bytes(),
                ),
                "p/head",
            );
        }
    }

    /// `key_check`: 16 bytes under the key, in the coded fields; at open a different one is the
    /// wrong-key error before any code is looked at, and after a good open it is tampering.
    #[test]
    fn key_check_is_compared_at_open_before_the_integrity_code() {
        let k = coded(1);
        let other = coded(2);
        let h = head3(JournalMode::Mvcc);
        let body = k.encode_head(&h);
        let text = String::from_utf8(body.clone()).unwrap();
        let kc = hex::encode(super::super::integrity::key_check(&ik(1)));
        assert_eq!(kc.len(), 32);
        assert!(
            text.contains(&format!(",\"key_check\":\"{kc}\",\"integrity\":")),
            "{text}"
        );
        assert_ne!(kc, hex::encode(super::super::integrity::key_check(&ik(2))));
        assert_eq!(k.decode_head_open(&body).unwrap(), h);
        let wrong = Error::Encryption(
            "the key in BUCKWAL_ENCRYPTION_KEY_FILE is not the key this bucket was written with"
                .into(),
        );
        let cap = Capture::new();
        let got = tracing::subscriber::with_default(cap.subscriber(), || {
            let flipped = text.replace(&kc, &format!("{}{}", "0", &kc[1..]));
            let stripped = text.replace(&format!(",\"key_check\":\"{kc}\""), "");
            [
                other.decode_head_open(&body),
                k.decode_head_open(flipped.as_bytes()),
                k.decode_head_open(stripped.as_bytes()),
            ]
        });
        for r in got {
            assert_eq!(r, Err(wrong.clone()));
        }
        assert!(cap.spans().events.is_empty(), "a wrong key logged a record");
        // Matching key_check, wrong code: tampering.
        let edited = text.replace("\"epoch\":3", "\"epoch\":4");
        assert_failed(k.decode_head_open(edited.as_bytes()), "p/head");
        // After a good open the same failures are tampering.
        assert_failed(other.decode_head(&body), "p/head");
        let flipped = text.replace(&kc, &format!("{}{}", "0", &kc[1..]));
        assert_failed(k.decode_head(flipped.as_bytes()), "p/head");
        // for_open keys read as an open does.
        assert_eq!(other.for_open().decode_head(&body), Err(wrong));
        // The error and a key_check never carry key material.
        assert!(!format!("{:?}", other.decode_head_open(&body)).contains(&kc));
    }

    #[test]
    fn coded_head_refused_by_older_decoders() {
        let body = coded(1).encode_head(&head3(JournalMode::Mvcc));
        for accepted in [&[FORMAT][..], &[FORMAT, FORMAT_V2][..]] {
            assert_eq!(
                Head::decode_accepting(&body, accepted),
                Err(Error::Corrupt("head: unsupported format 3".into()))
            );
        }
    }

    const ID_A: &str = "0123456789abcdef0123456789abcdef";
    const ID_B: &str = "fedcba9876543210fedcba9876543210";

    fn head4(mode: JournalMode, id: &str) -> Head {
        Head {
            format: FORMAT_V4,
            database_id: Some(id.into()),
            ..head3(mode)
        }
    }

    #[test]
    fn a_format_4_head_round_trips_with_its_database_id_and_is_coded_under_the_database_key() {
        let k = coded(1);
        for mode in [JournalMode::Wal, JournalMode::Mvcc] {
            let h = head4(mode, ID_A);
            let body = k.encode_head(&h);
            let text = String::from_utf8(body.clone()).unwrap();
            assert!(text.starts_with("{\"format\":4,"), "{text}");
            assert!(
                text.contains(&format!(",\"database_id\":\"{ID_A}\",")),
                "{text}"
            );
            assert_eq!(k.decode_head_open(&body).unwrap(), h);
            // The same fields under another id, or as a format 3 head, carry another code.
            let other = k.encode_head(&head4(mode, ID_B));
            assert_ne!(body, other);
            // Keys that have read no head take the other id (the first head decides).
            assert_eq!(
                coded(1)
                    .decode_head_open(&other)
                    .unwrap()
                    .database_id
                    .as_deref(),
                Some(ID_B)
            );
        }
        // Another key file is the wrong key, not tampering, even with a database id.
        let body = k.encode_head(&head4(JournalMode::Wal, ID_A));
        assert!(matches!(
            coded(2).decode_head_open(&body),
            Err(Error::Encryption(_))
        ));
    }

    #[test]
    fn a_format_4_head_with_an_edited_removed_or_malformed_database_id_fails_its_code() {
        let k = coded(1);
        let body = k.encode_head(&head4(JournalMode::Wal, ID_A));
        let text = String::from_utf8(body).unwrap();
        let removed = text.replace(&format!(",\"database_id\":\"{ID_A}\""), "");
        for (what, bad) in [
            ("edited", text.replace(ID_A, ID_B)),
            ("removed", removed),
            ("malformed", text.replace(ID_A, "not-hex")),
            ("short", text.replace(ID_A, &ID_A[..30])),
        ] {
            assert_ne!(bad, text, "{what}");
            assert_failed(k.decode_head_open(bad.as_bytes()), "p/head");
            assert_failed(k.decode_head(bad.as_bytes()), "p/head");
        }
    }

    #[test]
    fn a_database_id_is_allowed_only_in_a_format_4_head() {
        let body = coded(1).encode_head(&head3(JournalMode::Wal));
        let text = String::from_utf8(body).unwrap();
        let with = text.replace(
            ",\"journal_mode\"",
            &format!(",\"database_id\":\"{ID_A}\",\"journal_mode\""),
        );
        assert!(is_corrupt(Head::decode(with.as_bytes())), "{with}");
        let plain = br#"{"format":1,"generation":1,"manifest":null,"writer_id":"w1","database_id":"0123456789abcdef0123456789abcdef"}"#;
        assert!(is_corrupt(Head::decode(plain)));
    }

    #[test]
    fn older_decoders_refuse_a_format_4_head_and_a_keyless_open_names_the_key_file() {
        let body = coded(1).encode_head(&head4(JournalMode::Mvcc, ID_A));
        for accepted in [
            &[FORMAT][..],
            &[FORMAT, FORMAT_V2][..],
            &[FORMAT, FORMAT_V2, FORMAT_V3][..],
        ] {
            assert_eq!(
                Head::decode_accepting(&body, accepted),
                Err(Error::Corrupt("head: unsupported format 4".into()))
            );
        }
        match Keys::new("p").decode_head_open(&body) {
            Err(Error::Encryption(m)) => {
                assert!(
                    m.contains("format 4") && m.contains("BUCKWAL_ENCRYPTION_KEY_FILE"),
                    "{m}"
                )
            }
            r => panic!("{r:?}"),
        }
    }

    #[test]
    fn the_database_key_differs_per_id_and_from_the_integrity_key() {
        let parts: &[&[u8]] = &[b"record", b"x"];
        let a = ik(1).database_key(&[1; 16]);
        let b = ik(1).database_key(&[2; 16]);
        assert_eq!(a.code(parts), ik(1).database_key(&[1; 16]).code(parts));
        assert_ne!(a.code(parts), b.code(parts));
        assert_ne!(a.code(parts), ik(1).code(parts));
        assert_ne!(a.code(parts), ik(2).database_key(&[1; 16]).code(parts));
    }

    /// The keys learn the database's format from its first head; another id, or the other
    /// format, after that is tampering.
    #[test]
    fn keys_refuse_a_head_of_another_id_or_format_after_the_first() {
        let h3 = head3(JournalMode::Wal);
        let (a, b) = (head4(JournalMode::Wal, ID_A), head4(JournalMode::Wal, ID_B));
        let enc = |k: &Keys, h: &Head| k.encode_head(h);
        // Format 4, then another id, then format 3.
        let k = coded(1);
        k.decode_head_open(&enc(&k, &a)).unwrap();
        k.decode_head(&enc(&k, &a)).unwrap();
        assert_failed(k.decode_head(&enc(&k, &b)), "p/head");
        assert_eq!(
            k.decode_head(&enc(&k, &h3)).unwrap_err(),
            Error::Corrupt("integrity: p/head format downgraded".into())
        );
        // Format 3, then format 4.
        let k = coded(1);
        k.decode_head_open(&enc(&k, &h3)).unwrap();
        k.decode_head(&enc(&k, &h3)).unwrap();
        assert_failed(k.decode_head(&enc(&k, &a)), "p/head");
        // Clones share what was learned (they are the same database).
        let k = coded(1);
        let c = k.clone();
        k.decode_head_open(&enc(&k, &a)).unwrap();
        assert_failed(c.decode_head(&enc(&c, &b)), "p/head");
    }

    #[test]
    fn encrypted_open_refuses_heads_written_before_integrity_codes() {
        let k = coded(1);
        let v1 = head_v1(Some(1), Some(2), None);
        let v2 = Head {
            format: FORMAT_V2,
            journal_mode: JournalMode::Mvcc,
            ..v1.clone()
        };
        for h in [v1, v2] {
            // At open: a plaintext or pre-codes bucket. After a good open: a downgrade.
            assert_eq!(
                k.decode_head_open(&h.encode()),
                Err(Error::Encryption(
                    "the bucket has no integrity codes: it is plaintext, or was encrypted \
                     before this version"
                        .into()
                ))
            );
            assert_eq!(
                k.decode_head(&h.encode()),
                Err(Error::Corrupt("integrity: p/head format downgraded".into()))
            );
        }
    }

    #[test]
    fn plaintext_open_refuses_a_format_3_head_and_reads_the_others() {
        let plain = Keys::new("p");
        let body = coded(1).encode_head(&head3(JournalMode::Wal));
        match plain.decode_head(&body) {
            Err(Error::Encryption(m)) => {
                assert!(
                    m.contains("format 3") && m.contains("BUCKWAL_ENCRYPTION_KEY_FILE"),
                    "{m}"
                )
            }
            r => panic!("{r:?}"),
        }
        let v1 = head_v1(Some(1), Some(2), Some(5));
        assert_eq!(plain.decode_head(&v1.encode()).unwrap(), v1);
    }

    /// A previous build (format 1 and 2 only, unknown keys ignored) reads a plaintext head of a
    /// takeover's skip seal: generation G+2 naming manifest G+1, with the new fields.
    #[test]
    fn previous_build_follows_a_skipped_generation() {
        let h = Head {
            generation: 9,
            manifest: Some(8),
            next_generation_floor: Some(9),
            ..head_v1(Some(30_000), Some(10_000), Some(9))
        };
        let body = h.encode();
        let old = Head::decode_accepting(&body, &[FORMAT, FORMAT_V2]).unwrap();
        assert_eq!(
            (old.generation, old.manifest, old.format),
            (9, Some(8), FORMAT)
        );
    }

    // The plaintext bytes of main, written down: an encoded object of a database without
    // encryption is exactly these (the trailer is the SHA-256 of the rest).
    const GOLDEN_RECORD: &str = "54444c4f470000010101770100000001000000010000000000000000030000000102036a9d58532f48cff4220663817e761921820a6baa5b8280de12af672d16237973";
    const GOLDEN_FENCE: &str = "54444c4f4700000102017700000000070000000000000066df30f71bfc713f2075ae4cbe806f5fd7558a26bc65f144e8622781c5581e21";
    const GOLDEN_KIND3: &str = "54444c4f4700000103017701000000010000000205000000000000000f2636f4ee7d84fe5ea39b32e92b61376bf7ccaabc3c50c90e8b3303ed260a06";
    const GOLDEN_MANIFEST: &str = "54444d414e0000010010000003000000020000000100000002000000010000000000000003000000010000000200000000000000f7604622e0cc59f46527d3e3202baa1beaa28f24e77568d51da2b96289933cda";

    #[test]
    fn plaintext_objects_are_byte_identical_to_main() {
        let plain = Keys::new("p");
        let fence = LogRecord::Fence {
            writer_id: "w".into(),
            epoch: 7,
        };
        let log3 = LogRecord::Extents {
            file: LogFile::Log,
            writer_id: "w".into(),
            syncs: vec![vec![Op::Truncate { len: 5 }]],
            next_generation: None,
        };
        for (rec, golden) in [
            (extents(), GOLDEN_RECORD),
            (fence, GOLDEN_FENCE),
            (log3, GOLDEN_KIND3),
        ] {
            // Whatever the generation and seq: a plaintext record does not depend on them.
            assert_eq!(hex::encode(plain.encode_record(&rec, 3, 8)), golden);
            assert_eq!(hex::encode(encode_record(&rec)), golden);
            assert_eq!(
                plain
                    .decode_record(&hex::decode(golden).unwrap(), 1, 1)
                    .unwrap(),
                rec
            );
        }
        assert_eq!(
            hex::encode(plain.encode_manifest(&manifest(), 9)),
            GOLDEN_MANIFEST
        );
        assert_eq!(hex::encode(manifest().encode()), GOLDEN_MANIFEST);
        // A page object is the page.
        let page = vec![0x5a; 4096];
        assert_eq!(&*plain.seal_page(3, 2, &page), &page[..]);
        assert_eq!(plain.open_page(3, 2, &page).unwrap(), &page[..]);
        // The plaintext head has no code and keeps its format.
        let h = head_v1(Some(30_000), Some(10_000), None);
        let text = String::from_utf8(plain.encode_head(&h)).unwrap();
        assert!(!text.contains("integrity"), "{text}");
        assert!(text.starts_with("{\"format\":1,"), "{text}");
    }

    #[test]
    fn new_bucket_format_follows_encryption_and_mode() {
        let plain = Keys::new("p");
        assert_eq!(plain.new_format(JournalMode::Wal), FORMAT);
        assert_eq!(plain.new_format(JournalMode::Mvcc), FORMAT_V2);
        for mode in [JournalMode::Wal, JournalMode::Mvcc] {
            assert_eq!(coded(1).new_format(mode), FORMAT_V4);
        }
    }

    /// One ERROR record per failed check, with the key, kind, generation and seq, and nothing
    /// else of the object.
    #[test]
    fn a_failed_check_logs_one_error_record_without_data() {
        let k = coded(1);
        let obj = k.encode_record(&extents(), 4, 9);
        let page = seal_page(&ik(1), 7, 3, &[0xcd; 64]);
        let cap = Capture::new();
        tracing::subscriber::with_default(cap.subscriber(), || {
            let _ = k.decode_record(&obj, 4, 10);
            let _ = k.open_page(7, 4, &page);
            let _ = k.decode_manifest(&obj, 2);
            let _ = k.decode_head(b"{\"format\":3,\"generation\":5,\"manifest\":null,\"writer_id\":\"w\",\"epoch\":1,\"journal_mode\":\"wal\"}");
        });
        let events = cap.spans().events;
        let errs: Vec<_> = events
            .iter()
            .filter(|e| e.field("message") == Some("integrity check failed"))
            .collect();
        let got: Vec<_> = errs
            .iter()
            .map(|e| {
                (
                    e.level,
                    e.target.as_str(),
                    e.field("key").unwrap().to_string(),
                    e.field("kind").unwrap().to_string(),
                    e.field("generation").unwrap().to_string(),
                    e.field("seq").unwrap().to_string(),
                )
            })
            .collect();
        let s = |x: &str| x.to_string();
        assert_eq!(
            got,
            vec![
                (
                    tracing::Level::ERROR,
                    "buckwal",
                    k.log(4, 10),
                    s("record"),
                    s("4"),
                    s("10")
                ),
                (
                    tracing::Level::ERROR,
                    "buckwal",
                    k.page(4, 7),
                    s("page"),
                    s("4"),
                    s("0")
                ),
                (
                    tracing::Level::ERROR,
                    "buckwal",
                    k.manifest(2),
                    s("manifest"),
                    s("2"),
                    s("0")
                ),
                (
                    tracing::Level::ERROR,
                    "buckwal",
                    k.head(),
                    s("head"),
                    s("5"),
                    s("0")
                ),
            ]
        );
        for e in errs {
            let all = format!("{:?}", e.fields);
            assert!(!all.contains("cdcdcd") && !all.contains(&hex::encode(&obj[obj.len() - 32..])));
        }
    }
}

mod packer_format {
    use super::*;
    use crate::encryption::{Encryption, IntegrityKey};
    use crate::format::{FORMAT_V4, FORMAT_V5};
    use crate::pack::PackPointer;
    use turso_core::{CipherMode, EncryptionKey};

    const ID: &str = "0123456789abcdef0123456789abcdef";

    fn ik(seed: u8) -> IntegrityKey {
        Encryption::new(CipherMode::Aegis256, EncryptionKey::new_256([seed; 32])).integrity_key()
    }

    fn coded(seed: u8) -> Keys {
        Keys::new("p").with_integrity(Some(ik(seed)))
    }

    fn pointer(k: &Keys, id: Option<&str>) -> PackPointer {
        PackPointer {
            format: 5,
            pack_key: k.pack(15, "run1"),
            codes_key: k.pack_codes(15, "run1"),
            pack_generation: 15,
            size: 5_000_000,
            page_size: 4096,
            part_size: 16 << 20,
            block_size: 1 << 20,
            database_id: id.map(String::from),
        }
    }

    fn head5(id: Option<&str>) -> Head {
        Head {
            format: FORMAT_V5,
            generation: 16,
            manifest: None,
            writer_id: "w1".into(),
            epoch: 2,
            journal_mode: JournalMode::Mvcc,
            lease_ms: Some(30_000),
            request_timeout_ms: Some(10_000),
            next_generation_floor: Some(16),
            database_id: id.map(String::from),
        }
    }

    #[test]
    fn pack_keys_round_trip_and_stay_apart_from_the_other_keys() {
        let k = Keys::new("p");
        assert_eq!(k.pack(15, "run1"), "p/pack/0000000015/run1");
        assert_eq!(k.pack_codes(15, "run1"), "p/pack/0000000015/run1.codes");
        assert_eq!(k.pack_pointer(), "p/pack-latest");
        assert_eq!(k.pack_root(), "p/pack/");
        assert_eq!(
            k.parse_pack(&k.pack(15, "run1")),
            Some((15, "run1".to_string()))
        );
        assert_eq!(k.parse_pack(&k.pack_codes(15, "run1")), None);
        assert_eq!(k.parse_pack(&k.pack_pointer()), None);
        assert_eq!(k.parse_pack("p/pack/15/run1"), None);
        assert_eq!(k.parse_pack("q/pack/0000000015/run1"), None);
        assert_eq!(k.parse_pack(&k.log(15, 1)), None);
        assert_eq!(k.parse_log(&k.pack(15, "run1")), None);
        assert_eq!(Keys::new("").pack_pointer(), "pack-latest");
    }

    /// Spec rev 8: `pack-latest` holds the pack key and the codes key relative to the prefix,
    /// and the pointer's code binds those relative keys. So the same bytes decode at another
    /// prefix (a whole database copied there), to the keys under that prefix.
    #[test]
    fn a_pointer_stores_its_keys_relative_to_the_prefix() {
        for coded_bucket in [false, true] {
            let (k, p) = if coded_bucket {
                coded_with_id()
            } else {
                let k = Keys::new("p");
                let p = pointer(&k, None);
                (k, p)
            };
            let body = k.encode_pointer(&p);
            let text = String::from_utf8(body.clone()).unwrap();
            assert!(
                text.contains("\"pack_key\":\"pack/0000000015/run1\"")
                    && text.contains("\"codes_key\":\"pack/0000000015/run1.codes\""),
                "{text}"
            );
            assert!(!text.contains("p/pack"), "{text}");
            assert_eq!(k.decode_pointer(&body).unwrap(), p, "full keys in memory");
            let z = k.at_prefix("z/y");
            let q = z.decode_pointer(&body).unwrap();
            assert_eq!(
                (q.pack_key.as_str(), q.codes_key.as_str()),
                ("z/y/pack/0000000015/run1", "z/y/pack/0000000015/run1.codes")
            );
        }
    }

    #[test]
    fn a_plain_pointer_round_trips_without_a_code() {
        let k = Keys::new("p");
        let p = pointer(&k, None);
        let body = k.encode_pointer(&p);
        let text = String::from_utf8(body.clone()).unwrap();
        assert!(!text.contains("integrity"), "{text}");
        assert_eq!(k.decode_pointer(&body).unwrap(), p);
    }

    #[test]
    fn a_coded_pointer_round_trips_and_a_changed_field_fails_its_code() {
        let k = coded(1);
        k.init_database_id().unwrap();
        let id = k.database_id();
        let p = pointer(&k, id.as_deref());
        let body = k.encode_pointer(&p);
        assert!(String::from_utf8(body.clone())
            .unwrap()
            .contains("\"integrity\":\""));
        assert_eq!(k.decode_pointer(&body).unwrap(), p);
        let key = k.pack_pointer();
        let fail = Error::Corrupt(format!("integrity: {key}"));
        let edit = |from: &str, to: &str| {
            let text = String::from_utf8(body.clone()).unwrap();
            assert!(text.contains(from), "{text}");
            text.replace(from, to).into_bytes()
        };
        for changed in [
            edit("run1\"", "run2\""),
            edit("run1.codes", "run2.codes"),
            edit("\"size\":5000000", "\"size\":5000001"),
            edit("\"pack_generation\":15", "\"pack_generation\":16"),
        ] {
            assert_eq!(k.decode_pointer(&changed).unwrap_err(), fail);
        }
        // A pointer without a code, and one made under another key.
        let plain = Keys::new("p").encode_pointer(&p);
        assert_eq!(k.decode_pointer(&plain).unwrap_err(), fail);
        assert!(matches!(
            Keys::new("p").decode_pointer(&body),
            Err(Error::Encryption(_))
        ));
        let other = coded(2);
        other.init_database_id().unwrap();
        assert_eq!(other.decode_pointer(&body).unwrap_err(), fail);
    }

    fn coded_with_id() -> (Keys, PackPointer) {
        let k = coded(1);
        k.init_database_id().unwrap();
        let id = k.database_id();
        let p = pointer(&k, id.as_deref());
        (k, p)
    }

    #[test]
    fn the_pointer_code_covers_every_field() {
        let (k, p) = coded_with_id();
        let key = k.pack_pointer();
        let fail = Error::Corrupt(format!("integrity: {key}"));
        let body = String::from_utf8(k.encode_pointer(&p)).unwrap();
        for (from, to) in [
            ("\"page_size\":4096", "\"page_size\":8192"),
            ("\"part_size\":16777216", "\"part_size\":16777217"),
            ("\"block_size\":1048576", "\"block_size\":2097152"),
        ] {
            assert!(body.contains(from), "{body}");
            let edited = body.replace(from, to);
            assert_eq!(k.decode_pointer(edited.as_bytes()).unwrap_err(), fail);
        }
        // The code itself differs when only the database id differs.
        let code = |id: &str| {
            let q = PackPointer {
                database_id: Some(id.into()),
                ..p.clone()
            };
            crate::format::integrity::pointer_code(&ik(1).database_key(&[1; 16]), &q)
        };
        assert_ne!(code(ID), code("fedcba9876543210fedcba9876543210"));
    }

    #[test]
    fn a_pointer_is_bound_to_its_database_id() {
        // One base key, two databases: A's pointer does not verify as B's.
        let a = coded(1);
        a.init_database_id().unwrap();
        let b = coded(1);
        b.init_database_id().unwrap();
        assert_ne!(a.database_id(), b.database_id());
        let p = pointer(&a, a.database_id().as_deref());
        let body = a.encode_pointer(&p);
        let fail = Error::Corrupt(format!("integrity: {}", b.pack_pointer()));
        assert_eq!(b.decode_pointer(&body).unwrap_err(), fail);
        // B's keys with A's id in the pointer: the code is under A's key, so it fails too.
        let q = PackPointer {
            database_id: b.database_id(),
            ..p
        };
        assert_eq!(b.decode_pointer(&a.encode_pointer(&q)).unwrap_err(), fail);
    }

    #[test]
    fn a_pointer_with_another_database_id_than_the_keys_hold_is_refused() {
        let (k, p) = coded_with_id();
        let other = PackPointer {
            database_id: Some(ID.into()),
            ..p
        };
        assert_ne!(k.database_id().as_deref(), Some(ID));
        assert!(is_corrupt(k.decode_pointer(&k.encode_pointer(&other))));
    }

    #[test]
    fn a_coded_pointer_without_a_database_id_is_refused() {
        let (k, p) = coded_with_id();
        let none = PackPointer {
            database_id: None,
            ..p
        };
        assert!(is_corrupt(k.decode_pointer(&k.encode_pointer(&none))));
    }

    #[test]
    fn a_pointer_whose_codes_key_is_not_its_pack_key_plus_codes_is_refused() {
        let k = Keys::new("p");
        let p = PackPointer {
            codes_key: k.pack_codes(15, "run2"),
            ..pointer(&k, None)
        };
        assert!(is_corrupt(k.decode_pointer(&k.encode_pointer(&p))));
        let (k, p) = coded_with_id();
        let p = PackPointer {
            codes_key: k.pack_codes(15, "run2"),
            ..p
        };
        assert!(is_corrupt(k.decode_pointer(&k.encode_pointer(&p))));
    }

    #[test]
    fn a_pointer_whose_pack_key_does_not_parse_under_the_prefix_is_refused() {
        let k = Keys::new("p");
        for pack_key in [
            "q/pack/0000000015/run1",
            "p/pack/15/run1",
            "p/log/0000000015/run1",
        ] {
            let p = PackPointer {
                pack_key: pack_key.into(),
                codes_key: format!("{pack_key}.codes"),
                ..pointer(&k, None)
            };
            assert!(
                is_corrupt(k.decode_pointer(&k.encode_pointer(&p))),
                "{pack_key}"
            );
        }
    }

    #[test]
    fn a_pointer_whose_generation_is_not_its_pack_keys_is_refused() {
        let k = Keys::new("p");
        let p = PackPointer {
            pack_generation: 16,
            ..pointer(&k, None)
        };
        assert!(is_corrupt(k.decode_pointer(&k.encode_pointer(&p))));
    }

    #[test]
    fn a_run_id_with_dashes_and_underscores_round_trips_through_parse_pack() {
        let k = Keys::new("p");
        assert_eq!(
            k.parse_pack(&k.pack(7, "run-1_a")),
            Some((7, "run-1_a".to_string()))
        );
    }

    #[test]
    #[should_panic]
    #[cfg(debug_assertions)]
    fn a_run_id_outside_the_allowed_characters_panics_in_debug() {
        let _ = Keys::new("p").pack(1, "a/b");
    }

    #[test]
    #[should_panic]
    #[cfg(debug_assertions)]
    fn a_format_5_head_with_a_manifest_does_not_encode() {
        let h = Head {
            manifest: Some(1),
            ..head5(None)
        };
        let _ = h.encode();
    }

    #[test]
    fn a_bad_pointer_is_corrupt() {
        let k = Keys::new("p");
        for body in [
            &b"{}"[..],
            br#"{"format":4}"#,
            br#"{"format":5,"pack_key":"a","codes_key":"b","pack_generation":1,"size":1,"page_size":0,"part_size":1,"block_size":1}"#,
            b"not json",
        ] {
            assert!(is_corrupt(k.decode_pointer(body)), "{body:?}");
        }
    }

    #[test]
    fn a_format_5_head_has_no_manifest_and_keeps_its_other_fields() {
        let k = Keys::new("p");
        let h = head5(None);
        let body = k.encode_head(&h);
        let text = String::from_utf8(body.clone()).unwrap();
        assert!(
            text.starts_with("{\"format\":5,\"generation\":16,\"writer_id\""),
            "{text}"
        );
        assert!(!text.contains("manifest"), "{text}");
        assert!(text.contains("\"journal_mode\":\"mvcc\""), "{text}");
        assert_eq!(k.decode_head(&body).unwrap(), h);
        // A manifest key in a format 5 head is corrupt.
        let with = text.replace("\"generation\":16", "\"generation\":16,\"manifest\":null");
        assert!(is_corrupt(k.decode_head(with.as_bytes())));
        // A format 5 head of a WAL bucket.
        let wal = Head {
            journal_mode: JournalMode::Wal,
            ..h
        };
        assert_eq!(k.decode_head(&k.encode_head(&wal)).unwrap(), wal);
    }

    #[test]
    fn a_coded_format_5_head_keeps_database_id_and_key_check() {
        let k = coded(1);
        let h = head5(Some(ID));
        let body = k.encode_head(&h);
        let text = String::from_utf8(body.clone()).unwrap();
        assert!(!text.contains("manifest"), "{text}");
        assert!(
            text.contains(&format!("\"database_id\":\"{ID}\"")),
            "{text}"
        );
        assert!(text.contains("\"key_check\":\""), "{text}");
        assert_eq!(k.decode_head_open(&body).unwrap(), h);
        assert_eq!(coded(1).decode_head(&body).unwrap(), h);
        // Another key is the wrong key; no key is `Encryption` too.
        assert!(matches!(
            coded(2).decode_head_open(&body),
            Err(Error::Encryption(_))
        ));
        assert!(matches!(
            Keys::new("p").decode_head(&body),
            Err(Error::Encryption(_))
        ));
        // An edited field, a removed database id and a stripped head fail the code.
        let edited = text.replace("\"epoch\":2", "\"epoch\":3");
        assert!(is_corrupt(k.decode_head(edited.as_bytes())));
        let removed = text.replace(&format!(",\"database_id\":\"{ID}\""), "");
        assert!(coded(1).decode_head_open(removed.as_bytes()).is_err());
        let plain = Keys::new("p").encode_head(&head5(None));
        assert!(matches!(
            coded(1).decode_head_open(&plain),
            Err(Error::Encryption(_))
        ));
        assert!(is_corrupt(k.decode_head(&plain)));
        // A format 5 head over a known format 4 database with the same id is accepted; with
        // another id it is tampering.
        let k4 = coded(1);
        let h4 = Head {
            format: FORMAT_V4,
            manifest: Some(15),
            ..head5(Some(ID))
        };
        k4.decode_head_open(&k4.encode_head(&h4)).unwrap();
        k4.decode_head(&body).unwrap();
        let other_id = head5(Some("fedcba9876543210fedcba9876543210"));
        assert!(is_corrupt(k4.decode_head(&coded(1).encode_head(&other_id))));
    }

    #[test]
    fn formats_1_to_4_still_decode_and_new_format_is_unchanged_until_the_writer_packs() {
        let k = Keys::new("p");
        let v1 = br#"{"format":1,"generation":3,"manifest":"manifest/0000000002","writer_id":"w"}"#;
        assert_eq!(k.decode_head(v1).unwrap().manifest, Some(2));
        let null = br#"{"format":1,"generation":3,"manifest":null,"writer_id":"w"}"#;
        assert_eq!(k.decode_head(null).unwrap().manifest, None);
        // A format 1 to 4 head without a manifest key is still refused.
        let none = br#"{"format":1,"generation":3,"writer_id":"w"}"#;
        assert!(is_corrupt(k.decode_head(none)));
        assert_eq!(k.new_format(JournalMode::Wal), 1);
        assert_eq!(k.new_format_packed(), 5);
        assert_eq!(coded(1).new_format_packed(), 5);
    }
}
