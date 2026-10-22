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
    };
    assert_eq!(encode_record(&r), seal(body.clone()));
    assert_eq!(decode_record(&seal(body)).unwrap(), r);
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
    // Format 3 is still unknown.
    assert!(is_corrupt(Head::decode(
        br#"{"format":3,"generation":1,"manifest":null,"writer_id":"w1","journal_mode":"mvcc"}"#
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
    };
    assert_eq!(LocalState::decode(&s.encode()).unwrap(), s);
    assert!(String::from_utf8(s.encode())
        .unwrap()
        .starts_with(r#"{"format":2,"#));
    let dir = tempfile::tempdir().unwrap();
    s.write_atomic(dir.path()).unwrap();
    assert_eq!(LocalState::read(dir.path()).unwrap(), Some(s));
    assert!(is_corrupt(LocalState::decode(
        br#"{"format":3,"writer_id":"w1","generation":3,"complete":true}"#
    )));
}
