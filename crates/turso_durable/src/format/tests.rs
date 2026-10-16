use super::*;
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
    assert_eq!(k.page(2, 9), "p/page/0000000002/0000000009");
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
        assert_eq!(k.parse_manifest(&k.head()), None);
        assert_eq!(k.parse_manifest(&format!("{}/extra", k.manifest(1))), None);
    }
    assert_eq!(Keys::new("p").parse_manifest("manifest/0000000002"), None);
}

fn sample_extents() -> LogRecord {
    LogRecord::Extents {
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
    );
    assert_eq!(buf.len(), 13);
    assert!(buf[..10].iter().all(|b| *b == 0));
    assert_eq!(&buf[10..], &[7, 8, 9]);
    apply_ops(&mut buf, &[Op::Truncate { len: 4 }]);
    assert_eq!(buf.len(), 4);
    apply_ops(&mut buf, &[Op::Truncate { len: 6 }]);
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
            format: 1,
            generation: 3,
            manifest: Some(2),
            writer_id: "0123456789abcdef0123456789abcdef".into()
        }
    );
    assert_eq!(h.encode(), json.to_vec());

    let n = Head {
        format: 1,
        generation: 0,
        manifest: None,
        writer_id: "w".into(),
    };
    assert_eq!(
        String::from_utf8(n.encode()).unwrap(),
        r#"{"format":1,"generation":0,"manifest":null,"writer_id":"w"}"#
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
        r#"{"format":1,"generation":0,"manifest":null,"writer_id":"w\u0001"}"#,
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
    };
    s.write_atomic(dir.path()).unwrap();
    assert_eq!(LocalState::read(dir.path()).unwrap(), Some(s.clone()));
    assert_eq!(
        String::from_utf8(s.encode()).unwrap(),
        r#"{"format":1,"writer_id":"w1","generation":3,"complete":true}"#
    );
    let s2 = LocalState {
        complete: false,
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
