//! S3 client against a real RustFS. Run through `./scripts/test.sh`.
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use turso_durable::store::s3::{S3Config, S3Store};
use turso_durable::store::{CondPut, ObjectStore, StoreError};

fn env(k: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| {
        panic!("{k} is not set; run ./scripts/test.sh (or eval \"$(scripts/rustfs.sh env)\")")
    })
}

fn store() -> S3Store {
    let s = S3Store::new(S3Config {
        endpoint: env("DURABLE_ENDPOINT"),
        region: env("DURABLE_REGION"),
        bucket: env("DURABLE_BUCKET"),
        access_key: env("DURABLE_ACCESS_KEY"),
        secret_key: env("DURABLE_SECRET_KEY"),
        path_style: env("DURABLE_PATH_STYLE") == "true",
        request_timeout: Duration::from_secs(30),
    })
    .expect("store");
    s.create_bucket().expect("bucket");
    s
}

fn prefix(test: &str) -> String {
    let n = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("t/{test}-{n}/")
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn rustfs_conditional_create_and_match() {
    let s = store();
    let p = prefix("cond");
    let k = format!("{p}k");
    let CondPut::Created { etag: e1 } = s.put_if_absent(&k, b"one").unwrap() else {
        panic!("first put_if_absent must create");
    };
    assert_eq!(
        s.put_if_absent(&k, b"x").unwrap(),
        CondPut::PreconditionFailed
    );
    let CondPut::Created { etag: e2 } = s.put_if_match(&k, b"two", &e1).unwrap() else {
        panic!("current etag must match");
    };
    assert_ne!(e1, e2);
    assert_eq!(
        s.put_if_match(&k, b"three", &e1).unwrap(),
        CondPut::PreconditionFailed
    );
    assert_eq!(
        s.put_if_match(&format!("{p}missing"), b"x", &e1).unwrap(),
        CondPut::PreconditionFailed
    );
    let o = s.get(&k).unwrap().unwrap();
    assert_eq!((o.body.as_slice(), &o.etag), (&b"two"[..], &e2));
    let e3 = s.put(&k, b"two").unwrap();
    assert_eq!(s.get(&k).unwrap().unwrap().etag, e3);
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn rustfs_list_follows_pages() {
    let s = store();
    let p = prefix("list");
    let keys: Vec<String> = (0..1005).map(|i| format!("{p}{i:05}")).collect();
    std::thread::scope(|sc| {
        for chunk in keys.chunks(1005usize.div_ceil(8)) {
            let s = &s;
            sc.spawn(move || {
                for k in chunk {
                    s.put(k, b"x").unwrap();
                }
            });
        }
    });
    let before = s.stats();
    let listed = s.list(&p).unwrap();
    assert_eq!(listed, keys);
    assert_eq!(s.stats().since(&before).lists, 2);
}

/// Item 4: RustFS's 404 for `If-Match` on a missing key carries `NoSuchKey`
/// (`PreconditionFailed`, asserted above); one for a missing bucket carries `NoSuchBucket` and
/// is a permanent error naming the bucket, never `PreconditionFailed`.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn rustfs_if_match_on_missing_bucket_is_permanent() {
    let bucket = format!(
        "no-such-bucket-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let s = S3Store::new(S3Config {
        endpoint: env("DURABLE_ENDPOINT"),
        region: env("DURABLE_REGION"),
        bucket: bucket.clone(),
        access_key: env("DURABLE_ACCESS_KEY"),
        secret_key: env("DURABLE_SECRET_KEY"),
        path_style: env("DURABLE_PATH_STYLE") == "true",
        request_timeout: Duration::from_secs(30),
    })
    .expect("store");
    match s.put_if_match("k", b"x", "\"0\"") {
        Err(StoreError::Permanent(m)) => {
            assert!(m.contains(&bucket) && m.contains("NoSuchBucket"), "{m}")
        }
        r => panic!("If-Match on a missing bucket: {r:?}"),
    }
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn rustfs_get_missing_is_none() {
    let s = store();
    let k = format!("{}nope", prefix("getmissing"));
    assert_eq!(s.get(&k).unwrap(), None);
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn rustfs_delete() {
    let s = store();
    let k = format!("{}k", prefix("delete"));
    s.put(&k, b"v").unwrap();
    s.delete(&k).unwrap();
    assert_eq!(s.get(&k).unwrap(), None);
    s.delete(&k).unwrap(); // deleting a missing key is fine
    assert_eq!(s.stats().deletes, 2);
}

/// The public API end to end over S3: open (create), commit, close, restore, reopen.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn rustfs_open_commit_close_restore() {
    use turso_durable::{Config, Value};
    store(); // creates the bucket
    let p = prefix("api");
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = Config::from_env(dir.path()).expect("config");
    cfg.prefix = p.trim_end_matches('/').to_string();
    let db = turso_durable::open(cfg.clone()).expect("open");
    let c = db.connect().unwrap();
    c.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)", &[])
        .unwrap();
    for id in 1..=20 {
        c.execute(
            "INSERT INTO t(id, v) VALUES (?, 'x')",
            &[Value::from_i64(id)],
        )
        .unwrap();
    }
    c.close().unwrap();
    db.close().unwrap();

    let r = tempfile::tempdir().unwrap();
    let rep = turso_durable::restore(&cfg, r.path()).expect("restore");
    assert!(rep.generation >= 2, "close seals: {rep:?}");
    let mut cfg2 = cfg.clone();
    cfg2.dir = r.path().to_path_buf();
    let db = turso_durable::open(cfg2).expect("reopen restored");
    let rows = db
        .connect()
        .unwrap()
        .query("SELECT count(*) FROM t", &[])
        .unwrap();
    assert_eq!(rows[0][0].as_int(), Some(20));
}
