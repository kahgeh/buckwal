//! S3 client against a real RustFS. Run through `./scripts/test.sh`.
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use turso_durable::store::s3::{S3Config, S3Store};
use turso_durable::store::{CondPut, ObjectStore, StoreError};

fn env(k: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| {
        panic!("{k} is not set; run ./scripts/test.sh (or eval \"$(scripts/rustfs.sh env)\")")
    })
}

/// `DURABLE_CA_BUNDLE` (None when unset or empty): the CA of the TLS pass.
fn ca_bundle() -> Option<std::path::PathBuf> {
    std::env::var("DURABLE_CA_BUNDLE")
        .ok()
        .filter(|v| !v.is_empty())
        .map(Into::into)
}

/// The store from the `DURABLE_*` variables, without creating the bucket; never panics.
fn try_s3() -> Result<S3Store, String> {
    let v = |k: &str| std::env::var(k).map_err(|_| format!("{k} is not set"));
    S3Store::new(S3Config {
        endpoint: v("DURABLE_ENDPOINT")?,
        region: v("DURABLE_REGION")?,
        bucket: v("DURABLE_BUCKET")?,
        access_key: v("DURABLE_ACCESS_KEY")?,
        secret_key: v("DURABLE_SECRET_KEY")?,
        path_style: v("DURABLE_PATH_STYLE")? == "true",
        request_timeout: Duration::from_secs(30),
        ca_bundle: ca_bundle(),
    })
    .map_err(|e| format!("store: {e}"))
}

fn store() -> S3Store {
    env("DURABLE_ENDPOINT"); // the hint when the variables are missing
    let s = try_s3().expect("store");
    s.create_bucket().expect("bucket");
    s
}

/// Delete every key under `prefix` (which ends in `/`): the number deleted, or what failed.
fn delete_prefix(s: &S3Store, prefix: &str) -> Result<usize, String> {
    let keys = s.list(prefix).map_err(|e| format!("list {prefix}: {e}"))?;
    let failed: Vec<String> = keys
        .iter()
        .filter_map(|k| s.delete(k).err().map(|e| format!("{k}: {e}")))
        .collect();
    match failed.first() {
        None => Ok(keys.len()),
        Some(first) => Err(format!(
            "{} of {} deletes under {prefix} failed, the first: {first}",
            failed.len(),
            keys.len()
        )),
    }
}

/// A test's prefix, `t/<test>-<nanos>/`.
struct Prefix {
    p: String,
    /// Keep the prefix when the test fails (`DURABLE_KEEP_FAILED=1`).
    keep_failed: bool,
}

impl std::ops::Deref for Prefix {
    type Target = str;
    fn deref(&self) -> &str {
        &self.p
    }
}

impl std::fmt::Display for Prefix {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.p)
    }
}

impl Drop for Prefix {
    /// Delete every key under the prefix, also after a failure unless `keep_failed`, which
    /// prints the prefix instead. Never panics: errors are printed.
    fn drop(&mut self) {
        if std::thread::panicking() && self.keep_failed {
            eprintln!(
                "failed; bucket prefix {} kept (DURABLE_KEEP_FAILED=1)",
                self.p
            );
            return;
        }
        if let Err(e) = try_s3().and_then(|s| delete_prefix(&s, &self.p)) {
            eprintln!("cleanup of bucket prefix {}: {e}", self.p);
        }
    }
}

fn prefix(test: &str) -> Prefix {
    let n = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    Prefix {
        p: format!("t/{test}-{n}/"),
        keep_failed: std::env::var("DURABLE_KEEP_FAILED").as_deref() == Ok("1"),
    }
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn rustfs_conditional_create_and_match() {
    conditional_create_and_match("cond");
}

fn conditional_create_and_match(test: &str) {
    let s = store();
    let p = prefix(test);
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
        ca_bundle: ca_bundle(),
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
    let p = prefix("getmissing");
    let k = format!("{p}nope");
    assert_eq!(s.get(&k).unwrap(), None);
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn rustfs_delete() {
    let s = store();
    let p = prefix("delete");
    let k = format!("{p}k");
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

/// A test thread that fills its prefix and panics: its prefix, and whether it panicked.
fn failed_test_prefix(test: &'static str, keep_failed: bool) -> (String, bool) {
    let (tx, rx) = std::sync::mpsc::channel();
    let joined = std::thread::spawn(move || {
        let s = store();
        let mut p = prefix(test);
        p.keep_failed = keep_failed;
        s.put(&format!("{p}k"), b"x").unwrap();
        tx.send(p.p.clone()).unwrap();
        panic!("{test}: a deliberate failure (the cleanup test)");
    })
    .join();
    (rx.recv().unwrap(), joined.is_err())
}

/// Dropping a `Prefix` deletes every key under it: after a pass, and after a failure unless
/// `DURABLE_KEEP_FAILED=1` (`keep_failed`), which keeps it.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn rustfs_prefix_is_deleted_unless_a_failure_is_kept() {
    let s = store();
    let passed = {
        let p = prefix("cleanup_passed");
        for i in 0..5 {
            s.put(&format!("{p}{i}"), b"x").unwrap();
        }
        assert_eq!(s.list(&p).unwrap().len(), 5);
        p.p.clone()
    };
    assert_eq!(s.list(&passed).unwrap(), Vec::<String>::new(), "passed");
    let (failed, panicked) = failed_test_prefix("cleanup_failed", false);
    assert!(panicked);
    assert_eq!(s.list(&failed).unwrap(), Vec::<String>::new(), "failed");
    let (kept, panicked) = failed_test_prefix("cleanup_failed_kept", true);
    assert!(panicked);
    assert_eq!(
        s.list(&kept).unwrap().len(),
        1,
        "failed, DURABLE_KEEP_FAILED=1"
    );
    assert_eq!(delete_prefix(&s, &kept), Ok(1));
}

#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn stat_reports_etag_and_age() {
    let s = store();
    let p = prefix("stat");
    let k = format!("{p}k");
    assert!(s.stat(&format!("{p}missing")).unwrap().is_none());
    let etag = s.put(&k, b"x").unwrap();
    let st = s.stat(&k).unwrap().unwrap();
    assert_eq!(st.etag, etag);
    assert!(st.age < Duration::from_secs(5), "{:?}", st.age);
    std::thread::sleep(Duration::from_secs(2));
    let st = s.stat(&k).unwrap().unwrap();
    assert!(st.age >= Duration::from_secs(1), "{:?}", st.age);
}

fn require_tls() {
    assert!(
        env("DURABLE_ENDPOINT").starts_with("https://"),
        "tls_* tests run in the TLS pass of ./scripts/test.sh"
    );
}

#[test]
#[ignore = "needs RustFS in TLS mode: ./scripts/test.sh"]
fn tls_round_trip_with_ca_bundle() {
    require_tls();
    assert!(ca_bundle().is_some(), "DURABLE_CA_BUNDLE is not set");
    conditional_create_and_match("tls-cond");
}

/// `open` with `ca_bundle` as given must fail as a certificate error, and not by exhausting
/// the retry deadline.
fn open_fails_on_certificate(ca_bundle: Option<std::path::PathBuf>) {
    use turso_durable::{Config, Error};
    require_tls();
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = Config::from_env(dir.path()).expect("config");
    cfg.ca_bundle = ca_bundle;
    cfg.prefix = prefix("tls-fail").trim_end_matches('/').to_string();
    let t = std::time::Instant::now();
    let r = turso_durable::open(cfg);
    let took = t.elapsed();
    match r {
        Err(Error::Bucket(m)) => {
            assert!(
                m.contains("certificate") && m.contains("DURABLE_CA_BUNDLE"),
                "{m}"
            )
        }
        Err(e) => panic!("expected Error::Bucket, got {e:?}"),
        Ok(_) => panic!("open succeeded without a trusted CA"),
    }
    assert!(took < Duration::from_secs(5), "took {took:?}");
    eprintln!("failed after {took:?}");
}

#[test]
#[ignore = "needs RustFS in TLS mode: ./scripts/test.sh"]
fn tls_without_ca_bundle_fails_fast_as_certificate_error() {
    open_fails_on_certificate(None);
}

#[test]
#[ignore = "needs RustFS in TLS mode: ./scripts/test.sh"]
fn tls_wrong_ca_bundle_fails_fast() {
    let ca = ca_bundle().expect("DURABLE_CA_BUNDLE is not set");
    open_fails_on_certificate(Some(ca.with_file_name("other-ca.pem")));
}
