//! S3 client against a real RustFS. Run through `./scripts/test.sh`.
use buckwal::store::s3::{S3Config, S3Store};
use buckwal::store::{CondPut, ObjectStore, StoreError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn env(k: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| {
        panic!("{k} is not set; run ./scripts/test.sh (or eval \"$(scripts/rustfs.sh env)\")")
    })
}

/// `BUCKWAL_CA_BUNDLE` (None when unset or empty): the CA of the TLS pass.
fn ca_bundle() -> Option<std::path::PathBuf> {
    std::env::var("BUCKWAL_CA_BUNDLE")
        .ok()
        .filter(|v| !v.is_empty())
        .map(Into::into)
}

/// The store from the `BUCKWAL_*` variables, without creating the bucket; never panics.
fn try_s3() -> Result<S3Store, String> {
    let v = |k: &str| std::env::var(k).map_err(|_| format!("{k} is not set"));
    S3Store::new(S3Config {
        endpoint: v("BUCKWAL_ENDPOINT")?,
        region: v("BUCKWAL_REGION")?,
        bucket: v("BUCKWAL_BUCKET")?,
        access_key: v("BUCKWAL_ACCESS_KEY")?,
        secret_key: v("BUCKWAL_SECRET_KEY")?,
        path_style: v("BUCKWAL_PATH_STYLE")? == "true",
        request_timeout: Duration::from_secs(30),
        ca_bundle: ca_bundle(),
    })
    .map_err(|e| format!("store: {e}"))
}

fn store() -> S3Store {
    env("BUCKWAL_ENDPOINT"); // the hint when the variables are missing
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
    /// Keep the prefix when the test fails (`BUCKWAL_KEEP_FAILED=1`).
    keep_failed: bool,
    /// Keep the prefix, pass or fail (`BUCKWAL_KEEP_PREFIXES=1`): `scripts/test.sh` scans
    /// every object each pass leaves.
    keep_all: bool,
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
    /// prints the prefix instead; `keep_all` keeps it. Never panics: errors are printed.
    fn drop(&mut self) {
        if self.keep_all {
            return;
        }
        if std::thread::panicking() && self.keep_failed {
            eprintln!(
                "failed; bucket prefix {} kept (BUCKWAL_KEEP_FAILED=1)",
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
        keep_failed: std::env::var("BUCKWAL_KEEP_FAILED").as_deref() == Ok("1"),
        keep_all: std::env::var("BUCKWAL_KEEP_PREFIXES").as_deref() == Ok("1"),
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
        endpoint: env("BUCKWAL_ENDPOINT"),
        region: env("BUCKWAL_REGION"),
        bucket: bucket.clone(),
        access_key: env("BUCKWAL_ACCESS_KEY"),
        secret_key: env("BUCKWAL_SECRET_KEY"),
        path_style: env("BUCKWAL_PATH_STYLE") == "true",
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

/// A conditional delete (spec rev 11, rule 1: a create removes a leftover `pack-latest` with
/// `If-Match`): a matching ETag deletes, another ETag is a 412 and keeps the object, and a
/// missing key is no error.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn rustfs_delete_if_match() {
    let s = store();
    let p = prefix("delifmatch");
    let k = format!("{p}k");
    let e1 = s.put(&k, b"one").unwrap();
    let e2 = s.put(&k, b"two").unwrap();
    assert_ne!(e1, e2);
    assert!(!s.delete_if_match(&k, &e1).unwrap(), "an old ETag");
    assert_eq!(s.get(&k).unwrap().unwrap().body, b"two");
    assert!(s.delete_if_match(&k, &e2).unwrap());
    assert_eq!(s.get(&k).unwrap(), None);
    // A missing key: RustFS answers 412 (`false`), S3 a 404 `NoSuchKey` (`true`). Either is
    // no error, and a caller reads the key again (`pack::first`).
    let gone = s.delete_if_match(&k, &e2).unwrap();
    eprintln!("rustfs_delete_if_match: a missing key answers {gone}");
    assert_eq!(s.get(&k).unwrap(), None);
}

/// `list_after` against the real server: only keys strictly greater than `start_after`, in
/// order, and a listing past the 1,000 keys of one `ListObjectsV2` page comes back whole
/// (the continuation token is followed) with `start-after` honoured. About 1,100 tiny
/// objects; deleted by the prefix's drop.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn s3_list_after_start_after_and_paging() {
    let s = store();
    let p = prefix("listafter");
    let key = |i: usize| format!("{p}k/{i:010}");
    const N: usize = 1100;
    std::thread::scope(|sc| {
        let (s, key) = (&s, &key);
        for t in 0..8 {
            sc.spawn(move || {
                for i in (t..N).step_by(8) {
                    s.put(&key(i), b"x").unwrap();
                }
            });
        }
    });
    let dir = format!("{p}k/");
    let l0 = s.stats().lists;
    let all = s.list(&dir).unwrap();
    let full = s.stats().lists - l0;
    assert!(full >= 2, "1,100 keys took {full} LIST request(s)");
    assert_eq!(
        all,
        (0..N).map(key).collect::<Vec<_>>(),
        "list pages past 1000"
    );

    // Strictly greater: the start key itself is not returned, and it need not exist.
    let after = s.list_after(&dir, &key(4)).unwrap();
    assert_eq!(after, (5..N).map(key).collect::<Vec<_>>());
    assert!(
        after.len() > 1000,
        "{} keys: the listing paged",
        after.len()
    );
    let between = s.list_after(&dir, &format!("{}x", key(9))).unwrap();
    assert_eq!(between, (10..N).map(key).collect::<Vec<_>>());
    assert!(s.list_after(&dir, &key(N - 1)).unwrap().is_empty());
    assert_eq!(s.list_after(&dir, "").unwrap(), all);
    // Past the first page's last key: the server skips them, so one LIST holds the rest.
    let l1 = s.stats().lists;
    assert_eq!(
        s.list_after(&dir, &key(1049)).unwrap(),
        (1050..N).map(key).collect::<Vec<_>>()
    );
    assert_eq!(
        s.stats().lists - l1,
        1,
        "start-after left 50 keys: one LIST"
    );
    // A neighbouring prefix is not listed.
    s.put(&format!("{p}kk"), b"x").unwrap();
    assert_eq!(s.list_after(&dir, &key(N - 2)).unwrap(), vec![key(N - 1)]);
}

/// A source over bytes that records the largest offset read, checks a hook before each read
/// and fails the read at one offset.
struct Src<'a> {
    data: &'a [u8],
    before_read: Box<dyn Fn() + Send + Sync + 'a>,
    fail_at: Option<u64>,
}

impl buckwal::store::PartSource for Src<'_> {
    fn len(&self) -> u64 {
        self.data.len() as u64
    }
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<()> {
        (self.before_read)();
        if self.fail_at == Some(offset) {
            return Err(std::io::Error::other("injected"));
        }
        buf.copy_from_slice(&self.data[offset as usize..offset as usize + buf.len()]);
        Ok(())
    }
}

const MIB: usize = 1024 * 1024;

/// A 40 MB object in 16 MiB parts, 8 at once, against the real server: the object is
/// invisible until the upload completes, the part checksums are the SHA-256 of each part,
/// three ranges read it back, and `stat` and `list_sized` give its size.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn s3_multipart_40mb_then_range_reads() {
    use sha2::{Digest, Sha256};
    let s = store();
    let p = prefix("multipart");
    let key = format!("{p}pack");
    let data: Vec<u8> = (0..40_000_000u32).map(|i| (i % 251) as u8).collect();
    let src = Src {
        data: &data,
        before_read: Box::new(|| assert!(s.get(&key).unwrap().is_none(), "visible too early")),
        fail_at: None,
    };
    let mut src = src;
    let done = s.put_multipart(&key, &mut src, 16 * MIB, 8).unwrap();
    assert_eq!(done.part_sha256.len(), 3);
    for (i, sha) in done.part_sha256.iter().enumerate() {
        let part = &data[i * 16 * MIB..((i + 1) * 16 * MIB).min(data.len())];
        assert_eq!(sha.as_slice(), Sha256::digest(part).as_slice(), "part {i}");
    }
    assert!(done.etag.trim_matches('"').ends_with("-3"), "{}", done.etag);
    let mut back = Vec::new();
    for i in 0..3u64 {
        let r = s
            .get_range(&key, i * 16 * MIB as u64, 16 * MIB as u64)
            .unwrap();
        back.extend(r.expect("object is there"));
    }
    assert!(back == data, "ranges differ from the source");
    assert_eq!(s.stat(&key).unwrap().unwrap().size, 40_000_000);
    assert_eq!(s.list_sized(&p).unwrap(), vec![(key.clone(), 40_000_000)]);
    // Past the end: cut short, then nothing.
    let tail = s.get_range(&key, 39_999_990, 100).unwrap().unwrap();
    assert_eq!(tail, &data[39_999_990..]);
    assert_eq!(s.get_range(&key, 40_000_000, 10).unwrap().unwrap(), b"");
    assert_eq!(s.get_range(&format!("{p}missing"), 0, 10).unwrap(), None);
}

/// A part that fails leaves nothing visible; `abort_incomplete` keeps a fresh upload when
/// the age bound is an hour, removes it with a bound of zero, and then finds none. A
/// completed upload is not a candidate.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn s3_failed_part_is_invisible_and_abort_incomplete_removes_it() {
    let s = store();
    let p = prefix("abort");
    let data = vec![9u8; 12 * MIB];
    let mut bad = Src {
        data: &data,
        before_read: Box::new(|| {}),
        fail_at: Some(5 * MIB as u64),
    };
    let key = format!("{p}bad");
    assert!(s.put_multipart(&key, &mut bad, 5 * MIB, 1).is_err());
    assert!(s.get(&key).unwrap().is_none());
    assert!(s.list(&p).unwrap().is_empty());
    let mut ok = Src {
        data: &data,
        before_read: Box::new(|| {}),
        fail_at: None,
    };
    s.put_multipart(&format!("{p}ok"), &mut ok, 5 * MIB, 2)
        .unwrap();
    assert_eq!(
        s.abort_incomplete(&p, Duration::from_secs(3600)).unwrap(),
        0
    );
    assert_eq!(s.abort_incomplete(&p, Duration::ZERO).unwrap(), 1);
    assert_eq!(s.abort_incomplete(&p, Duration::ZERO).unwrap(), 0);
    assert_eq!(s.list(&p).unwrap(), vec![format!("{p}ok")]);
}

/// The public API end to end over S3: open (create), commit, close, restore, reopen.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn rustfs_open_commit_close_restore() {
    use buckwal::{Config, Value};
    store(); // creates the bucket
    let p = prefix("api");
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = Config::from_env(dir.path()).expect("config");
    cfg.prefix = p.trim_end_matches('/').to_string();
    let db = buckwal::open(cfg.clone()).expect("open");
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
    let rep = buckwal::restore(&cfg, r.path()).expect("restore");
    assert!(rep.generation >= 2, "close seals: {rep:?}");
    let mut cfg2 = cfg.clone();
    cfg2.dir = r.path().to_path_buf();
    let db = buckwal::open(cfg2).expect("reopen restored");
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
        p.keep_all = false;
        s.put(&format!("{p}k"), b"x").unwrap();
        tx.send(p.p.clone()).unwrap();
        panic!("{test}: a deliberate failure (the cleanup test)");
    })
    .join();
    (rx.recv().unwrap(), joined.is_err())
}

/// Dropping a `Prefix` deletes every key under it: after a pass, and after a failure unless
/// `BUCKWAL_KEEP_FAILED=1` (`keep_failed`), which keeps it. `keep_all`
/// (`BUCKWAL_KEEP_PREFIXES=1`) is off here, whatever the caller set.
#[test]
#[ignore = "needs RustFS: ./scripts/test.sh"]
fn rustfs_prefix_is_deleted_unless_a_failure_is_kept() {
    let s = store();
    let passed = {
        let mut p = prefix("cleanup_passed");
        p.keep_all = false;
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
        "failed, BUCKWAL_KEEP_FAILED=1"
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
        env("BUCKWAL_ENDPOINT").starts_with("https://"),
        "tls_* tests run in the TLS pass of ./scripts/test.sh"
    );
}

#[test]
#[ignore = "needs RustFS in TLS mode: ./scripts/test.sh"]
fn tls_round_trip_with_ca_bundle() {
    require_tls();
    assert!(ca_bundle().is_some(), "BUCKWAL_CA_BUNDLE is not set");
    conditional_create_and_match("tls-cond");
}

/// `open` with `ca_bundle` as given must fail as a certificate error, and not by exhausting
/// the retry deadline.
fn open_fails_on_certificate(ca_bundle: Option<std::path::PathBuf>) {
    use buckwal::{Config, Error};
    require_tls();
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = Config::from_env(dir.path()).expect("config");
    cfg.ca_bundle = ca_bundle;
    cfg.prefix = prefix("tls-fail").trim_end_matches('/').to_string();
    let t = std::time::Instant::now();
    let r = buckwal::open(cfg);
    let took = t.elapsed();
    match r {
        Err(Error::Bucket(m)) => {
            assert!(
                m.contains("certificate") && m.contains("BUCKWAL_CA_BUNDLE"),
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
    let ca = ca_bundle().expect("BUCKWAL_CA_BUNDLE is not set");
    open_fails_on_certificate(Some(ca.with_file_name("other-ca.pem")));
}

/// One HTTP/1.1 request from `s`: the request line, whether it carries `Authorization`, and
/// the `Content-Length` body.
fn read_request(s: &mut impl std::io::Read) -> (String, bool, Vec<u8>) {
    let mut head = Vec::new();
    let mut b = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") && s.read(&mut b).unwrap_or(0) == 1 {
        head.push(b[0]);
    }
    let text = String::from_utf8_lossy(&head).to_string();
    let first = text.split("\r\n").next().unwrap_or_default().to_string();
    let n = text
        .split("\r\n")
        .find_map(|h| {
            let (k, v) = h.split_once(':')?;
            k.eq_ignore_ascii_case("content-length")
                .then(|| v.trim().parse().ok())
                .flatten()
        })
        .unwrap_or(0);
    let auth = text
        .split("\r\n")
        .any(|h| h.to_ascii_lowercase().starts_with("authorization:"));
    let mut body = vec![0u8; n];
    let _ = s.read_exact(&mut body);
    (first, auth, body)
}

/// Serve `l`: a TLS connection (the test CA's server certificate) gets 307 with a
/// `Location` on plain `http://127.0.0.1:{plain_port}`; a plain connection sends what it got
/// to `reached` and gets 200 with an ETag.
fn serve_redirects(
    l: std::net::TcpListener,
    tls: std::sync::Arc<rustls::ServerConfig>,
    plain_port: u16,
    reached: std::sync::mpsc::Sender<String>,
) {
    use std::io::Write;
    std::thread::spawn(move || {
        for s in l.incoming() {
            let Ok(mut s) = s else { continue };
            let mut first = [0u8; 1];
            if s.peek(&mut first).is_err() {
                continue;
            }
            if first[0] != 0x16 {
                let (line, auth, body) = read_request(&mut s);
                let body = String::from_utf8_lossy(&body);
                let _ = reached.send(format!("{line} authorization={auth} body={body:?}"));
                let _ = write!(
                    s,
                    "HTTP/1.1 200 OK\r\nETag: \"e1\"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                );
                continue;
            }
            let Ok(conn) = rustls::ServerConnection::new(tls.clone()) else {
                continue;
            };
            let mut t = rustls::StreamOwned::new(conn, s);
            let (line, _, _) = read_request(&mut t);
            let path = line.split(' ').nth(1).unwrap_or("/").to_string();
            let _ = write!(
                t,
                "HTTP/1.1 307 Temporary Redirect\r\nLocation: http://127.0.0.1:{plain_port}{path}\r\n\
                 Content-Length: 0\r\nConnection: close\r\n\r\n"
            );
            t.conn.send_close_notify();
            let _ = t.flush();
        }
    });
}

/// B1 over TLS: an HTTPS endpoint answering 307 to plain http, on another port or on its own
/// port, must not get the body or the signature sent in clear, and its 307 is no ack:
/// every call is a permanent error naming the status.
#[test]
#[ignore = "needs RustFS in TLS mode: ./scripts/test.sh"]
fn tls_redirect_to_plain_http_is_not_followed() {
    use rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
    require_tls();
    let ca = ca_bundle().expect("BUCKWAL_CA_BUNDLE is not set");
    let server = ca.with_file_name("server");
    let _ = rustls::crypto::ring::default_provider().install_default();
    let certs: Vec<_> = CertificateDer::pem_file_iter(server.join("rustfs_cert.pem"))
        .expect("server certificate")
        .map(|c| c.expect("server certificate"))
        .collect();
    let key = PrivateKeyDer::from_pem_file(server.join("rustfs_key.pem")).expect("server key");
    let tls = std::sync::Arc::new(
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .expect("server config"),
    );
    let mut wrong = Vec::new();
    for same_port in [false, true] {
        let (tx, reached) = std::sync::mpsc::channel();
        let https = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = https.local_addr().unwrap().port();
        let plain_port = if same_port {
            port
        } else {
            let plain = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let p = plain.local_addr().unwrap().port();
            serve_redirects(plain, tls.clone(), p, tx.clone());
            p
        };
        serve_redirects(https, tls.clone(), plain_port, tx);
        let s = S3Store::new(S3Config {
            endpoint: format!("https://127.0.0.1:{port}"),
            region: "us-east-1".into(),
            bucket: "b".into(),
            access_key: "AK".into(),
            secret_key: "SK".into(),
            path_style: true,
            request_timeout: Duration::from_secs(5),
            ca_bundle: Some(ca.clone()),
        })
        .expect("store");
        let calls: [(&str, Result<(), StoreError>); 3] = [
            ("put", s.put("log/1/00000001", b"SECRET-ROW-DATA").map(drop)),
            (
                "put_if_absent",
                s.put_if_absent("log/1/00000001", b"SECRET-ROW-DATA")
                    .map(drop),
            ),
            ("get", s.get("log/1/00000001").map(drop)),
        ];
        for (call, r) in calls {
            match r {
                Err(StoreError::Permanent(m)) if m.contains("HTTP 307") => {}
                r => wrong.push(format!("same_port={same_port} {call}: {r:?}")),
            }
        }
        while let Ok(req) = reached.recv_timeout(Duration::from_millis(300)) {
            wrong.push(format!("same_port={same_port}: plain http got {req}"));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
