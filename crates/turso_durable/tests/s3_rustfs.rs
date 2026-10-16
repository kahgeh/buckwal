//! S3 client against a real RustFS. Run through `./scripts/test.sh`.
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use turso_durable::store::s3::{S3Config, S3Store};
use turso_durable::store::{CondPut, ObjectStore};

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
