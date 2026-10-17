use super::*;
use crate::config::Event;
use crate::store::MemStore;
use std::collections::BTreeMap;
use std::sync::mpsc;
use std::sync::Mutex;
use std::time::Instant;

fn keys() -> Keys {
    Keys::new("db")
}

fn head(generation: u64, manifest: Option<u64>) -> Head {
    Head {
        format: 1,
        generation,
        manifest,
        writer_id: "w".into(),
        epoch: 0,
    }
}

/// Keys under each root that no parser accepts: the trimmer keeps them.
fn junk(k: &Keys) -> [String; 3] {
    [
        format!("{}junk", k.log_root()),
        format!("{}0000000001/x", k.page_root()),
        format!("{}x", k.manifest_root()),
    ]
}

/// Generations 1-3 by hand: two logs in 1, one each in 2 and 3, manifests 1 and 2, pages
/// 1/1, 1/2 and 2/1. Manifest 2 maps page 1 to generation 2 and page 2 to generation 1.
/// Generation 3 is being sealed: `pages/3/1`, `pages/3/5` and `manifest/3` are uploaded but
/// the head does not reference them yet. Plus one unparseable key under each root.
fn three_generations() -> (MemStore, Manifest) {
    let k = keys();
    let mem = MemStore::new();
    let m2 = Manifest {
        page_size: 4096,
        page_count: 2,
        pages: BTreeMap::from([(1, 2), (2, 1)]),
    };
    for key in [
        k.log(1, 1),
        k.log(1, 2),
        k.log(2, 1),
        k.log(3, 1),
        k.page(1, 1),
        k.page(1, 2),
        k.page(2, 1),
        k.page(3, 1),
        k.page(3, 5),
        k.manifest(3),
    ]
    .into_iter()
    .chain(junk(&k))
    {
        mem.put(&key, b"x").unwrap();
    }
    mem.put(&k.manifest(1), b"m1").unwrap();
    mem.put(&k.manifest(2), &m2.encode()).unwrap();
    mem.put(&k.head(), &head(3, Some(2)).encode()).unwrap();
    (mem, m2)
}

fn all_keys(mem: &MemStore) -> Vec<String> {
    mem.list("").unwrap()
}

#[test]
fn trim_deletes_only_unreferenced_older_generations() {
    let k = keys();
    let (mem, m2) = three_generations();
    let deleted = trim_once(&mem, &k, &head(3, Some(2)), Some(&m2)).unwrap();
    assert_eq!(deleted, 5);
    // Kept: the current log, manifest 2 and what it references, the in-progress seal of
    // generation 3 (never touched: generation >= M) and keys no parser accepts.
    let mut want = vec![
        k.head(),
        k.log(3, 1),
        k.manifest(2),
        k.page(1, 2),
        k.page(2, 1),
        k.page(3, 1),
        k.page(3, 5),
        k.manifest(3),
    ];
    want.extend(junk(&k));
    want.sort();
    assert_eq!(all_keys(&mem), want);
    // Nothing more to do.
    assert_eq!(
        trim_once(&mem, &k, &head(3, Some(2)), Some(&m2)).unwrap(),
        0
    );
}

#[test]
fn trim_without_manifest_deletes_only_older_logs() {
    let k = keys();
    let (mem, _) = three_generations();
    let before = all_keys(&mem);
    let deleted = trim_once(&mem, &k, &head(2, None), None).unwrap();
    assert_eq!(deleted, 2);
    let mut want: Vec<String> = before
        .into_iter()
        .filter(|key| !key.starts_with(&k.log_prefix(1)))
        .collect();
    want.sort();
    assert_eq!(all_keys(&mem), want);
}

#[test]
fn trimmer_waits_for_grace_and_exits_on_disconnect() {
    let k = keys();
    let (mem, m2) = three_generations();
    let mem: Arc<MemStore> = Arc::new(mem);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let hook: EventHook = {
        let seen = seen.clone();
        Arc::new(move |e: &Event| {
            if let Event::Trimmed { deleted } = e {
                seen.lock().unwrap().push(*deleted);
            }
        })
    };
    let (tx, rx) = mpsc::channel();
    let grace = Duration::from_millis(150);
    let t = spawn_trimmer(mem.clone(), k.clone(), grace, rx, Some(hook));
    let start = Instant::now();
    tx.send(TrimSnapshot {
        at: start,
        head: head(3, Some(2)),
        manifest: Some(m2),
    })
    .unwrap();
    std::thread::sleep(Duration::from_millis(50));
    assert!(seen.lock().unwrap().is_empty(), "trimmed inside the grace");
    assert!(mem.get(&k.log(1, 1)).unwrap().is_some());
    while seen.lock().unwrap().is_empty() {
        assert!(start.elapsed() < Duration::from_secs(5), "never trimmed");
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(start.elapsed() >= grace);
    assert_eq!(*seen.lock().unwrap(), vec![5]);
    assert!(mem.get(&k.log(1, 1)).unwrap().is_none());
    drop(tx);
    t.join().unwrap();
}
