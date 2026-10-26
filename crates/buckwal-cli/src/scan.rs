//! `scan`: GET every object under the prefix (or in the whole bucket) and report which
//! plaintext needles appear in them. The test command's proof that an encrypted run leaves
//! no plaintext in the bucket (D16), and its control that a plain run does.
use crate::args::{Expect, ScanArgs};
use crate::{config, err, s3_store, say_all, CliResult};
use buckwal::store::{with_retry, ObjectStore, RetryPolicy};
use std::path::Path;

/// The label of the key's needles (lower-case hex, upper-case hex, raw bytes).
const KEY_LABEL: &str = "the encryption key";
/// Concurrent GETs.
const THREADS: usize = 8;

/// What a scan saw: the objects it read and, for each label found, how many objects hold
/// one of its needles and the first such key (in key order).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScanReport {
    pub objects: u64,
    pub bytes: u64,
    /// The objects read, by kind.
    pub kinds: Kinds,
    pub hits: Vec<(String, u64, String)>,
}

/// The objects a scan read, by the kind their key names.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Kinds {
    pub head: u64,
    /// `pack-latest`.
    pub pointer: u64,
    /// `pack/<generation>/<run id>`.
    pub pack: u64,
    /// `pack/<generation>/<run id>.codes`.
    pub codes: u64,
    pub log: u64,
    /// `pages/<g>/<page>` (formats 1 to 4).
    pub pages: u64,
    /// `manifest/<g>` (formats 1 to 4).
    pub manifest: u64,
    pub other: u64,
}

/// The size of each range GET of a large object (a pack).
const RANGE: u64 = 16 << 20;

/// The labels of `needles`, each once, in order.
fn labels(needles: &[(String, Vec<u8>)]) -> Vec<&str> {
    let mut out: Vec<&str> = Vec::new();
    for (label, _) in needles {
        if !out.contains(&label.as_str()) {
            out.push(label);
        }
    }
    out
}

fn contains(hay: &[u8], needle: &[u8]) -> bool {
    let Some((&first, rest)) = needle.split_first() else {
        return true;
    };
    let mut at = 0;
    while let Some(i) = hay[at..].iter().position(|&b| b == first) {
        let start = at + i;
        if hay[start + 1..].starts_with(rest) {
            return true;
        }
        at = start + 1;
    }
    false
}

/// What one thread saw of its keys: objects, bytes, kinds, and per label the count and
/// first key.
type Part = (u64, u64, Kinds, Vec<(u64, Option<String>)>);

impl Kinds {
    /// Count `key` as its kind: the last part of the key names it (`<prefix>/head`,
    /// `<prefix>/pack-latest`, `<prefix>/pack/<g>/<run id>[.codes]`, `<prefix>/log/...`,
    /// `<prefix>/pages/...`, `<prefix>/manifest/...`).
    fn count(&mut self, key: &str) {
        let parts: Vec<&str> = key.split('/').collect();
        let dir = |name: &str, depth: usize| {
            parts.len() > depth && parts[parts.len() - 1 - depth] == name
        };
        let last = parts[parts.len() - 1];
        let n = if last == "head" {
            &mut self.head
        } else if last == "pack-latest" {
            &mut self.pointer
        } else if dir("pack", 2) && last.ends_with(".codes") {
            &mut self.codes
        } else if dir("pack", 2) {
            &mut self.pack
        } else if dir("log", 2) {
            &mut self.log
        } else if dir("pages", 2) {
            &mut self.pages
        } else if dir("manifest", 1) {
            &mut self.manifest
        } else {
            &mut self.other
        };
        *n += 1;
    }

    fn add(&mut self, o: &Kinds) {
        self.head += o.head;
        self.pointer += o.pointer;
        self.pack += o.pack;
        self.codes += o.codes;
        self.log += o.log;
        self.pages += o.pages;
        self.manifest += o.manifest;
        self.other += o.other;
    }
}

/// The bytes of the object `key` of `size` bytes, given to `look` in order: one GET when
/// it is at most `range` bytes, else range GETs of `range` bytes, each with the last
/// `overlap` bytes of the one before it, so a needle across two ranges is seen. False when the
/// key was deleted since the listing.
fn read_object(
    store: &dyn ObjectStore,
    key: &str,
    size: u64,
    range: u64,
    overlap: usize,
    look: &mut dyn FnMut(&[u8]),
) -> Result<bool, buckwal::Error> {
    let policy = RetryPolicy::default();
    if size <= range {
        let got = with_retry(&policy, &format!("get {key}"), |_| store.get(key))?;
        let Some(object) = got else { return Ok(false) };
        look(&object.body);
        return Ok(true);
    }
    let mut tail: Vec<u8> = Vec::new();
    let mut at = 0u64;
    loop {
        let got = with_retry(&policy, &format!("get {key} at {at}"), |_| {
            store.get_range(key, at, range)
        })?;
        let Some(bytes) = got else { return Ok(false) };
        if bytes.is_empty() {
            return Ok(true);
        }
        let mut window = std::mem::take(&mut tail);
        window.extend_from_slice(&bytes);
        look(&window);
        at += bytes.len() as u64;
        let keep = overlap.min(window.len());
        tail = window[window.len() - keep..].to_vec();
        if (bytes.len() as u64) < range {
            return Ok(true);
        }
    }
}

/// GET each of `keys` (in order, with their sizes) and look for the needles of each of
/// `labels` in it.
fn scan_keys(
    store: &dyn ObjectStore,
    keys: &[(String, u64)],
    labels: &[&str],
    needles: &[(String, Vec<u8>)],
    range: u64,
) -> Result<Part, buckwal::Error> {
    let overlap = needles
        .iter()
        .map(|(_, n)| n.len().saturating_sub(1))
        .max()
        .unwrap_or(0);
    let mut part: Part = (0, 0, Kinds::default(), vec![(0, None); labels.len()]);
    for (key, size) in keys {
        let mut found = vec![false; labels.len()];
        let mut look = |bytes: &[u8]| {
            for (label, f) in labels.iter().zip(&mut found) {
                let mut mine = needles.iter().filter(|(l, _)| l == label);
                *f = *f || mine.any(|(_, n)| contains(bytes, n));
            }
        };
        if !read_object(store, key, *size, range, overlap, &mut look)? {
            continue;
        }
        part.0 += 1;
        part.1 += size;
        part.2.count(key);
        for (hit, f) in part.3.iter_mut().zip(found) {
            if f {
                hit.0 += 1;
                hit.1.get_or_insert_with(|| key.clone());
            }
        }
    }
    Ok(part)
}

/// GET every key under `prefix` and look for each needle in it. Needles sharing a label
/// count as one: an object holding any of them is a hit for that label. A key deleted
/// between the list and its GET is skipped. An object larger than 16 MiB (a pack) is read
/// in 16 MiB ranges.
pub fn scan(
    store: &dyn ObjectStore,
    prefix: &str,
    needles: &[(String, Vec<u8>)],
) -> Result<ScanReport, buckwal::Error> {
    scan_in(store, prefix, needles, RANGE)
}

/// [`scan`], reading an object larger than `range` bytes in ranges of `range` bytes.
pub fn scan_in(
    store: &dyn ObjectStore,
    prefix: &str,
    needles: &[(String, Vec<u8>)],
    range: u64,
) -> Result<ScanReport, buckwal::Error> {
    let policy = RetryPolicy::default();
    let keys: Vec<(String, u64)> = with_retry(&policy, &format!("list {prefix}"), |_| {
        store.list_sized(prefix)
    })?
    .into_iter()
    .map(|l| (l.key, l.size))
    .collect();
    let labels = labels(needles);
    let size = keys.len().div_ceil(THREADS).max(1);
    let parts: Vec<_> = std::thread::scope(|sc| {
        let running: Vec<_> = keys
            .chunks(size)
            .map(|chunk| sc.spawn(|| scan_keys(store, chunk, &labels, needles, range)))
            .collect();
        running.into_iter().map(|t| t.join().unwrap()).collect()
    });
    let mut report = ScanReport::default();
    let mut hits: Vec<(u64, Option<String>)> = vec![(0, None); labels.len()];
    // The chunks are in key order, so the first chunk with a hit holds its first key.
    for part in parts {
        let (objects, bytes, kinds, found) = part?;
        report.objects += objects;
        report.bytes += bytes;
        report.kinds.add(&kinds);
        for (hit, (n, first)) in hits.iter_mut().zip(found) {
            hit.0 += n;
            hit.1 = hit.1.take().or(first);
        }
    }
    report.hits = labels
        .iter()
        .zip(hits)
        .filter_map(|(label, (n, first))| Some((label.to_string(), n, first?)))
        .collect();
    Ok(report)
}

/// `scan: <n> objects, <bytes> bytes`, then one line per label: `<label>: not found` or
/// `<label>: found in <k> objects (first: <key>)`. Labels only, never a needle's bytes.
pub fn report_lines(report: &ScanReport, needles: &[(String, Vec<u8>)]) -> Vec<String> {
    let k = &report.kinds;
    let mut lines = vec![
        format!("scan: {} objects, {} bytes", report.objects, report.bytes),
        format!(
            "scan kinds: head={} pointer={} pack={} codes={} log={} pages={} manifest={} other={}",
            k.head, k.pointer, k.pack, k.codes, k.log, k.pages, k.manifest, k.other
        ),
    ];
    for label in labels(needles) {
        lines.push(match report.hits.iter().find(|(l, _, _)| l == label) {
            Some((_, n, first)) => format!("{label}: found in {n} objects (first: {first})"),
            None => format!("{label}: not found"),
        });
    }
    lines
}

/// The key in `text` (hex, either case, ASCII whitespace around it) as raw bytes; the
/// error never holds the text.
fn key_bytes(text: &[u8]) -> Option<Vec<u8>> {
    let hex = text.trim_ascii();
    if hex.is_empty() || !hex.len().is_multiple_of(2) {
        return None;
    }
    let digit = |c: u8| (c as char).to_digit(16).map(|d| d as u8);
    hex.chunks(2)
        .map(|p| Some(digit(p[0])? << 4 | digit(p[1])?))
        .collect()
}

/// The needles of the test command: the fixed ones, each marker, and the key in three forms
/// when `key` is given.
pub fn needles(markers: &[String], key: Option<&[u8]>) -> Vec<(String, Vec<u8>)> {
    let mut n = vec![
        ("CREATE TABLE".to_string(), b"CREATE TABLE".to_vec()),
        ("SQLite header".to_string(), b"SQLite format 3\0".to_vec()),
        ("bench payload".to_string(), vec![0xab; 64]),
    ];
    for (i, m) in markers.iter().enumerate() {
        n.push((format!("marker {}", i + 1), m.as_bytes().to_vec()));
    }
    if let Some(key) = key {
        let hex: String = key.iter().map(|b| format!("{b:02x}")).collect();
        n.push((KEY_LABEL.to_string(), hex.to_uppercase().into_bytes()));
        n.push((KEY_LABEL.to_string(), hex.into_bytes()));
        n.push((KEY_LABEL.to_string(), key.to_vec()));
    }
    n
}

/// Whether `report` is what `expect` asks for: `absent`, no label found; `present`,
/// `CREATE TABLE` and every marker found and the key not.
pub fn meets(expect: Expect, report: &ScanReport, needles: &[(String, Vec<u8>)]) -> bool {
    let found = |label: &str| report.hits.iter().any(|(l, _, _)| l == label);
    match expect {
        Expect::Absent => report.hits.is_empty(),
        Expect::Present => {
            let markers = labels(needles)
                .into_iter()
                .filter(|l| l.starts_with("marker "));
            found("CREATE TABLE") && markers.into_iter().all(found) && !found(KEY_LABEL)
        }
    }
}

/// `buckwal scan`: the `BUCKWAL_*` settings name the bucket, the prefix, the CA bundle
/// and the key file.
pub fn run(a: &ScanArgs) -> CliResult<()> {
    let config = config(Path::new("."))?;
    let key = match &config.encryption_key_file {
        Some(path) => {
            let text = std::fs::read(path)
                .map_err(|e| format!("encryption key file {}: {e}", path.display()))?;
            let key = key_bytes(&text)
                .ok_or_else(|| format!("encryption key file {}: not a hex key", path.display()))?;
            Some(key)
        }
        None => None,
    };
    let needles = needles(&a.markers, key.as_deref());
    let prefix = match (a.all, config.prefix.trim_matches('/')) {
        (true, _) | (false, "") => String::new(),
        (false, p) => format!("{p}/"),
    };
    let store = s3_store(&config)?;
    let report = scan(store.as_ref(), &prefix, &needles).map_err(err)?;
    say_all("", &report_lines(&report, &needles));
    if !meets(a.expect, &report, &needles) {
        return Err(format!("scan: not what --expect {} asks for", a.expect));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use buckwal::store::MemStore;

    fn needles(key: &[u8]) -> Vec<(String, Vec<u8>)> {
        super::needles(&[], Some(key))
    }

    /// Bytes from a fixed xorshift, so a failure reproduces.
    fn noise(seed: u64, len: usize) -> Vec<u8> {
        let mut x = seed | 1;
        (0..len)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect()
    }

    const KEY: [u8; 32] = [0x5a; 32];

    /// The objects of a format 5 bucket: the head, the pack pointer, a pack and its codes, and
    /// the log. Each is read and counted by its kind. A pack larger than one range is read in
    /// ranges, and a needle across two ranges is found.
    #[test]
    fn scan_reads_packs_and_codes() {
        let s = MemStore::new();
        let range = 1024u64;
        let mut pack = noise(4, 3 * range as usize + 100);
        let at = range as usize - 5;
        pack[at..at + 12].copy_from_slice(b"CREATE TABLE");
        let mut codes = noise(5, 64);
        codes.extend_from_slice(b"m-codes");
        s.put("t/head", b"{\"format\":5}").unwrap();
        s.put("t/pack-latest", b"{\"format\":1}").unwrap();
        s.put("t/pack/0000000003/r1", &pack).unwrap();
        s.put("t/pack/0000000003/r1.codes", &codes).unwrap();
        s.put("t/log/0000000003/0000000001", &noise(6, 50)).unwrap();
        let n = super::needles(&["m-codes".to_string()], Some(&KEY));
        let before = s.stats();
        let r = scan_in(&s, "t/", &n, range).unwrap();
        let gets = s.stats().since(&before).gets;
        let bytes = pack.len() + codes.len() + 50 + 12 + 12;
        assert_eq!((r.objects, r.bytes), (5, bytes as u64));
        assert_eq!(
            r.kinds,
            Kinds {
                head: 1,
                pointer: 1,
                pack: 1,
                codes: 1,
                log: 1,
                ..Kinds::default()
            }
        );
        assert_eq!(
            r.hits,
            vec![
                (
                    "CREATE TABLE".to_string(),
                    1,
                    "t/pack/0000000003/r1".to_string()
                ),
                (
                    "marker 1".to_string(),
                    1,
                    "t/pack/0000000003/r1.codes".to_string()
                ),
            ]
        );
        // The pack in 4 ranges, each other object in one GET.
        assert_eq!(gets, 4 + 4, "the pack is read in ranges");
        let lines = report_lines(&r, &n);
        assert_eq!(
            lines[1],
            "scan kinds: head=1 pointer=1 pack=1 codes=1 log=1 pages=0 manifest=0 other=0"
        );
        // The kinds of formats 1 to 4, and keys of no kind.
        let s = MemStore::new();
        for k in [
            "a/pages/0000000002/0000000001",
            "a/manifest/0000000002",
            "a/notes.txt",
            "head-not",
        ] {
            s.put(k, b"x").unwrap();
        }
        let r = scan_in(&s, "", &n, range).unwrap();
        assert_eq!(
            r.kinds,
            Kinds {
                pages: 1,
                manifest: 1,
                other: 2,
                ..Kinds::default()
            }
        );
    }

    fn db_config(dir: &Path, key: Option<&Path>) -> buckwal::Config {
        let mut c = buckwal::Config::new(dir, "http://127.0.0.1:1", "bucket", "ak", "sk");
        c.prefix = "t".to_string();
        c.retry_deadline = std::time::Duration::from_secs(2);
        c.encryption_key_file = key.map(Path::to_path_buf);
        c
    }

    /// A database with a marker row, its generation ended, then one pack run: the pack holds
    /// the marker row's page.
    fn packed_bucket(key: Option<&Path>, marker: &str) -> std::sync::Arc<MemStore> {
        use buckwal::{CheckpointMode, Value};
        let mem = std::sync::Arc::new(MemStore::new());
        let t = tempfile::tempdir().unwrap();
        let store: std::sync::Arc<dyn ObjectStore> = mem.clone();
        let db =
            buckwal::open_with_store(db_config(&t.path().join("db"), key), store.clone()).unwrap();
        let c = db.connect().unwrap();
        c.execute("CREATE TABLE m(t TEXT)", &[]).unwrap();
        c.execute(
            "INSERT INTO m VALUES (?)",
            &[Value::build_text(marker.to_string())],
        )
        .unwrap();
        c.checkpoint(CheckpointMode::Truncate).unwrap();
        c.close().unwrap();
        db.close().unwrap();
        let r = buckwal::pack_with_store(&db_config(&t.path().join("packer"), key), store).unwrap();
        assert_eq!(r.outcome, buckwal::PackOutcome::Packed, "{r:?}");
        mem
    }

    /// `scan --all` over the packs of a bucket: a plaintext bucket's pack holds `CREATE TABLE`
    /// and the marker; an encrypted bucket's pack holds no needle, the key included.
    #[test]
    fn scan_all_finds_plaintext_in_plain_pack_and_none_in_encrypted_pack() {
        let marker = "scan-pack-marker-0123456789";
        let n = super::needles(&[marker.to_string()], None);
        let plain = packed_bucket(None, marker);
        let r = scan(plain.as_ref(), "", &n).unwrap();
        assert!(r.kinds.pack >= 1 && r.kinds.codes >= 1, "{r:?}");
        assert!(meets(Expect::Present, &r, &n), "{r:?}");
        // The pack itself holds the table's page and the marker row.
        let packs = scan(plain.as_ref(), "t/pack/", &n).unwrap();
        let found = |label: &str| {
            packs
                .hits
                .iter()
                .any(|(l, _, k)| l == label && k.starts_with("t/pack/") && !k.ends_with(".codes"))
        };
        assert!(found("CREATE TABLE") && found("marker 1"), "{packs:?}");

        let keydir = tempfile::tempdir().unwrap();
        let key_file = keydir.path().join("key");
        std::fs::write(&key_file, "5a".repeat(32)).unwrap();
        let n = super::needles(&[marker.to_string()], Some(&KEY));
        let enc = packed_bucket(Some(&key_file), marker);
        let r = scan(enc.as_ref(), "", &n).unwrap();
        assert!(r.kinds.pack >= 1 && r.kinds.codes >= 1, "{r:?}");
        assert_eq!(r.hits, vec![], "an encrypted bucket holds no plaintext");
        assert!(meets(Expect::Absent, &r, &n));
    }

    #[test]
    fn scan_reports_each_needle() {
        let s = MemStore::new();
        let mut a = noise(1, 300);
        a.splice(100..100, b"CREATE TABLE t(x)".iter().copied());
        let mut b = noise(2, 300);
        b.splice(250..250, [0xab; 64]);
        s.put("p/a", &a).unwrap();
        s.put("p/b", &b).unwrap();
        s.put("p/c", &noise(3, 100)).unwrap();
        // Outside the prefix: not read.
        s.put("q/a", b"CREATE TABLE").unwrap();
        let r = scan(&s, "p/", &needles(&KEY)).unwrap();
        assert_eq!((r.objects, r.bytes), (3, (a.len() + b.len() + 100) as u64));
        assert_eq!(
            r.hits,
            vec![
                ("CREATE TABLE".to_string(), 1, "p/a".to_string()),
                ("bench payload".to_string(), 1, "p/b".to_string()),
            ]
        );
        let lines = report_lines(&r, &needles(&KEY));
        assert_eq!(
            lines,
            [
                format!("scan: 3 objects, {} bytes", r.bytes),
                "scan kinds: head=0 pointer=0 pack=0 codes=0 log=0 pages=0 manifest=0 other=3"
                    .to_string(),
                "CREATE TABLE: found in 1 objects (first: p/a)".to_string(),
                "SQLite header: not found".to_string(),
                "bench payload: found in 1 objects (first: p/b)".to_string(),
                "the encryption key: not found".to_string(),
            ]
        );
    }

    #[test]
    fn scan_absent_on_clean_store() {
        let s = MemStore::new();
        for i in 0..20u64 {
            s.put(&format!("p/{i:02}"), &noise(i + 10, 4096)).unwrap();
        }
        let r = scan(&s, "", &needles(&KEY)).unwrap();
        assert_eq!((r.objects, r.bytes), (20, 20 * 4096));
        assert_eq!(r.hits, vec![]);
        // An empty store is a clean one too.
        let r = scan(&MemStore::new(), "", &needles(&KEY)).unwrap();
        assert_eq!(r, ScanReport::default());
    }

    #[test]
    fn scan_never_prints_key() {
        let hex: String = KEY.iter().map(|b| format!("{b:02x}")).collect();
        let s = MemStore::new();
        s.put("k/lower", format!("key={hex}\n").as_bytes()).unwrap();
        s.put("k/upper", hex.to_uppercase().as_bytes()).unwrap();
        s.put("k/raw", &KEY).unwrap();
        let r = scan(&s, "k/", &needles(&KEY)).unwrap();
        assert_eq!(
            r.hits,
            vec![("the encryption key".to_string(), 3, "k/lower".to_string())]
        );
        let text = format!("{r:?}\n{}", report_lines(&r, &needles(&KEY)).join("\n"));
        assert!(
            text.contains("the encryption key: found in 3 objects (first: k/lower)"),
            "{text}"
        );
        assert!(!text.to_lowercase().contains(&hex), "{text}");
        assert!(
            !text.contains("5a, 5a") && !text.contains("90, 90"),
            "{text}"
        );
    }

    /// `present` needs `CREATE TABLE` and every marker, and fails on the key; `absent`
    /// fails on any label.
    #[test]
    fn expectations() {
        let n = super::needles(&["m".to_string()], Some(&KEY));
        let hit = |labels: &[&str]| ScanReport {
            objects: 1,
            bytes: 1,
            kinds: Kinds::default(),
            hits: labels
                .iter()
                .map(|l| (l.to_string(), 1, "k".to_string()))
                .collect(),
        };
        assert!(meets(Expect::Absent, &hit(&[]), &n));
        assert!(!meets(Expect::Absent, &hit(&["marker 1"]), &n));
        assert!(meets(
            Expect::Present,
            &hit(&["CREATE TABLE", "marker 1"]),
            &n
        ));
        assert!(!meets(Expect::Present, &hit(&["CREATE TABLE"]), &n));
        assert!(!meets(Expect::Present, &hit(&["marker 1"]), &n));
        let with_key = hit(&["CREATE TABLE", "marker 1", "the encryption key"]);
        assert!(!meets(Expect::Present, &with_key, &n));
    }

    #[test]
    fn key_file_text_is_hex_of_either_case() {
        assert_eq!(key_bytes(b" 0aFf\n"), Some(vec![0x0a, 0xff]));
        for bad in [&b""[..], b"0", b"0g", b"0a 0b"] {
            assert_eq!(key_bytes(bad), None, "{bad:?}");
        }
    }
}
