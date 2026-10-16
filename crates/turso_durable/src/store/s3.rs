// Derived from tursodatabase/turso spike/objstore (MIT), commit 417bd81c0.
//! Generic S3 client: SigV4 over reqwest's blocking client. Endpoint, region, path style
//! and credentials all come from settings; nothing here is specific to RustFS.
use super::{CondPut, Object, ObjectStore, StatsSnap, StoreError};
use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct S3Config {
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub path_style: bool,
    pub request_timeout: Duration,
}

pub struct S3Store {
    cfg: S3Config,
    http: reqwest::blocking::Client,
    puts: AtomicU64,
    gets: AtomicU64,
    lists: AtomicU64,
    deletes: AtomicU64,
    bytes_up: AtomicU64,
    bytes_down: AtomicU64,
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut m = <Hmac<Sha256> as KeyInit>::new_from_slice(key).expect("hmac takes any key length");
    m.update(data);
    m.finalize().into_bytes().to_vec()
}
fn sha256_hex(b: &[u8]) -> String {
    hex::encode(Sha256::digest(b))
}

/// RFC 3986 encoding as SigV4 wants it; `/` is kept in paths and encoded in query values.
pub(crate) fn uri_encode(s: &str, encode_slash: bool) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b'/' if !encode_slash => out.push('/'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// `YYYYMMDDTHHMMSSZ` for a Unix time, UTC (days-to-civil from H. Hinnant).
pub(crate) fn amz_date(unix: u64) -> String {
    let (days, secs) = ((unix / 86_400) as i64, unix % 86_400);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + if m <= 2 { 1 } else { 0 };
    format!(
        "{y:04}{m:02}{d:02}T{:02}{:02}{:02}Z",
        secs / 3600,
        secs / 60 % 60,
        secs % 60
    )
}

/// The `Authorization` header. `headers` are (lowercase name, value) and must include
/// host, x-amz-content-sha256 and x-amz-date.
pub(crate) fn authorization(
    cfg: &S3Config,
    method: &str,
    canonical_uri: &str,
    canonical_query: &str,
    headers: &[(String, String)],
    payload_hash: &str,
    amz_date: &str,
) -> String {
    let mut hs = headers.to_vec();
    hs.sort();
    let canonical_headers: String = hs
        .iter()
        .map(|(k, v)| format!("{k}:{}\n", v.trim()))
        .collect();
    let signed: Vec<&str> = hs.iter().map(|(k, _)| k.as_str()).collect();
    let signed = signed.join(";");
    let creq = format!("{method}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed}\n{payload_hash}");
    let date = &amz_date[..8];
    let scope = format!("{date}/{}/s3/aws4_request", cfg.region);
    let sts = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha256_hex(creq.as_bytes())
    );
    let k = hmac(
        format!("AWS4{}", cfg.secret_key).as_bytes(),
        date.as_bytes(),
    );
    let k = hmac(&k, cfg.region.as_bytes());
    let k = hmac(&k, b"s3");
    let k = hmac(&k, b"aws4_request");
    let sig = hex::encode(hmac(&k, sts.as_bytes()));
    format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope},SignedHeaders={signed},Signature={sig}",
        cfg.access_key
    )
}

/// (url, host header, canonical uri) for a key ("" addresses the bucket itself).
pub(crate) fn object_url(cfg: &S3Config, key: &str) -> (String, String, String) {
    let (scheme, authority) = cfg
        .endpoint
        .split_once("://")
        .expect("endpoint has a scheme (checked in S3Store::new)");
    let path_key = uri_encode(key, false);
    if cfg.path_style {
        let uri = if key.is_empty() {
            format!("/{}", cfg.bucket)
        } else {
            format!("/{}/{path_key}", cfg.bucket)
        };
        (
            format!("{scheme}://{authority}{uri}"),
            authority.to_string(),
            uri,
        )
    } else {
        let host = format!("{}.{authority}", cfg.bucket);
        let uri = format!("/{path_key}");
        (format!("{scheme}://{host}{uri}"), host, uri)
    }
}

pub(crate) struct ListPage {
    pub keys: Vec<String>,
    pub next: Option<String>,
}

fn between<'a>(s: &'a str, open: &str, close: &str) -> Vec<&'a str> {
    let mut out = Vec::new();
    let mut rest = s;
    while let Some(i) = rest.find(open) {
        let after = &rest[i + open.len()..];
        let Some(j) = after.find(close) else { break };
        out.push(&after[..j]);
        rest = &after[j + close.len()..];
    }
    out
}

/// Undo XML escaping in element text: the five predefined entities and numeric character
/// references (`&#NN;`, `&#xHH;`). Anything else (an unknown or malformed reference, a bare
/// `&`) is kept as it is.
fn xml_unescape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(i) = rest.find('&') {
        out.push_str(&rest[..i]);
        rest = &rest[i..];
        let decoded = rest.find(';').and_then(|j| {
            let c = match &rest[1..j] {
                "amp" => '&',
                "lt" => '<',
                "gt" => '>',
                "quot" => '"',
                "apos" => '\'',
                e => {
                    let n = match e.strip_prefix("#x").or_else(|| e.strip_prefix("#X")) {
                        Some(hex) => u32::from_str_radix(hex, 16).ok()?,
                        None => e.strip_prefix('#')?.parse().ok()?,
                    };
                    char::from_u32(n)?
                }
            };
            Some((c, j + 1))
        });
        match decoded {
            Some((c, len)) => {
                out.push(c);
                rest = &rest[len..];
            }
            None => {
                out.push('&');
                rest = &rest[1..];
            }
        }
    }
    out.push_str(rest);
    out
}

pub(crate) fn parse_list(xml: &str) -> ListPage {
    let keys = between(xml, "<Key>", "</Key>")
        .into_iter()
        .map(xml_unescape)
        .collect();
    let truncated = between(xml, "<IsTruncated>", "</IsTruncated>").first() == Some(&"true");
    let next = if truncated {
        between(xml, "<NextContinuationToken>", "</NextContinuationToken>")
            .first()
            .map(|s| xml_unescape(s))
    } else {
        None
    };
    ListPage { keys, next }
}

struct Reply {
    status: u16,
    etag: Option<String>,
    body: Vec<u8>,
}

/// Map an unexpected HTTP status to the error class callers act on.
fn status_error(what: &str, r: &Reply) -> StoreError {
    let msg = format!(
        "{what}: HTTP {}: {}",
        r.status,
        String::from_utf8_lossy(&r.body)
    );
    match r.status {
        408 | 429 | 500..=599 => StoreError::Transient(msg),
        _ => StoreError::Permanent(msg),
    }
}

impl S3Store {
    pub fn new(cfg: S3Config) -> crate::Result<Self> {
        let mut cfg = cfg;
        cfg.endpoint = cfg.endpoint.trim_end_matches('/').to_string();
        match cfg.endpoint.split_once("://") {
            Some(("https", _)) => return Err(crate::Error::Unsupported("https")),
            Some(("http", a)) if !a.is_empty() => {}
            _ => {
                return Err(crate::Error::Config(format!(
                    "endpoint {:?} must be http://host[:port]",
                    cfg.endpoint
                )))
            }
        }
        let http = reqwest::blocking::Client::builder()
            .timeout(cfg.request_timeout)
            .build()
            .map_err(|e| crate::Error::Config(format!("http client: {e}")))?;
        Ok(Self {
            cfg,
            http,
            puts: AtomicU64::new(0),
            gets: AtomicU64::new(0),
            lists: AtomicU64::new(0),
            deletes: AtomicU64::new(0),
            bytes_up: AtomicU64::new(0),
            bytes_down: AtomicU64::new(0),
        })
    }

    fn count_put(&self, n: usize) {
        self.puts.fetch_add(1, Relaxed);
        self.bytes_up.fetch_add(n as u64, Relaxed);
    }

    /// One signed request. `extra` headers (conditionals) are sent but not signed.
    fn send(
        &self,
        method: &str,
        key: &str,
        query: &[(&str, String)],
        body: &[u8],
        extra: &[(&str, &str)],
    ) -> Result<Reply, StoreError> {
        let (url, host, uri) = object_url(&self.cfg, key);
        let mut q: Vec<(String, String)> = query
            .iter()
            .map(|(k, v)| (uri_encode(k, true), uri_encode(v, true)))
            .collect();
        q.sort();
        let cq = q
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| StoreError::Permanent(format!("clock before epoch: {e}")))?
            .as_secs();
        let date = amz_date(now);
        let ph = sha256_hex(body);
        let headers = vec![
            ("host".to_string(), host),
            ("x-amz-content-sha256".to_string(), ph.clone()),
            ("x-amz-date".to_string(), date.clone()),
        ];
        let auth = authorization(&self.cfg, method, &uri, &cq, &headers, &ph, &date);
        let full = if cq.is_empty() {
            url
        } else {
            format!("{url}?{cq}")
        };
        let m = reqwest::Method::from_bytes(method.as_bytes())
            .map_err(|e| StoreError::Permanent(format!("bad method {method}: {e}")))?;
        let mut req = self
            .http
            .request(m, full)
            .header("x-amz-content-sha256", ph)
            .header("x-amz-date", date)
            .header("authorization", auth);
        for (k, v) in extra {
            req = req.header(*k, *v);
        }
        let net = |e: reqwest::Error| StoreError::Transient(format!("{method} {key}: {e}"));
        let resp = req.body(body.to_vec()).send().map_err(net)?;
        let status = resp.status().as_u16();
        // The bucket span this request runs in shows its last response's status.
        tracing::Span::current().record("s3.status", status);
        let etag = resp
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let body = resp.bytes().map_err(net)?.to_vec();
        Ok(Reply { status, etag, body })
    }

    /// Creates the bucket; an existing bucket we own is fine.
    pub fn create_bucket(&self) -> crate::Result<()> {
        let r = self
            .send("PUT", "", &[], &[], &[])
            .map_err(|e| crate::Error::Bucket(e.to_string()))?;
        let body = String::from_utf8_lossy(&r.body);
        if r.status == 200 || (r.status == 409 && body.contains("BucketAlreadyOwnedByYou")) {
            return Ok(());
        }
        Err(crate::Error::Bucket(format!(
            "create bucket: HTTP {}: {body}",
            r.status
        )))
    }

    fn put_with(
        &self,
        key: &str,
        body: &[u8],
        extra: &[(&str, &str)],
    ) -> Result<Reply, StoreError> {
        self.count_put(body.len());
        self.send("PUT", key, &[], body, extra)
    }
}

/// The `<Code>` of an S3 error response body, if it has one.
fn error_code(body: &[u8]) -> Option<String> {
    let body = String::from_utf8_lossy(body);
    between(&body, "<Code>", "</Code>")
        .first()
        .map(|c| c.trim().to_string())
}

/// The reply to a conditional PUT (`If-None-Match: *` or `If-Match`).
fn cond_put_reply(what: &str, r: &Reply) -> Result<CondPut, StoreError> {
    match (r.status, r.etag.clone()) {
        (200, Some(etag)) => Ok(CondPut::Created { etag }),
        (200, None) => Err(StoreError::Permanent(format!(
            "{what}: no ETag in response"
        ))),
        (412, _) => Ok(CondPut::PreconditionFailed),
        // A concurrent conditional write in flight; the caller may retry.
        (409, _) => Err(StoreError::Transient(format!("{what}: HTTP 409 conflict"))),
        _ => Err(status_error(what, r)),
    }
}

/// The reply to a PUT with `If-Match` in `bucket`. RustFS answers 404 `NoSuchKey` (not 412)
/// when the key is missing: the precondition is not met. Any other 404 is permanent: a
/// `NoSuchBucket` names the missing bucket, and a 404 with no recognisable code (a proxy, a
/// wrong endpoint or addressing style) is no evidence that the key is gone. Reporting either
/// as `PreconditionFailed` would turn a configuration error into `Fenced`, which sticks.
fn if_match_reply(what: &str, bucket: &str, r: &Reply) -> Result<CondPut, StoreError> {
    if r.status != 404 {
        return cond_put_reply(what, r);
    }
    match error_code(&r.body).as_deref() {
        Some("NoSuchKey") => Ok(CondPut::PreconditionFailed),
        Some("NoSuchBucket") => Err(StoreError::Permanent(format!(
            "{what}: bucket {bucket:?} does not exist (HTTP 404 NoSuchBucket)"
        ))),
        _ => Err(status_error(what, r)),
    }
}

impl ObjectStore for S3Store {
    fn put(&self, key: &str, body: &[u8]) -> Result<String, StoreError> {
        let r = self.put_with(key, body, &[])?;
        match (r.status, r.etag.clone()) {
            (200, Some(etag)) => Ok(etag),
            (200, None) => Err(StoreError::Permanent(format!(
                "PUT {key}: no ETag in response"
            ))),
            _ => Err(status_error(&format!("PUT {key}"), &r)),
        }
    }

    fn put_if_absent(&self, key: &str, body: &[u8]) -> Result<CondPut, StoreError> {
        let r = self.put_with(key, body, &[("if-none-match", "*")])?;
        cond_put_reply(&format!("PUT {key} (if-none-match)"), &r)
    }

    fn put_if_match(&self, key: &str, body: &[u8], etag: &str) -> Result<CondPut, StoreError> {
        let r = self.put_with(key, body, &[("if-match", etag)])?;
        if_match_reply(&format!("PUT {key} (if-match)"), &self.cfg.bucket, &r)
    }

    fn get(&self, key: &str) -> Result<Option<Object>, StoreError> {
        let r = self.send("GET", key, &[], &[], &[])?;
        self.gets.fetch_add(1, Relaxed);
        match r.status {
            200 => {
                self.bytes_down.fetch_add(r.body.len() as u64, Relaxed);
                let etag = r
                    .etag
                    .ok_or_else(|| StoreError::Permanent(format!("GET {key}: no ETag")))?;
                Ok(Some(Object { body: r.body, etag }))
            }
            404 if String::from_utf8_lossy(&r.body).contains("NoSuchKey") => Ok(None),
            _ => Err(status_error(&format!("GET {key}"), &r)),
        }
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        let mut keys = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut q = vec![
                ("list-type", "2".to_string()),
                ("prefix", prefix.to_string()),
            ];
            if let Some(t) = &token {
                q.push(("continuation-token", t.clone()));
            }
            let r = self.send("GET", "", &q, &[], &[])?;
            self.lists.fetch_add(1, Relaxed);
            if r.status != 200 {
                return Err(status_error(&format!("LIST {prefix}"), &r));
            }
            let page = parse_list(&String::from_utf8_lossy(&r.body));
            keys.extend(page.keys);
            match page.next {
                Some(t) => token = Some(t),
                None => break,
            }
        }
        keys.sort();
        Ok(keys)
    }

    fn delete(&self, key: &str) -> Result<(), StoreError> {
        let r = self.send("DELETE", key, &[], &[], &[])?;
        self.deletes.fetch_add(1, Relaxed);
        match r.status {
            200 | 204 | 404 => Ok(()),
            _ => Err(status_error(&format!("DELETE {key}"), &r)),
        }
    }

    fn stats(&self) -> StatsSnap {
        StatsSnap {
            puts: self.puts.load(Relaxed),
            gets: self.gets.load(Relaxed),
            lists: self.lists.load(Relaxed),
            deletes: self.deletes.load(Relaxed),
            bytes_up: self.bytes_up.load(Relaxed),
            bytes_down: self.bytes_down.load(Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reply(status: u16, etag: Option<&str>, body: &str) -> Reply {
        Reply {
            status,
            etag: etag.map(str::to_string),
            body: body.as_bytes().to_vec(),
        }
    }

    fn s3_error(code: &str) -> String {
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>{code}</Code>\
             <Message>m</Message><Resource>/b/k</Resource></Error>"
        )
    }

    /// Item 4: only a missing key is a failed `If-Match`; a missing bucket (or a 404 that
    /// names no recognisable code) is a permanent error, never `PreconditionFailed`.
    #[test]
    fn if_match_404_maps_by_error_code() {
        let what = "PUT k (if-match)";
        assert!(matches!(
            if_match_reply(what, "b", &reply(404, None, &s3_error("NoSuchKey"))),
            Ok(CondPut::PreconditionFailed)
        ));
        match if_match_reply(what, "b", &reply(404, None, &s3_error("NoSuchBucket"))) {
            Err(StoreError::Permanent(m)) => {
                assert!(
                    m.contains("bucket \"b\"") && m.contains("NoSuchBucket"),
                    "{m}"
                )
            }
            r => panic!("NoSuchBucket: {r:?}"),
        }
        for body in ["", "not found", &s3_error("AccessDenied")] {
            let r = if_match_reply(what, "b", &reply(404, None, body));
            assert!(
                matches!(r, Err(StoreError::Permanent(_))),
                "{body:?}: {r:?}"
            );
        }
        assert!(matches!(
            if_match_reply(what, "b", &reply(200, Some("\"e\""), "")),
            Ok(CondPut::Created { etag }) if etag == "\"e\""
        ));
        assert!(matches!(
            if_match_reply(
                what,
                "b",
                &reply(412, None, &s3_error("PreconditionFailed"))
            ),
            Ok(CondPut::PreconditionFailed)
        ));
    }

    #[test]
    fn conditional_put_status_mapping() {
        let what = "PUT k";
        assert!(matches!(
            cond_put_reply(what, &reply(200, Some("e"), "")),
            Ok(CondPut::Created { etag }) if etag == "e"
        ));
        assert!(matches!(
            cond_put_reply(what, &reply(200, None, "")),
            Err(StoreError::Permanent(_))
        ));
        assert!(matches!(
            cond_put_reply(what, &reply(412, None, "")),
            Ok(CondPut::PreconditionFailed)
        ));
        for status in [409, 408, 429, 500, 503] {
            assert!(
                matches!(
                    cond_put_reply(what, &reply(status, None, "")),
                    Err(StoreError::Transient(_))
                ),
                "{status}"
            );
        }
        for status in [400, 403, 404] {
            assert!(
                matches!(
                    cond_put_reply(what, &reply(status, None, "")),
                    Err(StoreError::Permanent(_))
                ),
                "{status}"
            );
        }
    }

    #[test]
    fn error_code_is_read_from_the_body() {
        assert_eq!(
            error_code(s3_error("NoSuchKey").as_bytes()),
            Some("NoSuchKey".into())
        );
        assert_eq!(error_code(b"plain"), None);
    }

    // AWS SigV4 documentation, "Example: GET Object" (s3, us-east-1, 2013-05-24).
    #[test]
    fn sigv4_matches_aws_example() {
        let cfg = S3Config {
            endpoint: "https://examplebucket.s3.amazonaws.com".into(),
            region: "us-east-1".into(),
            bucket: "examplebucket".into(),
            access_key: "AKIAIOSFODNN7EXAMPLE".into(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
            path_style: false,
            request_timeout: Duration::from_secs(5),
        };
        let empty = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let headers = vec![
            (
                "host".to_string(),
                "examplebucket.s3.amazonaws.com".to_string(),
            ),
            ("range".to_string(), "bytes=0-9".to_string()),
            ("x-amz-content-sha256".to_string(), empty.to_string()),
            ("x-amz-date".to_string(), "20130524T000000Z".to_string()),
        ];
        let auth = authorization(
            &cfg,
            "GET",
            "/test.txt",
            "",
            &headers,
            empty,
            "20130524T000000Z",
        );
        assert!(
            auth.ends_with(
                "Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
            ),
            "{auth}"
        );
        assert!(auth.contains("SignedHeaders=host;range;x-amz-content-sha256;x-amz-date"));
    }

    #[test]
    fn amz_date_formats_utc() {
        assert_eq!(amz_date(1_369_353_600), "20130524T000000Z");
        assert_eq!(amz_date(1_760_486_399), "20251014T235959Z");
    }

    #[test]
    fn uri_encode_query_values() {
        assert_eq!(uri_encode("p1/run-1/", true), "p1%2Frun-1%2F");
        assert_eq!(uri_encode("a+b=c~", true), "a%2Bb%3Dc~");
        assert_eq!(uri_encode("p1/run-1/x", false), "p1/run-1/x");
    }

    #[test]
    fn parse_list_truncated() {
        let xml = "<ListBucketResult><IsTruncated>true</IsTruncated>\
            <Contents><Key>p/1</Key></Contents><Contents><Key>p/2</Key></Contents>\
            <NextContinuationToken>tok/en+=</NextContinuationToken></ListBucketResult>";
        let page = parse_list(xml);
        assert_eq!(page.keys, vec!["p/1".to_string(), "p/2".to_string()]);
        assert_eq!(page.next.as_deref(), Some("tok/en+="));
        let last =
            parse_list("<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>");
        assert!(last.keys.is_empty() && last.next.is_none());
    }

    /// ListObjectsV2 escapes XML special characters in `<Key>` (and in the token): they are
    /// unescaped, so a key comes back exactly as it was written.
    #[test]
    fn parse_list_unescapes_xml() {
        let xml = "<ListBucketResult><IsTruncated>true</IsTruncated>\
            <Contents><Key>p&amp;q/&lt;x&gt;&quot;&apos;&#65;&#x42;&#x3c;/1</Key></Contents>\
            <Contents><Key>a&amp;amp;b &unknown; &#xZZ; &</Key></Contents>\
            <NextContinuationToken>t&amp;k</NextContinuationToken></ListBucketResult>";
        let page = parse_list(xml);
        assert_eq!(
            page.keys,
            vec![
                "p&q/<x>\"'AB</1".to_string(),
                "a&amp;b &unknown; &#xZZ; &".to_string()
            ]
        );
        assert_eq!(page.next.as_deref(), Some("t&k"));
    }

    #[test]
    fn urls_path_and_virtual_style() {
        let mut cfg = S3Config {
            endpoint: "http://127.0.0.1:9190".into(),
            region: "us-east-1".into(),
            bucket: "b".into(),
            access_key: "k".into(),
            secret_key: "s".into(),
            path_style: true,
            request_timeout: Duration::from_secs(5),
        };
        assert_eq!(
            object_url(&cfg, "p/1"),
            (
                "http://127.0.0.1:9190/b/p/1".to_string(),
                "127.0.0.1:9190".to_string(),
                "/b/p/1".to_string()
            )
        );
        cfg.path_style = false;
        cfg.endpoint = "https://s3.example.com".into();
        assert_eq!(
            object_url(&cfg, "p/1"),
            (
                "https://b.s3.example.com/p/1".to_string(),
                "b.s3.example.com".to_string(),
                "/p/1".to_string()
            )
        );
    }

    #[test]
    fn https_endpoint_is_unsupported() {
        let cfg = S3Config {
            endpoint: "https://s3.example.com".into(),
            region: "us-east-1".into(),
            bucket: "b".into(),
            access_key: "k".into(),
            secret_key: "s".into(),
            path_style: false,
            request_timeout: Duration::from_secs(5),
        };
        assert!(matches!(
            S3Store::new(cfg),
            Err(crate::Error::Unsupported("https"))
        ));
    }
}
