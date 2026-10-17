// Derived from tursodatabase/turso spike/objstore (MIT), commit 417bd81c0.
//! Generic S3 client: SigV4 over reqwest's blocking client. Endpoint, region, path style
//! and credentials all come from settings; nothing here is specific to RustFS.
use super::{CondPut, Object, ObjectStat, ObjectStore, StatsSnap, StoreError};
use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;

#[derive(Clone)]
pub struct S3Config {
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub path_style: bool,
    pub request_timeout: Duration,
    /// PEM file of the CAs that sign the endpoint's certificate; replaces the system trust store.
    pub ca_bundle: Option<std::path::PathBuf>,
}

/// `secret_key` is redacted, as in `Config`'s Debug: Debug output reaches logs, panics and
/// error messages.
impl std::fmt::Debug for S3Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Config")
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("bucket", &self.bucket)
            .field("access_key", &self.access_key)
            .field("secret_key", &"<redacted>")
            .field("path_style", &self.path_style)
            .field("request_timeout", &self.request_timeout)
            .field("ca_bundle", &self.ca_bundle)
            .finish()
    }
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
    last_modified: Option<String>,
    date: Option<String>,
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

/// `scheme://host[:port]` with the scheme in lower case, the host as reqwest sends it (its URL
/// parser's form: lower case, an IPv6 literal compressed), the port as a number (1-65535) and
/// a default port (443 for https, 80 for http) dropped, so the signed host header is what
/// clients send. An IPv6 host is a bracketed literal.
fn normalize_endpoint(endpoint: &str) -> crate::Result<String> {
    let bad = || {
        crate::Error::Config(format!(
            "endpoint {endpoint:?} must be http(s)://host[:port]"
        ))
    };
    let e = endpoint.trim_end_matches('/');
    let (scheme, authority) = e.split_once("://").ok_or_else(bad)?;
    let scheme = scheme.to_ascii_lowercase();
    if scheme != "http" && scheme != "https" {
        return Err(bad());
    }
    if authority.contains(['/', '?', '#', '@']) {
        return Err(bad());
    }
    let (host, port) = if let Some(rest) = authority.strip_prefix('[') {
        let (ip, after) = rest.split_once(']').ok_or_else(bad)?;
        let port = match after {
            "" => None,
            a => Some(a.strip_prefix(':').ok_or_else(bad)?),
        };
        (&authority[..ip.len() + 2], port)
    } else {
        match authority.split_once(':') {
            Some((h, p)) => (h, Some(p)),
            None => (authority, None),
        }
    };
    if host.is_empty() {
        return Err(bad());
    }
    let host = reqwest::Url::parse(&format!("{scheme}://{host}"))
        .ok()
        .and_then(|u| u.host_str().map(str::to_string))
        .ok_or_else(bad)?;
    let port = match port {
        None => None,
        Some(p) if !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()) => {
            Some(p.parse::<u16>().ok().filter(|&n| n != 0).ok_or_else(bad)?)
        }
        Some(_) => return Err(bad()),
    };
    let default = if scheme == "https" { 443 } else { 80 };
    Ok(match port {
        Some(p) if p != default => format!("{scheme}://{host}:{p}"),
        _ => format!("{scheme}://{host}"),
    })
}

fn http_client(cfg: &S3Config) -> crate::Result<reqwest::blocking::Client> {
    // reqwest's rustls backend panics at build() without a process crypto provider; a host app's earlier install wins.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut b = reqwest::blocking::Client::builder().timeout(cfg.request_timeout);
    if let Some(path) = &cfg.ca_bundle {
        let shown = path.display();
        let pem = std::fs::read(path)
            .map_err(|e| crate::Error::Config(format!("CA bundle {shown}: {e}")))?;
        let certs = reqwest::Certificate::from_pem_bundle(&pem)
            .map_err(|e| crate::Error::Config(format!("CA bundle {shown}: {e}")))?;
        if certs.is_empty() {
            return Err(crate::Error::Config(format!(
                "CA bundle {shown} holds no PEM certificate"
            )));
        }
        b = b.tls_certs_only(certs);
    }
    b.build()
        .map_err(|e| crate::Error::Config(format!("http client: {e}")))
}

/// The certificate-trust failure in `e`'s cause chain, if any. `io::Error::source` skips
/// the error it wraps, so each `io::Error` is descended by `get_ref`.
fn tls_failure(e: &reqwest::Error) -> Option<String> {
    use rustls::Error as Tls;
    let mut cur: Option<&(dyn std::error::Error + 'static)> = Some(e);
    while let Some(err) = cur {
        let mut inner = Some(err);
        while let Some(i) = inner {
            if let Some(t) = i.downcast_ref::<Tls>() {
                if matches!(
                    t,
                    Tls::InvalidCertificate(_)
                        | Tls::NoCertificatesPresented
                        | Tls::InvalidCertRevocationList(_)
                ) {
                    return Some(t.to_string());
                }
            }
            inner = i
                .downcast_ref::<std::io::Error>()
                .and_then(|io| io.get_ref())
                .map(|g| g as &(dyn std::error::Error + 'static));
        }
        cur = err.source();
    }
    None
}

impl S3Store {
    pub fn new(cfg: S3Config) -> crate::Result<Self> {
        let mut cfg = cfg;
        cfg.endpoint = normalize_endpoint(&cfg.endpoint)?;
        let http = http_client(&cfg)?;
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
        let net = |e: reqwest::Error| {
            match tls_failure(&e) {
            Some(why) => StoreError::Permanent(format!(
                "{method} {key}: TLS certificate not trusted: {why}; set DURABLE_CA_BUNDLE to the CA that signed the endpoint's certificate"
            )),
            None => StoreError::Transient(format!("{method} {key}: {e}")),
        }
        };
        let resp = req.body(body.to_vec()).send().map_err(net)?;
        let status = resp.status().as_u16();
        // The bucket span this request runs in shows its last response's status.
        tracing::Span::current().record("s3.status", status);
        let etag = resp
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let header = |name: &str| {
            resp.headers()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string)
        };
        let last_modified = header("last-modified");
        let date = header("date");
        let body = resp.bytes().map_err(net)?.to_vec();
        Ok(Reply {
            status,
            etag,
            last_modified,
            date,
            body,
        })
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

/// Seconds since the epoch of a proleptic Gregorian date (days-from-civil).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// An HTTP IMF-fixdate (`Sun, 06 Nov 1994 08:49:37 GMT`); nothing else, `GMT` only.
pub(crate) fn parse_http_date(s: &str) -> Option<std::time::SystemTime> {
    const DAYS: [&str; 7] = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let parts: Vec<&str> = s.split(' ').collect();
    let [dow, day, mon, year, time, tz] = parts[..] else {
        return None;
    };
    if !DAYS.contains(&dow.strip_suffix(',')?) || tz != "GMT" {
        return None;
    }
    let num = |t: &str, len: usize| -> Option<i64> {
        (t.len() == len && t.bytes().all(|b| b.is_ascii_digit()))
            .then(|| t.parse().ok())
            .flatten()
    };
    let day = num(day, 2)?;
    let month = MONTHS.iter().position(|m| *m == mon)? as i64 + 1;
    let year = num(year, 4)?;
    let hms: Vec<&str> = time.split(':').collect();
    let [h, mi, se] = hms[..] else { return None };
    let (h, mi, se) = (num(h, 2)?, num(mi, 2)?, num(se, 2)?);
    if !(1..=31).contains(&day) || h > 23 || mi > 59 || se > 60 {
        return None;
    }
    let secs = days_from_civil(year, month, day) * 86_400 + h * 3600 + mi * 60 + se;
    u64::try_from(secs)
        .ok()
        .map(|s| std::time::UNIX_EPOCH + Duration::from_secs(s))
}

/// The reply to a HEAD: 404 is a missing key; the age is `Date - Last-Modified`, both from
/// the store, so the caller's clock does not enter. Either header missing is permanent.
fn stat_reply(what: &str, r: &Reply) -> Result<Option<ObjectStat>, StoreError> {
    match r.status {
        200 => {}
        404 => return Ok(None),
        _ => return Err(status_error(what, r)),
    }
    let date_of = |name: &str, v: &Option<String>| {
        let v = v
            .as_deref()
            .ok_or_else(|| StoreError::Permanent(format!("{what}: no {name} header")))?;
        parse_http_date(v)
            .ok_or_else(|| StoreError::Permanent(format!("{what}: bad {name} header {v:?}")))
    };
    let etag = r
        .etag
        .clone()
        .ok_or_else(|| StoreError::Permanent(format!("{what}: no ETag")))?;
    let modified = date_of("Last-Modified", &r.last_modified)?;
    let now = date_of("Date", &r.date)?;
    let age = now.duration_since(modified).unwrap_or(Duration::ZERO);
    Ok(Some(ObjectStat { etag, age }))
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

    /// A signed HEAD. `Last-Modified` and `Date` have 1 s resolution, so the age can exceed
    /// the truth by just under 1 s. A 404 is `None` (a HEAD has no body to tell a missing
    /// key from a missing bucket): unknown, never old.
    fn stat(&self, key: &str) -> Result<Option<ObjectStat>, StoreError> {
        let r = self.send("HEAD", key, &[], &[], &[])?;
        self.gets.fetch_add(1, Relaxed);
        stat_reply(&format!("HEAD {key}"), &r)
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
            last_modified: None,
            date: None,
            body: body.as_bytes().to_vec(),
        }
    }

    #[test]
    fn parses_imf_fixdate() {
        use std::time::{Duration, UNIX_EPOCH};
        assert_eq!(
            parse_http_date("Sun, 06 Nov 1994 08:49:37 GMT"),
            Some(UNIX_EPOCH + Duration::from_secs(784_111_777))
        );
        assert_eq!(
            parse_http_date("Thu, 01 Jan 1970 00:00:10 GMT"),
            Some(UNIX_EPOCH + Duration::from_secs(10))
        );
        for bad in ["06 Nov 1994", "Sun, 06 Nov 1994 08:49:37 PST", ""] {
            assert_eq!(parse_http_date(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn stat_age_is_date_minus_last_modified() {
        let mut r = reply(200, Some("\"e\""), "");
        r.last_modified = Some("Sun, 06 Nov 1994 08:49:37 GMT".into());
        r.date = Some("Sun, 06 Nov 1994 08:50:07 GMT".into());
        let st = stat_reply("HEAD k", &r).unwrap().unwrap();
        assert_eq!(st.etag, "\"e\"");
        assert_eq!(st.age, std::time::Duration::from_secs(30));
        // A clock skew that puts Last-Modified after Date saturates at 0.
        std::mem::swap(&mut r.last_modified, &mut r.date);
        assert_eq!(
            stat_reply("HEAD k", &r).unwrap().unwrap().age,
            std::time::Duration::ZERO
        );
        assert!(stat_reply("HEAD k", &reply(404, None, ""))
            .unwrap()
            .is_none());
        r.date = None;
        assert!(matches!(
            stat_reply("HEAD k", &r),
            Err(StoreError::Permanent(_))
        ));
        assert!(matches!(
            stat_reply("HEAD k", &reply(503, None, "")),
            Err(StoreError::Transient(_))
        ));
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

    /// The secret key never reaches `Debug` (logs, panics, error output); the rest does.
    #[test]
    fn s3_config_debug_redacts_the_secret_key() {
        let cfg = S3Config {
            endpoint: "https://s3.example.com".into(),
            region: "eu-west-2".into(),
            bucket: "bkt".into(),
            access_key: "AKIAEXAMPLEACCESS".into(),
            secret_key: "s3cr3t/SECRET+value".into(),
            path_style: true,
            request_timeout: Duration::from_secs(5),
            ca_bundle: Some("/etc/ca.pem".into()),
        };
        for debug in [format!("{cfg:?}"), format!("{cfg:#?}")] {
            assert!(!debug.contains("s3cr3t"), "{debug}");
            assert!(debug.contains("<redacted>"), "{debug}");
            for shown in [
                "https://s3.example.com",
                "eu-west-2",
                "bkt",
                "AKIAEXAMPLEACCESS",
            ] {
                assert!(debug.contains(shown), "{shown}: {debug}");
            }
            assert!(debug.contains("/etc/ca.pem"), "{debug}");
        }
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
            ca_bundle: None,
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
            ca_bundle: None,
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

    fn cfg_for(endpoint: &str) -> S3Config {
        S3Config {
            endpoint: endpoint.into(),
            region: "us-east-1".into(),
            bucket: "b".into(),
            access_key: "k".into(),
            secret_key: "s".into(),
            path_style: false,
            request_timeout: Duration::from_secs(5),
            ca_bundle: None,
        }
    }

    #[test]
    fn https_endpoint_builds_a_client() {
        assert!(S3Store::new(cfg_for("https://s3.example.com")).is_ok());
    }

    #[test]
    fn default_ports_are_dropped_from_the_signed_host() {
        let host = |endpoint: &str, path_style: bool| {
            let mut cfg = cfg_for(endpoint);
            cfg.path_style = path_style;
            let store = S3Store::new(cfg).unwrap();
            object_url(&store.cfg, "k").1
        };
        assert_eq!(host("https://s3.example.com:443", true), "s3.example.com");
        assert_eq!(host("HTTP://h:80/", true), "h");
        assert_eq!(host("https://h:8443", true), "h:8443");
        assert_eq!(
            host("https://s3.example.com:443", false),
            "b.s3.example.com"
        );
        assert_eq!(
            object_url(&S3Store::new(cfg_for("HTTP://h:80/")).unwrap().cfg, "k").0,
            "http://b.h/k"
        );
    }

    /// The signed host is the host reqwest sends: its URL parser's form of the host (lower
    /// case, an IPv6 literal compressed, an IPv4 address dotted in full), and the port as a
    /// number with the default dropped.
    #[test]
    fn signed_host_is_the_host_reqwest_sends() {
        for (endpoint, want) in [
            ("https://S3.Example.COM", "s3.example.com"),
            ("HTTP://H:9000/", "h:9000"),
            ("https://h:08443", "h:8443"),
            ("https://h:0443", "h"),
            ("http://[::1]", "[::1]"),
            ("http://[::1]:9000", "[::1]:9000"),
            ("http://[::1]:80", "[::1]"),
            ("https://[::ABCD]:9000", "[::abcd]:9000"),
            ("http://[0:0::1]:9000", "[::1]:9000"),
            ("http://127.1:9000", "127.0.0.1:9000"),
        ] {
            let store = S3Store::new(cfg_for(endpoint)).unwrap();
            let mut cfg = store.cfg.clone();
            cfg.path_style = true;
            let (url, host, _) = object_url(&cfg, "k");
            assert_eq!(host, want, "{endpoint}");
            let u = reqwest::Url::parse(&url).unwrap();
            let sent = match u.port() {
                Some(p) => format!("{}:{p}", u.host_str().unwrap()),
                None => u.host_str().unwrap().to_string(),
            };
            assert_eq!(sent, host, "{endpoint}: reqwest sends another host");
        }
    }

    #[test]
    fn endpoint_with_a_bad_port_or_host_is_refused() {
        for e in [
            "https://h:",
            "https://h:abc",
            "https://h:+80",
            "https://h:0",
            "https://h:65536",
            "https://a:b:9000",
            "https://[::1]:",
            "https://[::1]x",
            "https://[::1",
            "https://[zz]",
            "https://[]",
            "https://h]",
            "https://u@h",
        ] {
            match S3Store::new(cfg_for(e)) {
                Err(crate::Error::Config(m)) => {
                    assert!(m.contains("must be http(s)://host[:port]"), "{e}: {m}")
                }
                r => panic!("{e}: {:?}", r.map(|s| s.cfg.endpoint)),
            }
        }
    }

    #[test]
    fn endpoint_with_a_path_is_refused() {
        for e in [
            "https://h/x",
            "https://h?q=1",
            "https://h#f",
            "https://",
            "ftp://h",
            "h:9000",
        ] {
            assert!(
                matches!(S3Store::new(cfg_for(e)), Err(crate::Error::Config(_))),
                "{e}"
            );
        }
    }

    #[test]
    fn ca_bundle_must_hold_a_certificate() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = cfg_for("https://s3.example.com");

        let missing = dir.path().join("missing.pem");
        cfg.ca_bundle = Some(missing.clone());
        match S3Store::new(cfg.clone()) {
            Err(crate::Error::Config(m)) => {
                assert!(m.contains(missing.to_str().unwrap()), "{m}")
            }
            r => panic!("{:?}", r.err()),
        }

        for (name, body) in [("empty.pem", &b""[..]), ("junk.pem", &b"not pem"[..])] {
            let f = dir.path().join(name);
            std::fs::write(&f, body).unwrap();
            cfg.ca_bundle = Some(f);
            match S3Store::new(cfg.clone()) {
                Err(crate::Error::Config(m)) => assert!(m.contains("no PEM certificate"), "{m}"),
                r => panic!("{name}: {:?}", r.err()),
            }
        }
    }
}
