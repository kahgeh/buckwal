// Derived from tursodatabase/turso spike/objstore (MIT), commit 417bd81c0.
//! Generic S3 client: SigV4 over reqwest's blocking client. Endpoint, region, path style
//! and credentials all come from settings; nothing here is specific to RustFS.
use super::{
    part_count, part_size_for, read_part, retry_store, sha256_of, CondPut, MultipartDone, Object,
    ObjectStat, ObjectStore, PartSource, RetryPolicy, StatsSnap, StoreError,
};
use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Mutex;
use std::time::Duration;
use std::time::SystemTime;

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
    /// How a multipart upload retries one request (a part, the start or the end).
    retry: RetryPolicy,
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

/// Standard base64 with padding: the form of the `x-amz-checksum-sha256` header.
fn base64(b: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(b.len().div_ceil(3) * 4);
    for chunk in b.chunks(3) {
        let n = chunk
            .iter()
            .enumerate()
            .fold(0u32, |n, (i, &x)| n | (x as u32) << (16 - 8 * i));
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(ALPHABET[(n >> (18 - 6 * i) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
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
    /// The `<Size>` of each entry, in the order of `keys`.
    pub sizes: Vec<u64>,
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
    let sizes = between(xml, "<Size>", "</Size>")
        .into_iter()
        .filter_map(|v| v.trim().parse().ok())
        .collect();
    ListPage { keys, sizes, next }
}

struct Reply {
    status: u16,
    etag: Option<String>,
    last_modified: Option<String>,
    date: Option<String>,
    /// `x-amz-request-id`: with the error `Code`, the only part of a failure reply that is logged.
    request_id: Option<String>,
    /// `Content-Length`: on a HEAD the size of the object.
    content_length: Option<u64>,
    /// `x-amz-checksum-sha256`: the checksum the server holds for a part.
    checksum_sha256: Option<String>,
    body: Vec<u8>,
}

/// What an error reply shows in an error text or a log line: the S3 `Code` and the request
/// id. The body can hold the access key id and the string to sign, so it is never printed.
fn error_detail(r: &Reply) -> String {
    let code = error_code(&r.body).unwrap_or_else(|| "no error code".to_string());
    match &r.request_id {
        Some(id) => format!("{code} (request id {id})"),
        None => code,
    }
}

/// Map an unexpected HTTP status to the error class callers act on. A 3xx (redirects are
/// not followed) is permanent.
fn status_error(what: &str, r: &Reply) -> StoreError {
    let note = match r.status {
        300..=399 => " (redirect not followed)",
        _ => "",
    };
    let msg = format!("{what}: HTTP {}{note}: {}", r.status, error_detail(r));
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
    // Redirects are never followed: reqwest's default follows up to 10, https to http
    // included, re-sends a PUT body on 307/308 and keeps `Authorization` on the same host
    // and port, and the redirect target's answer would be taken as the store's. A 3xx
    // reaches `status_error` as a permanent error.
    let mut b = reqwest::blocking::Client::builder()
        .timeout(cfg.request_timeout)
        .redirect(reqwest::redirect::Policy::none());
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

/// A CA bundle only applies to TLS; with an `http://` endpoint it would be silently ignored.
pub(crate) fn check_ca_bundle_scheme(
    endpoint: &str,
    ca_bundle: &Option<std::path::PathBuf>,
) -> crate::Result<()> {
    if ca_bundle.is_some()
        && endpoint
            .trim_start()
            .to_ascii_lowercase()
            .starts_with("http://")
    {
        return Err(crate::Error::Config(
            "BUCKWAL_CA_BUNDLE is set but the endpoint is http://".into(),
        ));
    }
    Ok(())
}

impl S3Store {
    pub fn new(cfg: S3Config) -> crate::Result<Self> {
        check_ca_bundle_scheme(&cfg.endpoint, &cfg.ca_bundle)?;
        let mut cfg = cfg;
        cfg.endpoint = normalize_endpoint(&cfg.endpoint)?;
        let http = http_client(&cfg)?;
        Ok(Self {
            cfg,
            http,
            retry: RetryPolicy::default(),
            puts: AtomicU64::new(0),
            gets: AtomicU64::new(0),
            lists: AtomicU64::new(0),
            deletes: AtomicU64::new(0),
            bytes_up: AtomicU64::new(0),
            bytes_down: AtomicU64::new(0),
        })
    }

    /// The policy a multipart upload retries each of its requests with.
    pub fn with_retry_policy(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    fn count_put(&self, n: usize) {
        self.puts.fetch_add(1, Relaxed);
        self.bytes_up.fetch_add(n as u64, Relaxed);
    }

    /// One signed request. `extra` headers are sent; those named `x-amz-*` are signed too,
    /// the others (conditionals, `range`) are not.
    fn send(
        &self,
        method: &str,
        key: &str,
        query: &[(&str, String)],
        body: &[u8],
        extra: &[(&str, &str)],
    ) -> Result<Reply, StoreError> {
        let (url, host, uri) = object_url(&self.cfg, key);
        let cq = canonical_query(query);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| StoreError::Permanent(format!("clock before epoch: {e}")))?
            .as_secs();
        let date = amz_date(now);
        let ph = sha256_hex(body);
        let mut headers = vec![
            ("host".to_string(), host),
            ("x-amz-content-sha256".to_string(), ph.clone()),
            ("x-amz-date".to_string(), date.clone()),
        ];
        headers.extend(
            extra
                .iter()
                .filter(|(k, _)| k.starts_with("x-amz-"))
                .map(|(k, v)| (k.to_string(), v.to_string())),
        );
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
                "{method} {key}: TLS certificate not trusted: {why}; set BUCKWAL_CA_BUNDLE to the CA that signed the endpoint's certificate"
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
        let request_id = header("x-amz-request-id");
        let content_length = header("content-length").and_then(|v| v.parse().ok());
        let checksum_sha256 = header("x-amz-checksum-sha256");
        let body = resp.bytes().map_err(net)?.to_vec();
        Ok(Reply {
            status,
            etag,
            last_modified,
            date,
            request_id,
            content_length,
            checksum_sha256,
            body,
        })
    }

    /// Creates the bucket; an existing bucket we own is fine. Outside `us-east-1` S3 needs
    /// the region as the `LocationConstraint`; in `us-east-1` it refuses one.
    pub fn create_bucket(&self) -> crate::Result<()> {
        let config = (self.cfg.region != "us-east-1").then(|| {
            format!(
                "<CreateBucketConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><LocationConstraint>{}</LocationConstraint></CreateBucketConfiguration>",
                self.cfg.region
            )
        });
        let r = self
            .send(
                "PUT",
                "",
                &[],
                config.as_deref().unwrap_or("").as_bytes(),
                &[],
            )
            .map_err(|e| crate::Error::Bucket(e.to_string()))?;
        if r.status == 200
            || (r.status == 409
                && error_code(&r.body).as_deref() == Some("BucketAlreadyOwnedByYou"))
        {
            return Ok(());
        }
        Err(crate::Error::Bucket(format!(
            "create bucket: HTTP {}: {}",
            r.status,
            error_detail(&r)
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
    let size = r
        .content_length
        .ok_or_else(|| StoreError::Permanent(format!("{what}: no Content-Length header")))?;
    let modified = date_of("Last-Modified", &r.last_modified)?;
    let now = date_of("Date", &r.date)?;
    let age = now.duration_since(modified).unwrap_or(Duration::ZERO);
    Ok(Some(ObjectStat { etag, age, size }))
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

/// The ListObjectsV2 query. `start_after` applies to the first page only: a later page's
/// continuation token already encodes where the listing stands.
fn list_query(
    prefix: &str,
    start_after: Option<&str>,
    token: Option<&str>,
) -> Vec<(&'static str, String)> {
    let mut q = vec![
        ("list-type", "2".to_string()),
        ("prefix", prefix.to_string()),
    ];
    match (token, start_after) {
        (Some(t), _) => q.push(("continuation-token", t.to_string())),
        (None, Some(a)) => q.push(("start-after", a.to_string())),
        (None, None) => {}
    }
    q
}

/// The canonical query string: names and values encoded, then sorted.
fn canonical_query(query: &[(&str, String)]) -> String {
    let mut q: Vec<(String, String)> = query
        .iter()
        .map(|(k, v)| (uri_encode(k, true), uri_encode(v, true)))
        .collect();
    q.sort();
    q.iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// A part that is up: the ETag the server gave it and the SHA-256 buckwal computed.
#[derive(Clone)]
struct PartDone {
    etag: String,
    sha256: [u8; 32],
}

/// The body of the call that completes a multipart upload.
fn complete_body(parts: &[PartDone]) -> String {
    let mut b =
        String::from("<CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
    for (i, p) in parts.iter().enumerate() {
        b.push_str(&format!(
            "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag><ChecksumSHA256>{}</ChecksumSHA256></Part>",
            i + 1,
            xml_escape(&p.etag),
            base64(&p.sha256)
        ));
    }
    b.push_str("</CompleteMultipartUpload>");
    b
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// `YYYY-MM-DDTHH:MM:SS[.fff]Z`, the form of an upload's `Initiated`; UTC only.
fn parse_iso8601(s: &str) -> Option<SystemTime> {
    let s = s.trim().strip_suffix('Z')?;
    let (date, time) = s.split_once('T')?;
    let time = time.split('.').next()?;
    let num = |t: &str| {
        t.bytes()
            .all(|b| b.is_ascii_digit())
            .then(|| t.parse::<i64>().ok())
            .flatten()
    };
    let d: Vec<&str> = date.split('-').collect();
    let t: Vec<&str> = time.split(':').collect();
    let [y, mo, da] = d[..] else { return None };
    let [h, mi, se] = t[..] else { return None };
    let (y, mo, da, h, mi, se) = (num(y)?, num(mo)?, num(da)?, num(h)?, num(mi)?, num(se)?);
    if !(1..=12).contains(&mo) || !(1..=31).contains(&da) || h > 23 || mi > 59 || se > 60 {
        return None;
    }
    let secs = days_from_civil(y, mo, da) * 86_400 + h * 3600 + mi * 60 + se;
    u64::try_from(secs)
        .ok()
        .map(|s| std::time::UNIX_EPOCH + Duration::from_secs(s))
}

struct UploadInfo {
    key: String,
    id: String,
    initiated: SystemTime,
}

struct UploadsPage {
    uploads: Vec<UploadInfo>,
    /// The key and upload id markers of the next page.
    next: Option<(String, String)>,
}

/// A `ListMultipartUploads` reply.
fn parse_uploads(xml: &str) -> Result<UploadsPage, StoreError> {
    let bad = |what: &str| StoreError::Permanent(format!("LIST uploads: {what} in a reply"));
    let one = |block: &str, tag: &str| {
        between(block, &format!("<{tag}>"), &format!("</{tag}>"))
            .first()
            .map(|v| xml_unescape(v))
    };
    let mut uploads = Vec::new();
    for block in between(xml, "<Upload>", "</Upload>") {
        let (Some(key), Some(id), Some(at)) = (
            one(block, "Key"),
            one(block, "UploadId"),
            one(block, "Initiated"),
        ) else {
            return Err(bad("an upload with no key, id or start time"));
        };
        let initiated = parse_iso8601(&at).ok_or_else(|| bad("a bad start time"))?;
        uploads.push(UploadInfo { key, id, initiated });
    }
    let next = if between(xml, "<IsTruncated>", "</IsTruncated>").first() == Some(&"true") {
        match (one(xml, "NextKeyMarker"), one(xml, "NextUploadIdMarker")) {
            (Some(k), Some(u)) => Some((k, u)),
            _ => return Err(bad("a truncated page with no marker")),
        }
    } else {
        None
    };
    Ok(UploadsPage { uploads, next })
}

/// The reply to the start of a multipart upload: the upload id.
fn initiate_reply(what: &str, r: &Reply) -> Result<String, StoreError> {
    if r.status != 200 {
        return Err(status_error(what, r));
    }
    let body = String::from_utf8_lossy(&r.body);
    between(&body, "<UploadId>", "</UploadId>")
        .first()
        .map(|v| xml_unescape(v))
        .ok_or_else(|| StoreError::Permanent(format!("{what}: no upload id in the reply")))
}

/// The reply to one part: its ETag. A checksum the server holds for the part must be ours.
fn part_reply(what: &str, r: &Reply, sent_b64: &str) -> Result<String, StoreError> {
    if r.status != 200 {
        return Err(status_error(what, r));
    }
    if r.checksum_sha256.as_deref().is_some_and(|c| c != sent_b64) {
        return Err(StoreError::Permanent(format!(
            "{what}: the server holds another SHA-256 checksum for the part"
        )));
    }
    r.etag
        .clone()
        .ok_or_else(|| StoreError::Permanent(format!("{what}: no ETag in response")))
}

/// The reply to the end of a multipart upload: the object's ETag. S3 can answer 200 and then
/// put an error in the body; that is a transient failure.
fn complete_reply(what: &str, r: &Reply) -> Result<String, StoreError> {
    if r.status != 200 {
        return Err(status_error(what, r));
    }
    let body = String::from_utf8_lossy(&r.body);
    if body.contains("<Error>") {
        return Err(StoreError::Transient(format!(
            "{what}: error in a 200 reply: {}",
            error_detail(r)
        )));
    }
    between(&body, "<ETag>", "</ETag>")
        .first()
        .map(|v| xml_unescape(v))
        .or_else(|| r.etag.clone())
        .ok_or_else(|| StoreError::Permanent(format!("{what}: no ETag in response")))
}

/// The reply to a ranged GET: `Some(bytes)`, or `None` for a missing key. A range that starts
/// at or past the end of the object is 416 and gives no bytes.
fn range_reply(what: &str, r: &Reply) -> Result<Option<Vec<u8>>, StoreError> {
    match r.status {
        206 => Ok(Some(r.body.clone())),
        416 => Ok(Some(Vec::new())),
        404 if String::from_utf8_lossy(&r.body).contains("NoSuchKey") => Ok(None),
        200 => Err(StoreError::Permanent(format!(
            "{what}: the server ignored the range"
        ))),
        _ => Err(status_error(what, r)),
    }
}

impl S3Store {
    fn initiate(&self, key: &str) -> Result<String, StoreError> {
        let what = format!("POST {key} (start multipart)");
        retry_store(&self.retry, &what, |_| {
            self.count_put(0);
            let r = self.send(
                "POST",
                key,
                &[("uploads", String::new())],
                &[],
                &[("x-amz-checksum-algorithm", "SHA256")],
            )?;
            initiate_reply(&what, &r)
        })
    }

    /// Part `number` (from 1), retried alone: a failed request does not restart the upload.
    fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        number: u64,
        body: Vec<u8>,
    ) -> Result<PartDone, StoreError> {
        let what = format!("PUT {key} part {number}");
        let sha256 = sha256_of(&body);
        let b64 = base64(&sha256);
        let query = [
            ("partNumber", number.to_string()),
            ("uploadId", upload_id.to_string()),
        ];
        let etag = retry_store(&self.retry, &what, |_| {
            self.count_put(body.len());
            let r = self.send(
                "PUT",
                key,
                &query,
                &body,
                &[("x-amz-checksum-sha256", b64.as_str())],
            )?;
            part_reply(&what, &r, &b64)
        })?;
        Ok(PartDone { etag, sha256 })
    }

    fn complete_upload(
        &self,
        key: &str,
        upload_id: &str,
        parts: &[PartDone],
    ) -> Result<String, StoreError> {
        let what = format!("POST {key} (complete multipart)");
        let body = complete_body(parts);
        let query = [("uploadId", upload_id.to_string())];
        retry_store(&self.retry, &what, |_| {
            self.count_put(body.len());
            let r = self.send("POST", key, &query, body.as_bytes(), &[])?;
            complete_reply(&what, &r)
        })
    }
}

impl S3Store {
    fn list_from(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> Result<Vec<String>, StoreError> {
        Ok(self
            .list_sized_from(prefix, start_after)?
            .into_iter()
            .map(|(k, _)| k)
            .collect())
    }

    fn list_sized_from(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> Result<Vec<(String, u64)>, StoreError> {
        let mut keys = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let q = list_query(prefix, start_after, token.as_deref());
            let r = self.send("GET", "", &q, &[], &[])?;
            self.lists.fetch_add(1, Relaxed);
            if r.status != 200 {
                return Err(status_error(&format!("LIST {prefix}"), &r));
            }
            let page = parse_list(&String::from_utf8_lossy(&r.body));
            if page.sizes.len() != page.keys.len() {
                return Err(StoreError::Permanent(format!(
                    "LIST {prefix}: {} keys but {} sizes in a reply",
                    page.keys.len(),
                    page.sizes.len()
                )));
            }
            keys.extend(page.keys.into_iter().zip(page.sizes));
            match page.next {
                Some(t) => token = Some(t),
                None => break,
            }
        }
        if let Some(a) = start_after {
            keys.retain(|(k, _)| k.as_str() > a);
        }
        keys.sort();
        Ok(keys)
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
        self.list_from(prefix, None)
    }

    fn list_after(&self, prefix: &str, start_after: &str) -> Result<Vec<String>, StoreError> {
        self.list_from(prefix, Some(start_after))
    }

    fn list_sized(&self, prefix: &str) -> Result<Vec<(String, u64)>, StoreError> {
        self.list_sized_from(prefix, None)
    }

    /// `concurrency` parts go up at once, each read from `source` when its turn comes. A
    /// failure stops new parts and leaves the upload in the bucket, hidden; see
    /// [`ObjectStore::abort_incomplete`].
    fn put_multipart(
        &self,
        key: &str,
        source: &mut dyn PartSource,
        part_size: usize,
        concurrency: usize,
    ) -> Result<MultipartDone, StoreError> {
        let source: &dyn PartSource = source;
        let size = part_size_for(source.len(), part_size);
        let n = part_count(source.len(), size);
        let upload_id = self.initiate(key)?;
        let next = AtomicU64::new(0);
        let parts: Mutex<Vec<Option<PartDone>>> = Mutex::new(vec![None; n as usize]);
        let failure: Mutex<Option<StoreError>> = Mutex::new(None);
        std::thread::scope(|sc| {
            for _ in 0..(concurrency.max(1) as u64).min(n) {
                sc.spawn(|| loop {
                    if failure.lock().unwrap().is_some() {
                        return;
                    }
                    let i = next.fetch_add(1, Relaxed);
                    if i >= n {
                        return;
                    }
                    let done = read_part(source, size, i)
                        .and_then(|body| self.upload_part(key, &upload_id, i + 1, body));
                    match done {
                        Ok(p) => parts.lock().unwrap()[i as usize] = Some(p),
                        Err(e) => {
                            failure.lock().unwrap().get_or_insert(e);
                        }
                    }
                });
            }
        });
        if let Some(e) = failure.into_inner().unwrap() {
            return Err(e);
        }
        let parts = parts
            .into_inner()
            .unwrap()
            .into_iter()
            .collect::<Option<Vec<PartDone>>>()
            .ok_or_else(|| StoreError::Permanent(format!("PUT {key}: a part is missing")))?;
        let etag = self.complete_upload(key, &upload_id, &parts)?;
        Ok(MultipartDone {
            etag,
            part_sha256: parts.iter().map(|p| p.sha256).collect(),
        })
    }

    fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Option<Vec<u8>>, StoreError> {
        if len == 0 {
            return Err(StoreError::Permanent(format!("GET {key}: an empty range")));
        }
        let range = format!("bytes={offset}-{}", offset.saturating_add(len) - 1);
        let r = self.send("GET", key, &[], &[], &[("range", range.as_str())])?;
        self.gets.fetch_add(1, Relaxed);
        let got = range_reply(&format!("GET {key} (range)"), &r)?;
        self.bytes_down
            .fetch_add(got.as_ref().map_or(0, Vec::len) as u64, Relaxed);
        Ok(got)
    }

    /// Ages come from the bucket: an upload's `Initiated` against the `Date` of the listing.
    fn abort_incomplete(&self, prefix: &str, older_than: Duration) -> Result<u64, StoreError> {
        let what = format!("LIST uploads {prefix}");
        let mut marker: Option<(String, String)> = None;
        let mut aborted = 0;
        loop {
            let mut q = vec![("uploads", String::new()), ("prefix", prefix.to_string())];
            if let Some((k, u)) = &marker {
                q.push(("key-marker", k.clone()));
                q.push(("upload-id-marker", u.clone()));
            }
            let r = self.send("GET", "", &q, &[], &[])?;
            self.lists.fetch_add(1, Relaxed);
            if r.status != 200 {
                return Err(status_error(&what, &r));
            }
            let now =
                r.date.as_deref().and_then(parse_http_date).ok_or_else(|| {
                    StoreError::Permanent(format!("{what}: no usable Date header"))
                })?;
            let page = parse_uploads(&String::from_utf8_lossy(&r.body))?;
            for u in page.uploads {
                let age = now.duration_since(u.initiated).unwrap_or(Duration::ZERO);
                if age < older_than {
                    continue;
                }
                let a = self.send("DELETE", &u.key, &[("uploadId", u.id)], &[], &[])?;
                self.deletes.fetch_add(1, Relaxed);
                match a.status {
                    200 | 204 => aborted += 1,
                    // Gone since the listing: another run aborted or completed it.
                    404 => {}
                    _ => return Err(status_error(&format!("DELETE {} (abort)", u.key), &a)),
                }
            }
            match page.next {
                Some(m) => marker = Some(m),
                None => return Ok(aborted),
            }
        }
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
            request_id: None,
            content_length: None,
            checksum_sha256: None,
            body: body.as_bytes().to_vec(),
        }
    }

    #[test]
    fn list_after_query_has_start_after_and_signs_sorted() {
        let q = list_query(
            "db/log/0000000001/",
            Some("db/log/0000000001/00000000000000000002"),
            None,
        );
        assert!(q.contains(&(
            "start-after",
            "db/log/0000000001/00000000000000000002".to_string()
        )));
        assert_eq!(
            canonical_query(&q),
            "list-type=2&prefix=db%2Flog%2F0000000001%2F&start-after=db%2Flog%2F0000000001%2F00000000000000000002"
        );
        // later pages carry the token alone: it already encodes where the listing stands
        let q = list_query("p/", Some("p/1"), Some("tok+/="));
        assert_eq!(
            canonical_query(&q),
            "continuation-token=tok%2B%2F%3D&list-type=2&prefix=p%2F"
        );
        // plain list sends no start-after
        let q = list_query("p/", None, None);
        assert_eq!(canonical_query(&q), "list-type=2&prefix=p%2F");
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
        r.content_length = Some(1234);
        let st = stat_reply("HEAD k", &r).unwrap().unwrap();
        assert_eq!(st.etag, "\"e\"");
        assert_eq!(st.size, 1234);
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
        for status in [301, 302, 307, 308, 400, 403, 404] {
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

    /// One HTTP/1.1 request from `s`: the request line, the header lines and the
    /// `Content-Length` body.
    fn read_request(s: &mut impl std::io::Read) -> (String, Vec<String>, Vec<u8>) {
        let mut head = Vec::new();
        let mut b = [0u8; 1];
        while !head.ends_with(b"\r\n\r\n") && s.read(&mut b).unwrap_or(0) == 1 {
            head.push(b[0]);
        }
        let text = String::from_utf8_lossy(&head).to_string();
        let mut lines = text.split("\r\n").map(str::to_string);
        let first = lines.next().unwrap_or_default();
        let headers: Vec<String> = lines.filter(|l| !l.is_empty()).collect();
        let n = headers
            .iter()
            .find_map(|h| {
                let (k, v) = h.split_once(':')?;
                k.eq_ignore_ascii_case("content-length")
                    .then(|| v.trim().parse().ok())
                    .flatten()
            })
            .unwrap_or(0);
        let mut body = vec![0u8; n];
        let _ = s.read_exact(&mut body);
        (first, headers, body)
    }

    /// A plain-HTTP endpoint on 127.0.0.1 that answers every request with `status` and a
    /// `Location` on a second listener, which sends the request line of anything that
    /// reaches it to the returned receiver and answers 200 with an ETag.
    fn redirecting_endpoint(status: u16) -> (String, std::sync::mpsc::Receiver<String>) {
        use std::io::Write;
        let target = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let target_port = target.local_addr().unwrap().port();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for s in target.incoming() {
                let Ok(mut s) = s else { continue };
                let _ = tx.send(read_request(&mut s).0);
                let _ = write!(
                    s,
                    "HTTP/1.1 200 OK\r\nETag: \"e1\"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                );
            }
        });
        let origin = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", origin.local_addr().unwrap());
        std::thread::spawn(move || {
            for s in origin.incoming() {
                let Ok(mut s) = s else { continue };
                let (first, _, _) = read_request(&mut s);
                let path = first.split(' ').nth(1).unwrap_or("/").to_string();
                let _ = write!(
                    s,
                    "HTTP/1.1 {status} Redirect\r\nLocation: http://127.0.0.1:{target_port}{path}\r\n\
                     Content-Length: 0\r\nConnection: close\r\n\r\n"
                );
            }
        });
        (endpoint, rx)
    }

    /// B1: a redirect is never followed (a followed one could carry the body, and the
    /// signature, to another server, and its answer would be taken as the store's): every
    /// call gets a permanent error naming the status, and nothing reaches the target.
    #[test]
    fn redirects_are_not_followed() {
        let mut wrong = Vec::new();
        for status in [301, 302, 307, 308] {
            let (endpoint, reached) = redirecting_endpoint(status);
            let mut cfg = cfg_for(&endpoint);
            cfg.path_style = true;
            let s = S3Store::new(cfg).unwrap();
            let calls: [(&str, Result<(), StoreError>); 12] = [
                ("put", s.put("log/1/00000001", b"ROW").map(drop)),
                ("put_if_absent", s.put_if_absent("k", b"ROW").map(drop)),
                (
                    "put_if_match",
                    s.put_if_match("k", b"ROW", "\"e\"").map(drop),
                ),
                ("get", s.get("k").map(drop)),
                ("stat", s.stat("k").map(drop)),
                ("list", s.list("log/").map(drop)),
                ("delete", s.delete("k")),
                ("list_sized", s.list_sized("log/").map(drop)),
                ("get_range", s.get_range("k", 0, 4).map(drop)),
                (
                    "abort_incomplete",
                    s.abort_incomplete("p/", Duration::ZERO).map(drop),
                ),
                (
                    "put_multipart",
                    s.put_multipart("k", &mut Bytes(vec![1; 3]), 4, 2).map(drop),
                ),
                (
                    "create_bucket",
                    s.create_bucket()
                        .map_err(|e| StoreError::Permanent(e.to_string())),
                ),
            ];
            for (call, r) in calls {
                match r {
                    Err(StoreError::Permanent(m)) if m.contains(&format!("HTTP {status}")) => {}
                    r => wrong.push(format!("{status} {call}: {r:?}")),
                }
            }
            while let Ok(req) = reached.recv_timeout(Duration::from_millis(200)) {
                wrong.push(format!("{status}: the redirect was followed: {req}"));
            }
        }
        assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    }

    /// A plain-HTTP endpoint on 127.0.0.1 that answers one request with `status`, the extra
    /// `headers` and `body`. The returned receiver gets the request line and the body.
    fn one_reply_endpoint(
        status: u16,
        headers: &'static str,
        body: &'static str,
    ) -> (String, std::sync::mpsc::Receiver<(String, Vec<u8>)>) {
        use std::io::Write;
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", l.local_addr().unwrap());
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let Ok((mut s, _)) = l.accept() else { return };
            let (first, _, got) = read_request(&mut s);
            let _ = tx.send((first, got));
            let _ = write!(
                s,
                "HTTP/1.1 {status} X\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
        });
        (endpoint, rx)
    }

    fn store_in(endpoint: &str, region: &str) -> S3Store {
        let mut cfg = cfg_for(endpoint);
        cfg.region = region.into();
        cfg.path_style = true;
        S3Store::new(cfg).unwrap()
    }

    #[test]
    fn create_bucket_sends_location_constraint_outside_us_east_1() {
        let (endpoint, rx) = one_reply_endpoint(200, "", "");
        store_in(&endpoint, "ap-southeast-2")
            .create_bucket()
            .unwrap();
        let (_, body) = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(
            String::from_utf8(body).unwrap(),
            "<CreateBucketConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><LocationConstraint>ap-southeast-2</LocationConstraint></CreateBucketConfiguration>"
        );
    }

    #[test]
    fn create_bucket_sends_no_body_in_us_east_1() {
        let (endpoint, rx) = one_reply_endpoint(200, "", "");
        store_in(&endpoint, "us-east-1").create_bucket().unwrap();
        let (_, body) = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(body.is_empty(), "{body:?}");
    }

    #[test]
    fn s3_error_log_holds_code_and_request_id_only() {
        const KEY_ID: &str = "AKIAEXAMPLEKEYID123";
        const BODY: &str = "<Error><Code>SignatureDoesNotMatch</Code><Message>bad</Message><AWSAccessKeyId>AKIAEXAMPLEKEYID123</AWSAccessKeyId><StringToSign>AWS4-HMAC-SHA256 STRINGTOSIGN</StringToSign></Error>";
        let hdr = "x-amz-request-id: REQ123\r\n";
        // create_bucket (a crate::Error) and put (a StoreError) both show the code and id only.
        let (endpoint, _rx) = one_reply_endpoint(403, hdr, BODY);
        let e = store_in(&endpoint, "us-east-1")
            .create_bucket()
            .unwrap_err()
            .to_string();
        let (endpoint, _rx) = one_reply_endpoint(403, hdr, BODY);
        let p = store_in(&endpoint, "us-east-1")
            .put("k", b"v")
            .unwrap_err()
            .to_string();
        for text in [e, p] {
            assert!(text.contains("SignatureDoesNotMatch"), "{text}");
            assert!(text.contains("REQ123"), "{text}");
            assert!(!text.contains(KEY_ID), "{text}");
            assert!(!text.contains("STRINGTOSIGN"), "{text}");
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

    #[test]
    fn s3_new_refuses_ca_bundle_with_http_endpoint() {
        for ep in ["HTTP://x", " http://x"] {
            let mut cfg = cfg_for("https://s3.example.com");
            cfg.endpoint = ep.to_string();
            cfg.ca_bundle = Some("/ca.pem".into());
            match S3Store::new(cfg) {
                Err(crate::Error::Config(m)) => assert_eq!(
                    m, "BUCKWAL_CA_BUNDLE is set but the endpoint is http://",
                    "{ep:?}"
                ),
                r => panic!("{ep:?}: {:?}", r.err()),
            }
        }
    }

    #[test]
    fn s3_new_accepts_ca_bundle_with_https_endpoint() {
        let mut cfg = cfg_for("https://s3.example.com");
        cfg.ca_bundle = Some("/nonexistent/ca.pem".into());
        match S3Store::new(cfg) {
            Err(crate::Error::Config(m)) => {
                assert!(!m.contains("endpoint is http://"), "{m}");
                assert!(m.contains("/nonexistent/ca.pem"), "{m}");
            }
            r => panic!("{:?}", r.err()),
        }
    }

    #[test]
    fn base64_matches_rfc4648_vectors() {
        for (raw, enc) in [
            (&b""[..], ""),
            (b"f", "Zg=="),
            (b"fo", "Zm8="),
            (b"foo", "Zm9v"),
            (b"foob", "Zm9vYg=="),
            (b"fooba", "Zm9vYmE="),
            (b"foobar", "Zm9vYmFy"),
        ] {
            assert_eq!(base64(raw), enc);
        }
    }

    #[test]
    fn iso8601_dates_parse() {
        use std::time::UNIX_EPOCH;
        assert_eq!(
            parse_iso8601("1994-11-06T08:49:37.000Z"),
            Some(UNIX_EPOCH + Duration::from_secs(784_111_777))
        );
        assert_eq!(
            parse_iso8601("1970-01-01T00:00:10Z"),
            Some(UNIX_EPOCH + Duration::from_secs(10))
        );
        for bad in ["", "1994-11-06", "1994-11-06T08:49:37", "x-11-06T08:49:37Z"] {
            assert_eq!(parse_iso8601(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn list_sized_page_pairs_keys_and_sizes() {
        let xml = "<ListBucketResult><IsTruncated>false</IsTruncated>\
            <Contents><Key>a/1</Key><LastModified>x</LastModified><Size>12</Size></Contents>\
            <Contents><Key>a/2&amp;</Key><Size>0</Size></Contents></ListBucketResult>";
        let page = parse_list(xml);
        assert_eq!(page.keys, ["a/1", "a/2&"]);
        assert_eq!(page.sizes, [12, 0]);
    }

    #[test]
    fn uploads_page_parses_ids_ages_and_markers() {
        let xml = "<ListMultipartUploadsResult><IsTruncated>true</IsTruncated>\
            <NextKeyMarker>p/b</NextKeyMarker><NextUploadIdMarker>u2</NextUploadIdMarker>\
            <Upload><Key>p/a</Key><UploadId>u1</UploadId><Initiated>2026-10-25T01:00:00.000Z</Initiated></Upload>\
            <Upload><Key>p/b&amp;</Key><UploadId>u2</UploadId><Initiated>2026-10-25T02:00:00.000Z</Initiated></Upload>\
            </ListMultipartUploadsResult>";
        let page = parse_uploads(xml).unwrap();
        assert_eq!(page.uploads.len(), 2);
        assert_eq!(
            (page.uploads[1].key.as_str(), page.uploads[1].id.as_str()),
            ("p/b&", "u2")
        );
        assert_eq!(page.next, Some(("p/b".to_string(), "u2".to_string())));
        let done = parse_uploads("<ListMultipartUploadsResult><IsTruncated>false</IsTruncated></ListMultipartUploadsResult>").unwrap();
        assert!(done.uploads.is_empty() && done.next.is_none());
    }

    #[test]
    fn complete_body_lists_every_part_with_its_checksum() {
        let parts = vec![
            PartDone {
                etag: "\"e1\"".into(),
                sha256: [1; 32],
            },
            PartDone {
                etag: "\"e2\"".into(),
                sha256: [2; 32],
            },
        ];
        let b = complete_body(&parts);
        assert!(b.starts_with("<CompleteMultipartUpload"), "{b}");
        assert!(b.contains("<Part><PartNumber>1</PartNumber><ETag>&quot;e1&quot;</ETag><ChecksumSHA256>AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=</ChecksumSHA256></Part>"), "{b}");
        assert!(b.contains("<PartNumber>2</PartNumber>"), "{b}");
    }

    /// A plain-HTTP endpoint on 127.0.0.1 that answers `script` in order, one connection per
    /// reply. The receiver gets each request's line, headers and body.
    #[allow(clippy::type_complexity)]
    fn scripted_endpoint(
        script: Vec<(u16, String, String)>,
    ) -> (
        String,
        std::sync::mpsc::Receiver<(String, Vec<String>, Vec<u8>)>,
    ) {
        use std::io::Write;
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", l.local_addr().unwrap());
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for (status, headers, body) in script {
                let Ok((mut s, _)) = l.accept() else { return };
                let _ = tx.send(read_request(&mut s));
                let _ = write!(
                    s,
                    "HTTP/1.1 {status} X\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
            }
        });
        (endpoint, rx)
    }

    struct Bytes(Vec<u8>);

    impl crate::store::PartSource for Bytes {
        fn len(&self) -> u64 {
            self.0.len() as u64
        }
        fn read_at(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<()> {
            buf.copy_from_slice(&self.0[offset as usize..offset as usize + buf.len()]);
            Ok(())
        }
    }

    fn fast_retry() -> crate::store::RetryPolicy {
        crate::store::RetryPolicy {
            deadline: Duration::from_secs(5),
            first_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
        }
    }

    /// One part fails with a 503, then goes through: the retry repeats that part only. The
    /// part carries its SHA-256 as a signed header, and complete lists the part.
    #[test]
    fn multipart_retries_the_part_not_the_upload() {
        let sha = base64(&Sha256::digest(b"hello"));
        let part_ok = format!("ETag: \"p1\"\r\nx-amz-checksum-sha256: {sha}\r\n");
        let (endpoint, rx) = scripted_endpoint(vec![
            (200, String::new(), "<InitiateMultipartUploadResult><UploadId>U1</UploadId></InitiateMultipartUploadResult>".into()),
            (503, String::new(), s3_error("SlowDown")),
            (200, part_ok, String::new()),
            (200, String::new(), "<CompleteMultipartUploadResult><ETag>&quot;final-1&quot;</ETag></CompleteMultipartUploadResult>".into()),
        ]);
        let s = store_in(&endpoint, "us-east-1").with_retry_policy(fast_retry());
        let done = s
            .put_multipart("pack/1", &mut Bytes(b"hello".to_vec()), 16, 8)
            .unwrap();
        assert_eq!(done.etag, "\"final-1\"");
        assert_eq!(
            done.part_sha256,
            vec![<[u8; 32]>::from(Sha256::digest(b"hello"))]
        );
        let reqs: Vec<_> = rx.try_iter().collect();
        let lines: Vec<&str> = reqs.iter().map(|r| r.0.as_str()).collect();
        assert_eq!(
            lines,
            [
                "POST /b/pack/1?uploads= HTTP/1.1",
                "PUT /b/pack/1?partNumber=1&uploadId=U1 HTTP/1.1",
                "PUT /b/pack/1?partNumber=1&uploadId=U1 HTTP/1.1",
                "POST /b/pack/1?uploadId=U1 HTTP/1.1",
            ]
        );
        let has = |r: &(String, Vec<String>, Vec<u8>), h: &str| {
            r.1.iter().any(|x| x.eq_ignore_ascii_case(h))
        };
        assert!(
            has(&reqs[1], &format!("x-amz-checksum-sha256: {sha}")),
            "{:?}",
            reqs[1].1
        );
        assert!(
            has(&reqs[0], "x-amz-checksum-algorithm: SHA256"),
            "{:?}",
            reqs[0].1
        );
        let auth = reqs[1]
            .1
            .iter()
            .find(|h| h.to_ascii_lowercase().starts_with("authorization"))
            .unwrap();
        assert!(
            auth.contains("x-amz-checksum-sha256"),
            "the header is signed: {auth}"
        );
        assert!(String::from_utf8_lossy(&reqs[3].2).contains("<PartNumber>1</PartNumber>"));
    }

    /// A server that echoes another checksum for a part is an error, not an upload.
    #[test]
    fn multipart_refuses_a_part_with_a_wrong_checksum() {
        let (endpoint, _rx) = scripted_endpoint(vec![
            (200, String::new(), "<InitiateMultipartUploadResult><UploadId>U1</UploadId></InitiateMultipartUploadResult>".into()),
            (200, "ETag: \"p1\"\r\nx-amz-checksum-sha256: AAAA\r\n".into(), String::new()),
        ]);
        let s = store_in(&endpoint, "us-east-1").with_retry_policy(fast_retry());
        let e = s
            .put_multipart("k", &mut Bytes(b"hello".to_vec()), 16, 1)
            .unwrap_err();
        assert!(
            matches!(&e, StoreError::Permanent(m) if m.contains("checksum")),
            "{e:?}"
        );
    }

    /// The new requests print the error `Code` and request id only, as every other request does.
    #[test]
    fn new_requests_log_code_and_request_id_only() {
        const BODY: &str = "<Error><Code>AccessDenied</Code><AWSAccessKeyId>AKIAEXAMPLEKEYID123</AWSAccessKeyId><StringToSign>STRINGTOSIGN</StringToSign></Error>";
        let hdr = "x-amz-request-id: REQ9\r\n";
        let texts = [
            (|s: &S3Store| s.get_range("k", 0, 4).unwrap_err()) as fn(&S3Store) -> StoreError,
            |s| s.abort_incomplete("p/", Duration::ZERO).unwrap_err(),
            |s| s.list_sized("p/").unwrap_err(),
            |s| s.put_multipart("k", &mut Bytes(vec![1]), 4, 1).unwrap_err(),
        ];
        for call in texts {
            let (endpoint, _rx) = one_reply_endpoint(403, hdr, BODY);
            let t = call(&store_in(&endpoint, "us-east-1")).to_string();
            assert!(t.contains("AccessDenied") && t.contains("REQ9"), "{t}");
            assert!(
                !t.contains("AKIAEXAMPLEKEYID123") && !t.contains("STRINGTOSIGN"),
                "{t}"
            );
        }
    }

    #[test]
    fn get_range_sends_a_range_header_and_maps_statuses() {
        let (endpoint, rx) = scripted_endpoint(vec![(206, String::new(), "abcd".into())]);
        let s = store_in(&endpoint, "us-east-1");
        assert_eq!(s.get_range("k", 10, 4).unwrap().unwrap(), b"abcd");
        let (line, headers, _) = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(line.starts_with("GET /b/k "), "{line}");
        assert!(
            headers
                .iter()
                .any(|h| h.eq_ignore_ascii_case("range: bytes=10-13")),
            "{headers:?}"
        );
        let (endpoint, _rx) = one_reply_endpoint(404, "", "<Error><Code>NoSuchKey</Code></Error>");
        assert_eq!(
            store_in(&endpoint, "us-east-1")
                .get_range("k", 0, 4)
                .unwrap(),
            None
        );
        let (endpoint, _rx) =
            one_reply_endpoint(416, "", "<Error><Code>InvalidRange</Code></Error>");
        assert_eq!(
            store_in(&endpoint, "us-east-1")
                .get_range("k", 99, 4)
                .unwrap()
                .unwrap(),
            b""
        );
    }
}
