//! Server configuration: [`ServeConfig`] and the bearer [`Token`]. Settings come from
//! `DURABLE_SERVE_*` variables (an empty value counts as unset), and the listen address
//! also from the `--listen` flag, which wins. Errors are plain strings naming the variable
//! or flag at fault; none ever carries the token file's content.
use sha2::{Digest, Sha256};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;
use subtle::ConstantTimeEq;

/// The listen address when neither `--listen` nor `DURABLE_SERVE_LISTEN` is given.
pub const DEFAULT_LISTEN: &str = "127.0.0.1:9295";

/// The shortest token the server accepts, in bytes (after trimming).
const MIN_TOKEN_LEN: usize = 16;

/// The certificate chain and private key (PEM files) for HTTPS.
#[derive(Debug, Clone)]
pub struct TlsFiles {
    /// The PEM certificate chain (`DURABLE_SERVE_TLS_CERT`).
    pub cert: PathBuf,
    /// The PEM private key (`DURABLE_SERVE_TLS_KEY`).
    pub key: PathBuf,
}

/// Everything `serve` needs besides the database's own `Config`.
#[derive(Debug, Clone)]
pub struct ServeConfig {
    /// Where to listen: an IP and a port (`--listen`, `DURABLE_SERVE_LISTEN`).
    pub listen: SocketAddr,
    /// Serve HTTPS when set.
    pub tls: Option<TlsFiles>,
    /// The file holding the bearer token (`DURABLE_SERVE_TOKEN_FILE`); no auth when unset.
    pub token_file: Option<PathBuf>,
    /// How long a stream may sit idle before it is closed
    /// (`DURABLE_SERVE_STREAM_TIMEOUT_MS`, default 10 s).
    pub stream_timeout: Duration,
    /// How long shutdown waits for in-flight work (`DURABLE_SERVE_SHUTDOWN_MS`, default 45 s).
    pub shutdown_timeout: Duration,
    /// SQLite busy timeout of each connection (`DURABLE_SERVE_BUSY_TIMEOUT_MS`, default 5 s).
    pub busy_timeout: Duration,
    /// The most streams open at once (`DURABLE_SERVE_MAX_STREAMS`, default 256).
    pub max_streams: usize,
    /// Worker threads running statements (`DURABLE_SERVE_WORKERS`, default 32).
    pub workers: usize,
}

impl ServeConfig {
    /// Read the settings from the process environment; `listen_flag` is `--listen`.
    pub fn from_env(listen_flag: Option<&str>) -> Result<ServeConfig, String> {
        ServeConfig::from_lookup(listen_flag, |k| std::env::var(k).ok())
    }

    /// Like [`ServeConfig::from_env`], reading variables through `get`.
    pub fn from_lookup(
        listen_flag: Option<&str>,
        get: impl Fn(&str) -> Option<String>,
    ) -> Result<ServeConfig, String> {
        let get = |k: &str| get(k).filter(|v| !v.is_empty());
        let (listen_src, listen_text) = match listen_flag.filter(|v| !v.is_empty()) {
            Some(v) => ("--listen", v.to_string()),
            None => (
                "DURABLE_SERVE_LISTEN",
                get("DURABLE_SERVE_LISTEN").unwrap_or_else(|| DEFAULT_LISTEN.to_string()),
            ),
        };
        let listen: SocketAddr = listen_text.parse().map_err(|_| {
            format!(
                "{listen_src} must be IP:port (for example 127.0.0.1:9295), not {listen_text:?}"
            )
        })?;
        let tls = match (get("DURABLE_SERVE_TLS_CERT"), get("DURABLE_SERVE_TLS_KEY")) {
            (None, None) => None,
            (Some(cert), Some(key)) => Some(TlsFiles {
                cert: cert.into(),
                key: key.into(),
            }),
            (Some(_), None) => {
                return Err("DURABLE_SERVE_TLS_CERT needs DURABLE_SERVE_TLS_KEY".to_string())
            }
            (None, Some(_)) => {
                return Err("DURABLE_SERVE_TLS_KEY needs DURABLE_SERVE_TLS_CERT".to_string())
            }
        };
        let ms = |var: &str, default: u64| -> Result<Duration, String> {
            Ok(Duration::from_millis(positive(&get, var, default)?))
        };
        Ok(ServeConfig {
            listen,
            tls,
            token_file: get("DURABLE_SERVE_TOKEN_FILE").map(PathBuf::from),
            stream_timeout: ms("DURABLE_SERVE_STREAM_TIMEOUT_MS", 10_000)?,
            shutdown_timeout: ms("DURABLE_SERVE_SHUTDOWN_MS", 45_000)?,
            busy_timeout: ms("DURABLE_SERVE_BUSY_TIMEOUT_MS", 5_000)?,
            max_streams: positive(&get, "DURABLE_SERVE_MAX_STREAMS", 256)? as usize,
            workers: positive(&get, "DURABLE_SERVE_WORKERS", 32)? as usize,
        })
    }

    /// A non-loopback address exposes the server to the network, so it needs both TLS and
    /// a token; a loopback one needs neither.
    pub fn check_bind(&self) -> Result<(), String> {
        if self.listen.ip().is_loopback() || (self.tls.is_some() && self.token_file.is_some()) {
            return Ok(());
        }
        Err(format!(
            "listening on {} (not loopback) needs TLS (DURABLE_SERVE_TLS_CERT and \
             DURABLE_SERVE_TLS_KEY) and a token (DURABLE_SERVE_TOKEN_FILE)",
            self.listen
        ))
    }
}

/// A variable holding an integer of at least 1, or `default` when unset.
fn positive(get: &impl Fn(&str) -> Option<String>, var: &str, default: u64) -> Result<u64, String> {
    let Some(v) = get(var) else {
        return Ok(default);
    };
    match v.trim().parse::<u64>() {
        Ok(n) if n >= 1 => Ok(n),
        _ => Err(format!("{var} must be an integer of at least 1, not {v:?}")),
    }
}

/// The bearer token, kept only as its SHA-256.
pub struct Token([u8; 32]);

impl Token {
    /// Read the token from `path`: surrounding ASCII whitespace is trimmed, and what remains must
    /// be at least 16 bytes of visible ASCII (0x21 to 0x7E, so no whitespace inside). Errors name the path, never the content.
    pub fn load(path: &Path) -> Result<Token, String> {
        let shown = path.display();
        let bytes = std::fs::read(path).map_err(|e| {
            format!(
                "DURABLE_SERVE_TOKEN_FILE {shown}: cannot read: {}",
                e.kind()
            )
        })?;
        let text = bytes.trim_ascii();
        if text.is_empty() {
            return Err(format!("DURABLE_SERVE_TOKEN_FILE {shown}: file is empty"));
        }
        if text.len() < MIN_TOKEN_LEN {
            return Err(format!(
                "DURABLE_SERVE_TOKEN_FILE {shown}: token is shorter than {MIN_TOKEN_LEN} bytes"
            ));
        }
        if text.iter().any(u8::is_ascii_whitespace) {
            return Err(format!(
                "DURABLE_SERVE_TOKEN_FILE {shown}: token contains whitespace"
            ));
        }
        if !text.iter().all(|b| (0x21..=0x7e).contains(b)) {
            return Err(format!(
                "DURABLE_SERVE_TOKEN_FILE {shown}: token must be visible ASCII (0x21 to 0x7E)"
            ));
        }
        let text = std::str::from_utf8(text).expect("visible ASCII is UTF-8");
        Ok(Token::from_text(text))
    }

    fn from_text(text: &str) -> Token {
        Token(Sha256::digest(text.as_bytes()).into())
    }

    /// Whether `presented` is the token, compared in constant time over the hashes.
    pub fn matches(&self, presented: &str) -> bool {
        self.0.ct_eq(&Token::from_text(presented).0).into()
    }
}

impl std::fmt::Debug for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Token(<redacted>)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn lookup(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let m: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |k| m.get(k).cloned()
    }

    fn cfg(listen: &str, tls: bool, token: bool) -> ServeConfig {
        let mut c = ServeConfig::from_lookup(Some(listen), |_| None).unwrap();
        if tls {
            c.tls = Some(TlsFiles {
                cert: "c.pem".into(),
                key: "k.pem".into(),
            });
        }
        if token {
            c.token_file = Some("t".into());
        }
        c
    }

    #[test]
    fn defaults() {
        let c = ServeConfig::from_lookup(None, |_| None).unwrap();
        assert_eq!(c.listen, "127.0.0.1:9295".parse().unwrap());
        assert_eq!(c.stream_timeout, Duration::from_secs(10));
        assert_eq!(c.shutdown_timeout, Duration::from_secs(45));
        assert_eq!(c.busy_timeout, Duration::from_secs(5));
        assert_eq!(c.max_streams, 256);
        assert_eq!(c.workers, 32);
        assert!(c.tls.is_none());
        assert!(c.token_file.is_none());
    }

    #[test]
    fn flag_overrides_env() {
        let get = lookup(&[("DURABLE_SERVE_LISTEN", "127.0.0.1:1")]);
        let c = ServeConfig::from_lookup(Some("127.0.0.1:0"), get).unwrap();
        assert_eq!(c.listen, "127.0.0.1:0".parse().unwrap());
        let get = lookup(&[("DURABLE_SERVE_LISTEN", "127.0.0.1:1")]);
        let c = ServeConfig::from_lookup(None, get).unwrap();
        assert_eq!(c.listen.port(), 1);
    }

    #[test]
    fn listen_must_be_ip_port() {
        for bad in ["localhost:9295", "9295", "127.0.0.1"] {
            let e = ServeConfig::from_lookup(Some(bad), |_| None).unwrap_err();
            assert!(e.contains("--listen"), "{bad}: {e}");
        }
    }

    #[test]
    fn tls_needs_both_files() {
        let e = ServeConfig::from_lookup(None, lookup(&[("DURABLE_SERVE_TLS_CERT", "c.pem")]))
            .unwrap_err();
        assert!(e.contains("DURABLE_SERVE_TLS_KEY"), "{e}");
        let e = ServeConfig::from_lookup(None, lookup(&[("DURABLE_SERVE_TLS_KEY", "k.pem")]))
            .unwrap_err();
        assert!(e.contains("DURABLE_SERVE_TLS_CERT"), "{e}");
        let c = ServeConfig::from_lookup(
            None,
            lookup(&[
                ("DURABLE_SERVE_TLS_CERT", "c.pem"),
                ("DURABLE_SERVE_TLS_KEY", "k.pem"),
                ("DURABLE_SERVE_TOKEN_FILE", ""),
            ]),
        )
        .unwrap();
        assert_eq!(c.tls.unwrap().cert, PathBuf::from("c.pem"));
        assert!(c.token_file.is_none());
    }

    #[test]
    fn bad_numbers_are_errors() {
        for var in [
            "DURABLE_SERVE_STREAM_TIMEOUT_MS",
            "DURABLE_SERVE_WORKERS",
            "DURABLE_SERVE_MAX_STREAMS",
        ] {
            for bad in ["0", "x"] {
                let e = ServeConfig::from_lookup(None, lookup(&[(var, bad)])).unwrap_err();
                assert!(e.contains(var), "{var}={bad}: {e}");
            }
        }
        let c = ServeConfig::from_lookup(None, lookup(&[("DURABLE_SERVE_WORKERS", "3")])).unwrap();
        assert_eq!(c.workers, 3);
    }

    #[test]
    fn loopback_binds_without_tls_or_token() {
        for l in ["127.0.0.1:0", "127.5.5.5:0", "[::1]:0"] {
            cfg(l, false, false).check_bind().unwrap();
        }
    }

    #[test]
    fn non_loopback_needs_tls_and_token() {
        for l in ["0.0.0.0:0", "192.168.1.10:0"] {
            for (tls, token) in [(false, false), (true, false), (false, true)] {
                let e = cfg(l, tls, token).check_bind().unwrap_err();
                assert!(e.contains("DURABLE_SERVE_TLS_CERT"), "{e}");
                assert!(e.contains("DURABLE_SERVE_TOKEN_FILE"), "{e}");
            }
            cfg(l, true, true).check_bind().unwrap();
        }
    }

    fn token_file(content: &[u8]) -> (tempfile::TempDir, PathBuf) {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("token");
        std::fs::write(&p, content).unwrap();
        (d, p)
    }

    #[test]
    fn token_file_is_trimmed() {
        let (_d, p) = token_file(b"abcdefghijklmnop0123\n");
        let t = Token::load(&p).unwrap();
        assert!(t.matches("abcdefghijklmnop0123"));
        assert!(!t.matches("abcdefghijklmnop012"));
        assert!(!t.matches(""));
    }

    #[test]
    fn token_file_rules() {
        let d = tempfile::tempdir().unwrap();
        let missing = d.path().join("nope");
        let e = Token::load(&missing).unwrap_err();
        assert!(e.contains(missing.to_str().unwrap()), "{e}");
        let cases: [(&[u8], &str); 5] = [
            (b"", ""),
            (b"\n", ""),
            (b"SHORTSECRET123", "SHORTSECRET123"),
            (b"INNERSECRET ab cdefgh", "INNERSECRET"),
            (b"LINE1SECRETabcdef\nLINE2SECRETabcdef", "LINE1SECRET"),
        ];
        for (content, needle) in cases {
            let (_d, p) = token_file(content);
            let e = Token::load(&p).unwrap_err();
            assert!(e.contains(p.to_str().unwrap()), "{e}");
            if !needle.is_empty() {
                assert!(!e.contains(needle), "{e}");
                assert!(!e.contains("LINE2SECRET"), "{e}");
            }
        }
    }

    #[test]
    fn token_must_be_visible_ascii() {
        let cases: [(&[u8], &str); 6] = [
            ("éééééééé".as_bytes(), "éé"),
            (b"CTRLSECRET\x07abcdefg", "CTRLSECRET"),
            (b"NULSECRET\0abcdefgh", "NULSECRET"),
            ("\u{a0}EDGESECRETabcdefg".as_bytes(), "EDGESECRET"),
            ("EDGESECRETabcdefg\u{a0}".as_bytes(), "EDGESECRET"),
            (b"BINSECRET\xff\xfeabcdefg", "BINSECRET"),
        ];
        for (content, needle) in cases {
            let (_d, p) = token_file(content);
            let e = Token::load(&p).unwrap_err();
            assert!(e.contains(p.to_str().unwrap()), "{e}");
            assert!(e.contains("visible ASCII"), "{e}");
            assert!(!e.contains(needle), "{e}");
        }
        let (_d, p) = token_file(b"  \tabcdefghijklmnop0123\r\n");
        assert!(Token::load(&p).unwrap().matches("abcdefghijklmnop0123"));
    }

    #[test]
    fn token_debug_is_redacted() {
        let (_d, p) = token_file(b"abcdefghijklmnop0123");
        let t = Token::load(&p).unwrap();
        let s = format!("{t:?}");
        assert!(!s.contains("abcdefghij"), "{s}");
        assert!(s.contains("redacted"), "{s}");
    }
}
