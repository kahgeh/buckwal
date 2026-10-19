//! Test-only blocking client of tursodb's SQL over HTTP v3. Builds request JSON by hand and
//! never uses the server crate's types. Most builders copy the shapes libSQL's clients send,
//! which the protocol (it descends from libSQL's Hrana) still accepts; `describe` is
//! tursodb's. The crate's name is historical: renaming it would change the audited Cargo.lock.

use base64::Engine as _;
use serde_json::{json, Map, Value};
use std::path::Path;
use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Proto {
    V2,
    V3,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Arg {
    Null,
    Int(i64),
    Float(f64),
    Text(String),
    Blob(Vec<u8>),
}

#[derive(Clone, Debug)]
pub struct Reply {
    pub status: u16,
    pub body: Value,
}

pub struct Client {
    http: reqwest::blocking::Client,
    base: String,
    token: Option<String>,
}

pub struct Stream<'a> {
    client: &'a Client,
    proto: Proto,
    baton: Option<String>,
}

impl Client {
    pub fn new(base: &str, token: Option<&str>, ca_pem: Option<&Path>) -> Result<Client, String> {
        // reqwest's rustls backend needs a process crypto provider; an earlier install wins.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let mut b = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(60))
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy();
        if let Some(path) = ca_pem {
            let pem = std::fs::read(path).map_err(|e| format!("CA {}: {e}", path.display()))?;
            let certs = reqwest::Certificate::from_pem_bundle(&pem)
                .map_err(|e| format!("CA {}: {e}", path.display()))?;
            if certs.is_empty() {
                return Err(format!("CA {}: no certificate in the file", path.display()));
            }
            for c in certs {
                b = b.add_root_certificate(c);
            }
        }
        Ok(Client {
            http: b.build().map_err(|e| e.to_string())?,
            base: base.trim_end_matches('/').to_string(),
            token: token.map(str::to_string),
        })
    }

    pub fn stream(&self, v: Proto) -> Stream<'_> {
        Stream {
            client: self,
            proto: v,
            baton: None,
        }
    }

    fn send(
        &self,
        req: reqwest::blocking::RequestBuilder,
        auth: Option<&str>,
    ) -> Result<(u16, String), String> {
        let req = match auth {
            Some(a) => req.header("Authorization", a),
            None => req,
        };
        let resp = req.send().map_err(|e| e.to_string())?;
        let status = resp.status().as_u16();
        let text = resp.text().map_err(|e| e.to_string())?;
        Ok((status, text))
    }

    fn default_auth(&self) -> Option<String> {
        self.token.as_ref().map(|t| format!("Bearer {t}"))
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base, path)
    }

    pub fn get(&self, path: &str) -> Result<Reply, String> {
        let auth = self.default_auth();
        let (status, text) = self.send(self.http.get(self.url(path)), auth.as_deref())?;
        Ok(reply(status, &text))
    }

    /// POSTs `body` as-is. `auth` is the whole Authorization header value, replacing the client's token.
    pub fn post_raw(&self, path: &str, body: Vec<u8>, auth: Option<&str>) -> Result<Reply, String> {
        let req = self
            .http
            .post(self.url(path))
            .header("Content-Type", "application/json")
            .body(body);
        let (status, text) = self.send(req, auth)?;
        Ok(reply(status, &text))
    }

    fn post_json(&self, path: &str, body: &Value) -> Result<(u16, String), String> {
        let auth = self.default_auth();
        let req = self
            .http
            .post(self.url(path))
            .header("Content-Type", "application/json")
            .body(body.to_string());
        self.send(req, auth.as_deref())
    }
}

fn reply(status: u16, text: &str) -> Reply {
    let body = serde_json::from_str(text).unwrap_or_else(|_| json!({ "raw": text }));
    Reply { status, body }
}

impl Stream<'_> {
    pub fn baton(&self) -> Option<&str> {
        self.baton.as_deref()
    }

    pub fn set_baton(&mut self, b: Option<String>) {
        self.baton = b;
    }

    pub fn pipeline(&mut self, requests: Vec<Value>) -> Result<Reply, String> {
        let path = match self.proto {
            Proto::V2 => "/v2/pipeline",
            Proto::V3 => "/v3/pipeline",
        };
        let body = json!({"baton": self.baton, "requests": requests});
        let (status, text) = self.client.post_json(path, &body)?;
        let r = reply(status, &text);
        if status == 200 {
            self.baton = r
                .body
                .get("baton")
                .and_then(Value::as_str)
                .map(str::to_string);
        }
        Ok(r)
    }

    /// v3 only. Returns the status and every NDJSON entry after the first line.
    pub fn cursor(&mut self, batch: Value) -> Result<(u16, Vec<Value>), String> {
        let body = json!({"baton": self.baton, "batch": batch});
        let (status, text) = self.client.post_json("/v3/cursor", &body)?;
        if status != 200 {
            return Ok((status, vec![reply(status, &text).body]));
        }
        let (baton, entries) = split_ndjson(&text)?;
        self.baton = baton;
        Ok((status, entries))
    }
}

/// Splits a cursor body: the first line's baton, then the remaining entries.
fn split_ndjson(body: &str) -> Result<(Option<String>, Vec<Value>), String> {
    let mut lines = body.lines().filter(|l| !l.trim().is_empty());
    let first = lines.next().ok_or("empty cursor body")?;
    let first: Value = serde_json::from_str(first).map_err(|e| format!("first line: {e}"))?;
    let baton = first
        .get("baton")
        .and_then(Value::as_str)
        .map(str::to_string);
    let entries = lines
        .map(|l| serde_json::from_str(l).map_err(|e| format!("line {l:?}: {e}")))
        .collect::<Result<Vec<Value>, String>>()?;
    Ok((baton, entries))
}

fn value(a: &Arg) -> Value {
    match a {
        Arg::Null => json!({"type":"null"}),
        Arg::Int(i) => json!({"type":"integer","value":i.to_string()}),
        Arg::Float(f) => json!({"type":"float","value":f}),
        Arg::Text(t) => json!({"type":"text","value":t}),
        Arg::Blob(b) => {
            json!({"type":"blob","base64":base64::engine::general_purpose::STANDARD.encode(b)})
        }
    }
}

pub fn stmt(sql: &str, args: &[Arg]) -> Value {
    json!({"sql":sql,"args":args.iter().map(value).collect::<Vec<_>>(),"want_rows":true})
}

pub fn execute(sql: &str, args: &[Arg]) -> Value {
    json!({"type":"execute","stmt":stmt(sql, args)})
}

pub fn execute_named(sql: &str, named: &[(&str, Arg)]) -> Value {
    let mut s = stmt(sql, &[]);
    let named: Vec<Value> = named
        .iter()
        .map(|(n, a)| json!({"name":n,"value":value(a)}))
        .collect();
    s.as_object_mut()
        .expect("object")
        .insert("named_args".into(), Value::Array(named));
    json!({"type":"execute","stmt":s})
}

fn batch_body(steps: Vec<(Option<Value>, Value)>) -> Value {
    let steps: Vec<Value> = steps
        .into_iter()
        .map(|(c, s)| {
            let mut m = Map::new();
            if let Some(c) = c {
                m.insert("condition".into(), c);
            }
            m.insert("stmt".into(), s);
            Value::Object(m)
        })
        .collect();
    json!({"steps": steps})
}

pub fn batch(steps: Vec<(Option<Value>, Value)>) -> Value {
    json!({"type":"batch","batch":batch_body(steps)})
}

pub fn cond_ok(step: usize) -> Value {
    json!({"type":"ok","step":step})
}

pub fn cond_not(c: Value) -> Value {
    json!({"type":"not","cond":c})
}

pub fn cond_and(cs: Vec<Value>) -> Value {
    json!({"type":"and","conds":cs})
}

pub fn cond_is_autocommit() -> Value {
    json!({"type":"is_autocommit"})
}

pub fn sequence(sql: &str) -> Value {
    json!({"type":"sequence","sql":sql})
}

pub fn store_sql(id: i64, sql: &str) -> Value {
    json!({"type":"store_sql","sql_id":id,"sql":sql})
}

pub fn close() -> Value {
    json!({"type":"close"})
}

pub fn get_autocommit() -> Value {
    json!({"type":"get_autocommit"})
}

/// tursodb's serverless drivers send this before every prepare (PROTOCOL.md 6.4).
pub fn describe(sql: &str) -> Value {
    json!({"type":"describe","sql":sql})
}

/// The batch body (`{"steps":[...]}`) of @libsql/client 0.15.15 hrana.js transactions.
/// Usable as a cursor `batch`; [`tx_batch_request`] wraps it for a pipeline.
pub fn tx_batch(stmts: &[Value]) -> Value {
    let mut steps = vec![(None, stmt("BEGIN", &[]))];
    for (i, s) in stmts.iter().enumerate() {
        steps.push((Some(cond_ok(i)), s.clone()));
    }
    let commit = stmts.len() + 1;
    steps.push((Some(cond_ok(stmts.len())), stmt("COMMIT", &[])));
    steps.push((Some(cond_not(cond_ok(commit))), stmt("ROLLBACK", &[])));
    batch_body(steps)
}

/// The pipeline request of @libsql/client 0.15.15 hrana.js transactions: [`tx_batch`] as a
/// `batch` request.
pub fn tx_batch_request(stmts: &[Value]) -> Value {
    json!({"type":"batch","batch": tx_batch(stmts)})
}

fn from_hrana(v: &Value) -> Result<Value, String> {
    let b64 = base64::engine::general_purpose::STANDARD_NO_PAD;
    match v.get("type").and_then(Value::as_str) {
        Some("null") => Ok(Value::Null),
        Some("integer") => {
            let s = v
                .get("value")
                .and_then(Value::as_str)
                .ok_or("integer value")?;
            s.parse::<i64>()
                .map(|i| json!(i))
                .map_err(|e| e.to_string())
        }
        Some("float") => v.get("value").cloned().ok_or_else(|| "float value".into()),
        Some("text") => v.get("value").cloned().ok_or_else(|| "text value".into()),
        Some("blob") => {
            let s = v
                .get("base64")
                .and_then(Value::as_str)
                .ok_or("blob base64")?;
            let bytes = b64
                .decode(s.trim_end_matches('='))
                .map_err(|e| e.to_string())?;
            Ok(json!(bytes))
        }
        _ => Err(format!("unknown value {v}")),
    }
}

/// Rows of `results[i].response.result.rows`, blobs as arrays of byte numbers.
pub fn result_rows(reply: &Reply, i: usize) -> Result<Vec<Vec<Value>>, String> {
    let rows = reply
        .body
        .pointer(&format!("/results/{i}/response/result/rows"))
        .and_then(Value::as_array)
        .ok_or_else(|| format!("no rows in result {i}: {}", reply.body))?;
    rows.iter()
        .map(|r| {
            r.as_array()
                .ok_or("row not an array")?
                .iter()
                .map(from_hrana)
                .collect()
        })
        .collect()
}

/// `(message, code)` when `results[i]` is an error.
pub fn result_error(reply: &Reply, i: usize) -> Option<(String, Option<String>)> {
    let r = reply.body.pointer(&format!("/results/{i}"))?;
    if r.get("type")?.as_str()? != "error" {
        return None;
    }
    let e = r.get("error")?;
    Some((
        e.get("message")?.as_str()?.to_string(),
        e.get("code").and_then(Value::as_str).map(str::to_string),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn builders_match_client_shapes() {
        // Shapes copied from libSQL's clients (provenance). SQL over HTTP v3 descends from
        // libSQL's Hrana, and these shapes remain accepted.
        // @libsql/hrana-client 0.9.0, v2 encoding of an execute request.
        assert_eq!(
            execute("SELECT ?", &[Arg::Int(1)]),
            json!({"type":"execute","stmt":{"sql":"SELECT ?","args":[{"type":"integer","value":"1"}],"want_rows":true}})
        );
        // @libsql/hrana-client 0.9.0: named args are {name, value}.
        assert_eq!(
            execute_named("SELECT :a", &[("a", Arg::Text("x".into()))]),
            json!({"type":"execute","stmt":{"sql":"SELECT :a","args":[],"named_args":[{"name":"a","value":{"type":"text","value":"x"}}],"want_rows":true}})
        );
        // @libsql/hrana-client 0.9.0: values.
        assert_eq!(
            stmt("x", &[Arg::Null, Arg::Float(1.5), Arg::Blob(vec![1, 2])])["args"],
            json!([{"type":"null"},{"type":"float","value":1.5},{"type":"blob","base64":"AQI="}])
        );
        // libsql Rust crate 0.10.0: i64::MIN goes as a string.
        assert_eq!(
            stmt("x", &[Arg::Int(i64::MIN)])["args"],
            json!([{"type":"integer","value":"-9223372036854775808"}])
        );
        // @libsql/hrana-client 0.9.0: store_sql, sequence, close, get_autocommit.
        assert_eq!(
            store_sql(1, "SELECT 1"),
            json!({"type":"store_sql","sql_id":1,"sql":"SELECT 1"})
        );
        assert_eq!(sequence("A;B"), json!({"type":"sequence","sql":"A;B"}));
        assert_eq!(close(), json!({"type":"close"}));
        assert_eq!(get_autocommit(), json!({"type":"get_autocommit"}));
        // tursodb SQL over HTTP v3 (PROTOCOL.md 6.4): describe.
        assert_eq!(
            describe("SELECT 1"),
            json!({"type":"describe","sql":"SELECT 1"})
        );
        // @libsql/hrana-client 0.9.0: batch conditions.
        let s = stmt("S", &[]);
        assert_eq!(
            batch(vec![
                (None, s.clone()),
                (
                    Some(cond_and(vec![cond_ok(0), cond_not(cond_is_autocommit())])),
                    s.clone()
                ),
            ]),
            json!({"type":"batch","batch":{"steps":[
                {"stmt":{"sql":"S","args":[],"want_rows":true}},
                {"condition":{"type":"and","conds":[{"type":"ok","step":0},{"type":"not","cond":{"type":"is_autocommit"}}]},
                 "stmt":{"sql":"S","args":[],"want_rows":true}}
            ]}})
        );
    }

    #[test]
    fn tx_batch_matches_hrana_js() {
        // @libsql/client 0.15.15 hrana.js: BEGIN; each stmt ok(prev); COMMIT ok(last); ROLLBACK not(ok(commit)).
        let a = stmt("A", &[]);
        let b = stmt("B", &[]);
        assert_eq!(
            tx_batch(&[a, b]),
            json!({"steps":[
                {"stmt":{"sql":"BEGIN","args":[],"want_rows":true}},
                {"condition":{"type":"ok","step":0},"stmt":{"sql":"A","args":[],"want_rows":true}},
                {"condition":{"type":"ok","step":1},"stmt":{"sql":"B","args":[],"want_rows":true}},
                {"condition":{"type":"ok","step":2},"stmt":{"sql":"COMMIT","args":[],"want_rows":true}},
                {"condition":{"type":"not","cond":{"type":"ok","step":3}},"stmt":{"sql":"ROLLBACK","args":[],"want_rows":true}}
            ]})
        );
    }

    #[test]
    fn tx_batch_request_wraps_the_body() {
        let a = stmt("A", &[]);
        assert_eq!(
            tx_batch_request(std::slice::from_ref(&a)),
            json!({"type":"batch","batch": tx_batch(&[a])})
        );
    }

    #[test]
    fn ca_file_without_a_certificate_is_refused() {
        let dir = std::env::temp_dir().join(format!("hrana-client-ca-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("empty.pem");
        std::fs::write(&path, b"not a certificate\n").unwrap();
        let r = Client::new("https://localhost:1", None, Some(&path));
        std::fs::remove_file(&path).unwrap();
        std::fs::remove_dir(&dir).unwrap();
        let e = r.err().expect("an empty CA bundle must be refused");
        assert!(e.contains("no certificate"), "{e}");
    }

    #[test]
    fn results_parse() {
        let reply = Reply {
            status: 200,
            body: json!({"results":[
                {"type":"ok","response":{"type":"execute","result":{"rows":[[
                    {"type":"null"},{"type":"integer","value":"7"},{"type":"float","value":2.5},
                    {"type":"text","value":"t"},{"type":"blob","base64":"AQI="}]]}}},
                {"type":"error","error":{"message":"boom","code":"SQLITE_ERROR"}},
                {"type":"error","error":{"message":"nocode"}}
            ]}),
        };
        assert_eq!(
            result_rows(&reply, 0).unwrap(),
            vec![vec![
                json!(null),
                json!(7),
                json!(2.5),
                json!("t"),
                json!([1, 2])
            ]]
        );
        assert!(result_rows(&reply, 1).is_err());
        assert_eq!(result_error(&reply, 0), None);
        assert_eq!(
            result_error(&reply, 1),
            Some(("boom".into(), Some("SQLITE_ERROR".into())))
        );
        assert_eq!(result_error(&reply, 2), Some(("nocode".into(), None)));
    }

    #[test]
    fn ndjson_split() {
        let body = "{\"baton\":\"b\",\"base_url\":null}\n{\"type\":\"step_begin\",\"step\":0}\n\n{\"type\":\"step_end\"}\n";
        let (baton, entries) = split_ndjson(body).unwrap();
        assert_eq!(baton, Some("b".to_string()));
        assert_eq!(
            entries,
            vec![
                json!({"type":"step_begin","step":0}),
                json!({"type":"step_end"})
            ]
        );
    }
}
