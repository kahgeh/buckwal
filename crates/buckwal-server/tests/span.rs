//! The `buckwal.server.request` span, in its own test binary: the capturing subscriber is
//! the process-wide default, because the server runs on tokio worker and blocking threads.
mod common;

use buckwal_hrana_client::{execute, Arg, Proto};
use common::{start, Opts, TOKEN};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

#[derive(Debug, Clone)]
struct SpanRec {
    id: u64,
    parent: Option<u64>,
    name: String,
    fields: BTreeMap<String, String>,
}

#[derive(Default, Clone)]
struct Capture {
    spans: Arc<Mutex<Vec<SpanRec>>>,
    /// Every event's fields, as `name=value` strings.
    events: Arc<Mutex<Vec<String>>>,
}

struct Fields<'a>(&'a mut BTreeMap<String, String>);

impl Visit for Fields<'_> {
    fn record_str(&mut self, f: &Field, v: &str) {
        self.0.insert(f.name().to_string(), v.to_string());
    }
    fn record_debug(&mut self, f: &Field, v: &dyn std::fmt::Debug) {
        self.0.insert(f.name().to_string(), format!("{v:?}"));
    }
}

impl<S> Layer<S> for Capture
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    /// Only `buckwal*` targets, as the exporter's filter: turso_core's own spans are
    /// then never created, so the commit span's parent is the span the caller is in.
    fn enabled(&self, metadata: &Metadata<'_>, _ctx: Context<'_, S>) -> bool {
        metadata.target().starts_with("buckwal")
    }

    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let mut fields = BTreeMap::new();
        attrs.record(&mut Fields(&mut fields));
        let parent = ctx
            .span(id)
            .and_then(|s| s.parent())
            .map(|p| p.id().into_u64());
        self.spans.lock().unwrap().push(SpanRec {
            id: id.into_u64(),
            parent,
            name: attrs.metadata().name().to_string(),
            fields,
        });
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, _ctx: Context<'_, S>) {
        let mut spans = self.spans.lock().unwrap();
        // Span ids are reused after close; the latest span with the id is the live one.
        if let Some(s) = spans.iter_mut().rev().find(|s| s.id == id.into_u64()) {
            values.record(&mut Fields(&mut s.fields));
        }
    }

    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut fields = BTreeMap::new();
        event.record(&mut Fields(&mut fields));
        let mut events = self.events.lock().unwrap();
        for (k, v) in fields {
            events.push(format!("{k}={v}"));
        }
    }
}

#[test]
fn request_span_has_no_sql_or_token() {
    let capture = Capture::default();
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(capture.clone()))
        .unwrap();
    let marker = "MARKER-5d1c0e7a";
    let s = start(Opts {
        token: true,
        ..Opts::default()
    });
    let c = s.client(Some(TOKEN));
    let mut st = c.stream(Proto::V2);
    let r = st
        .pipeline(vec![execute("CREATE TABLE t(x TEXT, y TEXT)", &[])])
        .unwrap();
    assert_eq!(r.status, 200, "{}", r.body);
    let from = capture.spans.lock().unwrap().len();
    let sql = format!("INSERT INTO t VALUES ('{marker}', ?)");
    let r = st
        .pipeline(vec![execute(&sql, &[Arg::Text(marker.into())])])
        .unwrap();
    assert_eq!(r.status, 200, "{}", r.body);
    assert_eq!(r.body["results"][0]["type"], "ok", "{}", r.body);
    let baton = st.baton().unwrap().to_string();
    let refused_from = capture.spans.lock().unwrap().len();
    let body = serde_json::json!({"baton": null, "requests": [execute(&sql, &[])]});
    let r = c
        .post_raw(
            "/v2/pipeline",
            body.to_string().into_bytes(),
            Some(&format!("Bearer {TOKEN}x")),
        )
        .unwrap();
    assert_eq!(r.status, 401, "{}", r.body);
    let keyed_from = capture.spans.lock().unwrap().len();
    let key = "KEY-VALUE-9b3f27c4";
    let keyed = reqwest::blocking::Client::builder()
        .no_proxy()
        .build()
        .unwrap()
        .post(format!("{}/v3/pipeline", s.url()))
        .header("Authorization", format!("Bearer {TOKEN}"))
        .header("x-turso-encryption-key", key)
        .body(body.to_string())
        .send()
        .unwrap();
    assert_eq!(keyed.status().as_u16(), 400);
    s.stop().unwrap();

    let spans = capture.spans.lock().unwrap().clone();
    let reqs: Vec<&SpanRec> = spans[from..refused_from]
        .iter()
        .filter(|s| s.name == "buckwal.server.request")
        .collect();
    assert_eq!(reqs.len(), 1, "{reqs:?}");
    let req = reqs[0];
    let f = |k: &str| req.fields.get(k).map(String::as_str);
    assert_eq!(f("http.route"), Some("/v2/pipeline"), "{req:?}");
    assert_eq!(f("http.status"), Some("200"), "{req:?}");
    assert_eq!(f("buckwal.statements"), Some("1"), "{req:?}");
    assert_eq!(f("buckwal.outcome"), Some("ok"), "{req:?}");
    let stream: u64 = f("buckwal.stream").unwrap().parse().unwrap();
    assert!(stream >= 1, "{req:?}");
    // Only the spans opened during the INSERT request: span ids are reused once closed.
    let during = &spans[from..refused_from];
    assert!(
        during
            .iter()
            .any(|s| s.name == "buckwal.commit" && s.parent == Some(req.id)),
        "the library's commit span is a child of the request span: {during:?}"
    );
    let refused: Vec<&SpanRec> = spans[refused_from..keyed_from]
        .iter()
        .filter(|s| s.name == "buckwal.server.request")
        .collect();
    assert_eq!(refused.len(), 1, "{refused:?}");
    let g = |k: &str| refused[0].fields.get(k).map(String::as_str);
    assert_eq!(g("http.status"), Some("401"), "{refused:?}");
    assert_eq!(g("buckwal.outcome"), Some("unauthorized"), "{refused:?}");
    assert_eq!(g("buckwal.stream"), None, "{refused:?}");
    let keyed: Vec<&SpanRec> = spans[keyed_from..]
        .iter()
        .filter(|s| s.name == "buckwal.server.request")
        .collect();
    assert_eq!(keyed.len(), 1, "{keyed:?}");
    let k = |f: &str| keyed[0].fields.get(f).map(String::as_str);
    assert_eq!(k("http.status"), Some("400"), "{keyed:?}");
    assert_eq!(k("buckwal.outcome"), Some("bad_request"), "{keyed:?}");
    assert_eq!(k("buckwal.stream"), None, "{keyed:?}");
    let secret_part = baton.split_once('.').unwrap().1.to_string();
    let events = capture.events.lock().unwrap().clone();
    let values = spans
        .iter()
        .flat_map(|s| s.fields.values().cloned())
        .chain(events);
    for v in values {
        for needle in [marker, TOKEN, &baton, &secret_part, key] {
            assert!(!v.contains(needle), "telemetry holds {needle:?}: {v}");
        }
    }
}
