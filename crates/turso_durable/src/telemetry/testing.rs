//! A capture [`Layer`] for tests: every span (name, target, parent, links, fields, when it
//! was created and closed) and every event, on any thread running under its subscriber.
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::Instant;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::subscriber::Interest;
use tracing::{Dispatch, Event, Level, Metadata, Subscriber};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

/// One captured span. `id` is its index in capture order (span ids are reused once closed).
#[derive(Debug, Clone)]
pub(crate) struct SpanRec {
    pub id: usize,
    pub name: &'static str,
    pub target: String,
    pub level: Level,
    pub parent: Option<usize>,
    pub parent_name: Option<&'static str>,
    pub follows_from: Vec<usize>,
    pub fields: HashMap<String, String>,
    pub created: Instant,
    pub closed: Option<Instant>,
}

impl SpanRec {
    /// A recorded field's value: `Display` and `str` values as text, others as `Debug`.
    pub(crate) fn field(&self, name: &str) -> Option<&str> {
        self.fields.get(name).map(String::as_str)
    }
}

/// One captured event; `message` is among its fields.
#[derive(Debug, Clone)]
pub(crate) struct EventRec {
    pub level: Level,
    pub target: String,
    /// The span it was emitted in.
    #[allow(dead_code)]
    pub span: Option<usize>,
    pub fields: HashMap<String, String>,
}

impl EventRec {
    pub(crate) fn field(&self, name: &str) -> Option<&str> {
        self.fields.get(name).map(String::as_str)
    }
}

#[derive(Default)]
struct State {
    spans: Vec<SpanRec>,
    events: Vec<EventRec>,
    /// Open span ids to their index.
    live: HashMap<u64, usize>,
}

/// Records everything it sees; [`Capture::spans`] takes a snapshot.
#[derive(Clone, Default)]
pub(crate) struct Capture {
    state: Arc<Mutex<State>>,
}

/// A subscriber that wants nothing but asks to be consulted at every call.
struct Idle;

impl Subscriber for Idle {
    fn register_callsite(&self, _: &'static Metadata<'static>) -> Interest {
        Interest::sometimes()
    }
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        false
    }
    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }
    fn record(&self, _: &Id, _: &Record<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn event(&self, _: &Event<'_>) {}
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}
}

/// tracing caches each callsite's interest. While at most one dispatcher is registered
/// (tracing-core's `Dispatchers::has_just_one`), a callsite first hit asks only that thread's
/// subscriber, so one first hit on a thread without a capture (another test's uploader, say)
/// is cached as `never` and hidden from every capture until the next dispatcher registers.
/// Two idle dispatchers that live for the whole test process keep that state from ever
/// returning: every callsite asks all registered dispatchers, and the idle ones' `sometimes`
/// makes each call check the current thread's subscriber.
fn keep_interest_per_call() {
    static IDLE: OnceLock<[Dispatch; 2]> = OnceLock::new();
    IDLE.get_or_init(|| [Dispatch::new(Idle), Dispatch::new(Idle)]);
}

impl Capture {
    pub(crate) fn new() -> Capture {
        keep_interest_per_call();
        Capture::default()
    }

    /// A registry with this layer, for `tracing::subscriber::with_default`.
    pub(crate) fn subscriber(&self) -> impl Subscriber + Send + Sync + 'static {
        tracing_subscriber::registry().with(self.clone())
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Everything captured so far.
    pub(crate) fn spans(&self) -> Spans {
        let s = self.lock();
        Spans {
            spans: s.spans.clone(),
            events: s.events.clone(),
        }
    }
}

/// A snapshot of a [`Capture`].
#[derive(Debug, Clone)]
pub(crate) struct Spans {
    pub spans: Vec<SpanRec>,
    pub events: Vec<EventRec>,
}

impl Spans {
    pub(crate) fn named(&self, name: &str) -> Vec<&SpanRec> {
        self.spans.iter().filter(|s| s.name == name).collect()
    }

    /// The last span called `name` to be created.
    pub(crate) fn last_named(&self, name: &str) -> &SpanRec {
        self.spans
            .iter()
            .rev()
            .find(|s| s.name == name)
            .unwrap_or_else(|| panic!("no span named {name}"))
    }

    /// The ids of `parent`'s children called `name`.
    pub(crate) fn children_of(&self, parent: usize, name: &str) -> Vec<usize> {
        self.spans
            .iter()
            .filter(|s| s.name == name && s.parent == Some(parent))
            .map(|s| s.id)
            .collect()
    }

    /// The one child of `parent` called `name`.
    pub(crate) fn with_parent(&self, parent: usize, name: &str) -> &SpanRec {
        let ids = self.children_of(parent, name);
        assert_eq!(ids.len(), 1, "{name} spans under span {parent}: {ids:?}");
        self.by_id(ids[0])
    }

    pub(crate) fn by_id(&self, id: usize) -> &SpanRec {
        &self.spans[id]
    }
}

struct Fields<'a>(&'a mut HashMap<String, String>);

impl Visit for Fields<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_string(), value.to_string());
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().to_string(), format!("{value:?}"));
    }
}

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Capture {
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let mut fields = HashMap::new();
        attrs.record(&mut Fields(&mut fields));
        let parent_id = ctx.span(id).and_then(|s| s.parent()).map(|p| p.id());
        let meta = attrs.metadata();
        let mut st = self.lock();
        let parent = parent_id.and_then(|p| st.live.get(&p.into_u64()).copied());
        let rec = SpanRec {
            id: st.spans.len(),
            name: meta.name(),
            target: meta.target().to_string(),
            level: *meta.level(),
            parent,
            parent_name: parent.map(|i| st.spans[i].name),
            follows_from: Vec::new(),
            fields,
            created: Instant::now(),
            closed: None,
        };
        st.live.insert(id.into_u64(), rec.id);
        st.spans.push(rec);
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, _: Context<'_, S>) {
        let mut st = self.lock();
        if let Some(&i) = st.live.get(&id.into_u64()) {
            values.record(&mut Fields(&mut st.spans[i].fields));
        }
    }

    fn on_follows_from(&self, span: &Id, follows: &Id, _: Context<'_, S>) {
        let mut st = self.lock();
        if let (Some(&i), Some(&f)) = (
            st.live.get(&span.into_u64()),
            st.live.get(&follows.into_u64()),
        ) {
            st.spans[i].follows_from.push(f);
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let mut fields = HashMap::new();
        event.record(&mut Fields(&mut fields));
        let span_id = ctx.event_span(event).map(|s| s.id());
        let meta = event.metadata();
        let mut st = self.lock();
        let span = span_id.and_then(|s| st.live.get(&s.into_u64()).copied());
        st.events.push(EventRec {
            level: *meta.level(),
            target: meta.target().to_string(),
            span,
            fields,
        });
    }

    fn on_close(&self, id: Id, _: Context<'_, S>) {
        let mut st = self.lock();
        if let Some(i) = st.live.remove(&id.into_u64()) {
            st.spans[i].closed = Some(Instant::now());
        }
    }
}
