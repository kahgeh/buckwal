//! The export pipeline behind [`super::init`]: the tracer and logger providers, their
//! processors (`test_run_id` on everything, a time on every log record) and the two
//! filtered layers.
use super::export_filter;
use crate::config::defaults;
use crate::error::{Error, Result};
use opentelemetry::logs::LogRecord as _;
use opentelemetry::trace::{Span as _, TracerProvider as _};
use opentelemetry::{Context, InstrumentationScope, KeyValue};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_sdk::error::OTelSdkResult;
#[cfg(test)]
use opentelemetry_sdk::logs::LogExporter;
use opentelemetry_sdk::logs::{
    self, BatchLogProcessor, LogProcessor, SdkLogRecord, SdkLoggerProvider,
};
#[cfg(test)]
use opentelemetry_sdk::trace::SpanExporter;
use opentelemetry_sdk::trace::{
    self, BatchSpanProcessor, SdkTracerProvider, Span, SpanData, SpanProcessor,
};
use opentelemetry_sdk::Resource;
use std::sync::mpsc;
use std::time::{Duration, Instant, SystemTime};
use tracing::subscriber::Interest;
use tracing::{Level, Metadata, Subscriber};
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::layer::{self as layer_, Filter, Layer, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

/// The service every export names.
const SERVICE: &str = "turso-durable";
/// The instrumentation scope of every span and log record.
const SCOPE: &str = "turso_durable";
/// The span and log attribute that names a test run.
const RUN_ID: &str = "test_run_id";

/// What every export carries.
fn resource() -> Resource {
    Resource::builder().with_service_name(SERVICE).build()
}

/// Adds `test_run_id` to every span as it starts, when one is set, then hands it on.
#[derive(Debug)]
pub(crate) struct RunIdSpans<P> {
    inner: P,
    run_id: Option<String>,
}

impl<P: SpanProcessor> RunIdSpans<P> {
    pub(crate) fn new(inner: P, run_id: Option<String>) -> Self {
        RunIdSpans { inner, run_id }
    }
}

impl<P: SpanProcessor> SpanProcessor for RunIdSpans<P> {
    fn on_start(&self, span: &mut Span, cx: &Context) {
        if let Some(id) = &self.run_id {
            span.set_attribute(KeyValue::new(RUN_ID, id.clone()));
        }
        self.inner.on_start(span, cx);
    }
    fn on_end(&self, span: SpanData) {
        self.inner.on_end(span);
    }
    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }
    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }
    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}

/// Gives every log record a record time and, when one is set, `test_run_id`, then hands it
/// on. A record keeps its own time; without one it gets its observed time, and without that
/// the current time. The tracing bridge never sets a record time, and the collector's
/// ingestor reads only that one.
#[derive(Debug)]
pub(crate) struct StampedLogs<P> {
    inner: P,
    run_id: Option<String>,
}

impl<P: LogProcessor> StampedLogs<P> {
    pub(crate) fn new(inner: P, run_id: Option<String>) -> Self {
        StampedLogs { inner, run_id }
    }
}

impl<P: LogProcessor> LogProcessor for StampedLogs<P> {
    fn emit(&self, record: &mut SdkLogRecord, scope: &InstrumentationScope) {
        if record.timestamp().is_none() {
            let t = record.observed_timestamp().unwrap_or_else(SystemTime::now);
            record.set_timestamp(t);
        }
        if let Some(id) = &self.run_id {
            record.add_attribute(RUN_ID, id.clone());
        }
        self.inner.emit(record, scope);
    }
    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }
    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }
    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}

/// What one export layer takes: `buckwal*` targets only, spans (for the span layer)
/// or events (for the log bridge) at `max` and above. It depends on the callsite alone, so
/// tracing caches its answer per callsite.
#[derive(Debug, Clone, Copy)]
struct ExportFilter {
    spans: bool,
    max: Level,
}

impl ExportFilter {
    fn wants(&self, meta: &Metadata<'_>) -> bool {
        let kind = if self.spans {
            meta.is_span()
        } else {
            meta.is_event()
        };
        kind && *meta.level() <= self.max && export_filter()(meta.target())
    }
}

impl<S> Filter<S> for ExportFilter {
    fn enabled(&self, meta: &Metadata<'_>, _: &layer_::Context<'_, S>) -> bool {
        self.wants(meta)
    }
    fn callsite_enabled(&self, meta: &'static Metadata<'static>) -> Interest {
        if self.wants(meta) {
            Interest::always()
        } else {
            Interest::never()
        }
    }
    fn max_level_hint(&self) -> Option<LevelFilter> {
        Some(LevelFilter::from_level(self.max))
    }
}

/// Stops one provider.
type ProviderStop = Box<dyn FnOnce() + Send>;

/// The tracer and logger providers.
pub(crate) struct Providers {
    tracer: SdkTracerProvider,
    logger: SdkLoggerProvider,
}

impl Providers {
    fn new(spans: impl SpanProcessor + 'static, logs: impl LogProcessor + 'static) -> Self {
        Providers {
            tracer: SdkTracerProvider::builder()
                .with_resource(resource())
                .with_span_processor(spans)
                .build(),
            logger: SdkLoggerProvider::builder()
                .with_resource(resource())
                .with_log_processor(logs)
                .build(),
        }
    }

    /// OTLP/gRPC to `endpoint` through batch processors with bounded queues, and the
    /// one-worker runtime the tonic exporters run on.
    pub(crate) fn otlp(
        endpoint: &str,
        run_id: Option<String>,
    ) -> Result<(Providers, tokio::runtime::Runtime)> {
        use opentelemetry_otlp::WithExportConfig as _;
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("buckwal-otel")
            .enable_all()
            .build()
            .map_err(|e| Error::Io(format!("telemetry runtime: {e}")))?;
        let exporters = {
            let _rt = rt.enter();
            opentelemetry_otlp::SpanExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint)
                .with_timeout(defaults::OTEL_EXPORT_TIMEOUT)
                .build()
                .and_then(|spans| {
                    let logs = opentelemetry_otlp::LogExporter::builder()
                        .with_tonic()
                        .with_endpoint(endpoint)
                        .with_timeout(defaults::OTEL_EXPORT_TIMEOUT)
                        .build()?;
                    Ok((spans, logs))
                })
        };
        let (spans, logs) = match exporters {
            Ok(x) => x,
            Err(e) => {
                // A plain drop of the runtime panics when the caller is in an async context.
                rt.shutdown_background();
                return Err(Error::Config(format!("OTLP exporter for {endpoint}: {e}")));
            }
        };
        let spans = BatchSpanProcessor::builder(spans)
            .with_batch_config(
                trace::BatchConfigBuilder::default()
                    .with_max_queue_size(defaults::OTEL_QUEUE)
                    .build(),
            )
            .build();
        let logs = BatchLogProcessor::builder(logs)
            .with_batch_config(
                logs::BatchConfigBuilder::default()
                    .with_max_queue_size(defaults::OTEL_QUEUE)
                    .build(),
            )
            .build();
        let providers = Providers::new(
            RunIdSpans::new(spans, run_id.clone()),
            StampedLogs::new(logs, run_id),
        );
        Ok((providers, rt))
    }

    /// The two export layers: spans become OpenTelemetry spans, events at `level` and above
    /// become log records.
    fn layers<S>(&self, level: Level) -> impl Layer<S>
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        let spans = tracing_opentelemetry::layer()
            .with_tracer(self.tracer.tracer(SCOPE))
            .with_location(false)
            .with_tracked_inactivity(false)
            .with_filter(ExportFilter {
                spans: true,
                max: Level::INFO,
            });
        let logs = OpenTelemetryTracingBridge::new(&self.logger).with_filter(ExportFilter {
            spans: false,
            max: level,
        });
        spans.and_then(logs)
    }

    /// A registry with the export layers.
    pub(crate) fn subscriber(&self, level: Level) -> impl Subscriber + Send + Sync + 'static {
        tracing_subscriber::registry().with(self.layers(level))
    }

    /// Send everything queued now.
    #[cfg(test)]
    pub(crate) fn force_flush(&self) {
        let spans = self.tracer.force_flush();
        let logs = self.logger.force_flush();
        spans.and(logs).expect("flush");
    }

    /// Stop both providers together, each sending what it has queued, and return once they
    /// have stopped or `budget` has passed. A provider still stopping then carries on in the
    /// background.
    pub(crate) fn shutdown_within(&self, budget: Duration) {
        let deadline = Instant::now() + budget;
        let tracer = self.tracer.clone();
        let logger = self.logger.clone();
        let stops: [(&str, ProviderStop); 2] = [
            (
                "spans",
                Box::new(move || drop(tracer.shutdown_with_timeout(budget))),
            ),
            (
                "logs",
                Box::new(move || drop(logger.shutdown_with_timeout(budget))),
            ),
        ];
        let (stopped, stopped_signals) = mpsc::channel::<()>();
        let mut stopping = 0;
        for (name, stop) in stops {
            let stopped = stopped.clone();
            let spawned = std::thread::Builder::new()
                .name(format!("buckwal-otel-stop-{name}"))
                .spawn(move || {
                    stop();
                    let _ = stopped.send(());
                });
            if spawned.is_ok() {
                stopping += 1;
            }
        }
        for _ in 0..stopping {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if stopped_signals.recv_timeout(remaining).is_err() {
                return;
            }
        }
    }
}

/// The pipeline over any exporters (the in-memory ones), at the default level, sending each
/// span and log record as it ends, on the thread that ends it.
#[cfg(test)]
pub(crate) fn build_for_test(
    spans: impl SpanExporter + 'static,
    logs: impl LogExporter + 'static,
    run_id: Option<String>,
) -> (impl Subscriber + Send + Sync + 'static, Providers) {
    let providers = Providers::new(
        RunIdSpans::new(trace::SimpleSpanProcessor::new(spans), run_id.clone()),
        StampedLogs::new(logs::SimpleLogProcessor::new(logs), run_id),
    );
    (providers.subscriber(defaults::OTEL_LEVEL), providers)
}
