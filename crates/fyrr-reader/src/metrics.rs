//! Process-wide metrics in the Prometheus text format, served on an opt-in
//! listener by the reader (`reader.http.metrics_listen`) and the writer
//! (`writer.serve.metrics_listen`).
//!
//! The endpoint has no authentication, so a label value is only ever a fixed
//! word or a number of the code: never a namespace, a principal, a document or
//! a query. Each label has a handful of values, which bounds the series.
use axum::{Router, http::header, routing::get};
use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::Mutex;
use std::time::Duration;

#[derive(Clone, Copy)]
enum Kind {
    Counter,
    Gauge,
    Histogram,
}

/// One metric family: its name, help text and kind.
pub struct Metric {
    name: &'static str,
    help: &'static str,
    kind: Kind,
}
impl Metric {
    pub const fn counter(name: &'static str, help: &'static str) -> Self {
        Self {
            name,
            help,
            kind: Kind::Counter,
        }
    }
    pub const fn gauge(name: &'static str, help: &'static str) -> Self {
        Self {
            name,
            help,
            kind: Kind::Gauge,
        }
    }
    pub const fn histogram(name: &'static str, help: &'static str) -> Self {
        Self {
            name,
            help,
            kind: Kind::Histogram,
        }
    }
}

/// Upper bounds of every histogram, in seconds; one more slot counts the rest.
const BOUNDS: [f64; 13] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 120.0,
];

enum Value {
    Number(i64),
    Histogram {
        counts: [u64; BOUNDS.len() + 1],
        sum: f64,
    },
}
struct Family {
    metric: &'static Metric,
    /// By the labels, written as in the exposition: `route="mcp",code="200"`.
    samples: BTreeMap<String, Value>,
}
static REGISTRY: Mutex<BTreeMap<&'static str, Family>> = Mutex::new(BTreeMap::new());

fn update(metric: &'static Metric, labels: &[(&str, &str)], change: impl FnOnce(&mut Value)) {
    let labels = labels
        .iter()
        .map(|(name, value)| format!("{name}=\"{value}\""))
        .collect::<Vec<_>>()
        .join(",");
    let mut registry = crate::sync::lock(&REGISTRY);
    let family = registry.entry(metric.name).or_insert_with(|| Family {
        metric,
        samples: BTreeMap::new(),
    });
    change(
        family
            .samples
            .entry(labels)
            .or_insert_with(|| match metric.kind {
                Kind::Histogram => Value::Histogram {
                    counts: [0; BOUNDS.len() + 1],
                    sum: 0.0,
                },
                Kind::Counter | Kind::Gauge => Value::Number(0),
            }),
    );
}

/// Adds to a counter, or to a gauge when `by` is negative.
pub fn add(metric: &'static Metric, labels: &[(&str, &str)], by: i64) {
    update(metric, labels, |value| {
        if let Value::Number(number) = value {
            *number = number.saturating_add(by);
        }
    });
}
pub fn set(metric: &'static Metric, labels: &[(&str, &str)], to: i64) {
    update(metric, labels, |value| {
        if let Value::Number(number) = value {
            *number = to;
        }
    });
}
pub fn observe(metric: &'static Metric, labels: &[(&str, &str)], elapsed: Duration) {
    let seconds = elapsed.as_secs_f64();
    update(metric, labels, |value| {
        if let Value::Histogram { counts, sum } = value {
            counts[BOUNDS.partition_point(|bound| *bound < seconds)] += 1;
            *sum += seconds;
        }
    });
}

fn render() -> String {
    let mut text = String::new();
    for family in crate::sync::lock(&REGISTRY).values() {
        let kind = match family.metric.kind {
            Kind::Counter => "counter",
            Kind::Gauge => "gauge",
            Kind::Histogram => "histogram",
        };
        let name = family.metric.name;
        let _ = writeln!(
            text,
            "# HELP {name} {}\n# TYPE {name} {kind}",
            family.metric.help
        );
        for (labels, value) in &family.samples {
            let braces = |extra: &str| match (labels.is_empty(), extra.is_empty()) {
                (true, true) => String::new(),
                (true, false) => format!("{{{extra}}}"),
                (false, true) => format!("{{{labels}}}"),
                (false, false) => format!("{{{labels},{extra}}}"),
            };
            match value {
                Value::Number(number) => {
                    let _ = writeln!(text, "{name}{} {number}", braces(""));
                }
                Value::Histogram { counts, sum } => {
                    let mut total = 0;
                    for (index, count) in counts.iter().enumerate() {
                        total += count;
                        let bound = BOUNDS.get(index).map_or("+Inf".into(), f64::to_string);
                        let _ = writeln!(
                            text,
                            "{name}_bucket{} {total}",
                            braces(&format!("le=\"{bound}\""))
                        );
                    }
                    let _ = writeln!(text, "{name}_sum{} {sum}", braces(""));
                    let _ = writeln!(text, "{name}_count{} {total}", braces(""));
                }
            }
        }
    }
    text
}

/// Binds `address` and serves `GET /metrics` on it until the task is aborted.
pub async fn listen(address: &str) -> std::io::Result<tokio::task::JoinHandle<()>> {
    let listener = tokio::net::TcpListener::bind(address).await?;
    let router = Router::new().route(
        "/metrics",
        get(|| async {
            (
                [(
                    header::CONTENT_TYPE,
                    "text/plain; version=0.0.4; charset=utf-8",
                )],
                render(),
            )
        }),
    );
    // The connection limit and header bounds of the main listener.
    Ok(tokio::spawn(async move {
        let sockets =
            std::sync::Arc::new(tokio::sync::Semaphore::new(crate::http::MAX_CONNECTIONS));
        let drain = tokio_util::sync::CancellationToken::new();
        let mut connections = tokio::task::JoinSet::new();
        loop {
            let accepted = tokio::select! {
                Some(_) = connections.join_next(), if !connections.is_empty() => continue,
                accepted = listener.accept() => accepted,
            };
            let Ok((socket, _)) = accepted else {
                break;
            };
            let Ok(permit) = sockets.clone().try_acquire_owned() else {
                continue;
            };
            connections.spawn(crate::http::serve_connection(
                socket,
                router.clone(),
                drain.clone(),
                permit,
            ));
        }
    }))
}

pub(crate) static HTTP_REQUESTS: Metric = Metric::histogram(
    "fyrr_reader_http_request_duration_seconds",
    "HTTP requests by route and status code, with their latency.",
);
pub(crate) static QUEUE_WAIT: Metric = Metric::histogram(
    "fyrr_reader_queue_wait_seconds",
    "Time from the arrival of a request to the start of its execution.",
);
pub(crate) static MIRROR_HITS: Metric = Metric::counter(
    "fyrr_reader_mirror_hits_total",
    "Reads answered from the local mirror.",
);
pub(crate) static OBJECT_STORE_REQUESTS: Metric = Metric::counter(
    "fyrr_reader_object_store_requests_total",
    "Reads the mirror sent to the object store.",
);
pub(crate) static OBJECT_STORE_BYTES: Metric = Metric::counter(
    "fyrr_reader_object_store_bytes_total",
    "Bytes the mirror read from the object store.",
);
pub(crate) static MIRROR_BYTES: Metric = Metric::gauge(
    "fyrr_reader_mirror_bytes",
    "Bytes of the local mirrors, as last measured.",
);
pub(crate) static MIRROR_EVICTIONS: Metric = Metric::counter(
    "fyrr_reader_mirror_evictions_total",
    "Mirror files removed to make room.",
);
pub(crate) static OPEN_NAMESPACES: Metric = Metric::gauge(
    "fyrr_reader_open_namespaces",
    "Namespaces the reader holds open.",
);
pub(crate) static RECALL_SAMPLES: Metric = Metric::counter(
    "fyrr_reader_recall_samples_total",
    "Background recall samples by outcome: completed or the reason it was skipped.",
);

#[cfg(test)]
mod tests {
    use super::*;

    static LATENCY: Metric = Metric::histogram("test_latency_seconds", "A test.");

    #[test]
    fn histogram_buckets_are_cumulative_and_end_at_infinity() {
        observe(&LATENCY, &[("route", "a")], Duration::from_millis(5));
        observe(&LATENCY, &[("route", "a")], Duration::from_secs(500));
        let text = render();
        for line in [
            "test_latency_seconds_bucket{route=\"a\",le=\"0.005\"} 1\n",
            "test_latency_seconds_bucket{route=\"a\",le=\"120\"} 1\n",
            "test_latency_seconds_bucket{route=\"a\",le=\"+Inf\"} 2\n",
            "test_latency_seconds_count{route=\"a\"} 2\n",
        ] {
            assert!(text.contains(line), "{line}\n{text}");
        }
    }
}
