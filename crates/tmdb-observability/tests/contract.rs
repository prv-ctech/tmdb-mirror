use std::{
    io::{self, Write},
    sync::{Arc, Mutex},
    time::Duration,
};

use tmdb_observability::{
    CatalogScope, Component, ComponentState, HttpRequestLabels, HttpRequestLog, Listener,
    LogFormat, MethodClass, Metrics, QueueState, ReadinessFailureReason, StatusClass,
};
use tracing_subscriber::fmt::MakeWriter;

#[derive(Clone, Debug)]
struct SharedWriter(Arc<Mutex<Vec<u8>>>);

#[derive(Debug)]
struct WriterGuard(Arc<Mutex<Vec<u8>>>);

impl<'a> MakeWriter<'a> for SharedWriter {
    type Writer = WriterGuard;

    fn make_writer(&'a self) -> Self::Writer {
        WriterGuard(self.0.clone())
    }
}

impl Write for WriterGuard {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .map_err(|_| io::Error::other("writer mutex poisoned"))?
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn metrics_expose_build_identity_and_openmetrics_eof() -> Result<(), Box<dyn std::error::Error>> {
    let metrics = Metrics::new("tmdb-api", "0.1.0", "test-revision");
    let labels = HttpRequestLabels::new(
        Listener::Public,
        MethodClass::Get,
        "/health/live",
        StatusClass::Success,
    );
    metrics.observe_http(&labels, Duration::from_millis(12));
    metrics.inc_readiness_failure(ReadinessFailureReason::ProbeTimeout);

    let encoded = metrics.encode()?;
    assert!(encoded.contains("tmdb_build_info"));
    assert!(encoded.contains("tmdb_http_requests_total"));
    assert!(encoded.contains("tmdb_readiness_failures_total"));
    assert!(encoded.ends_with("# EOF\n"));
    assert!(encoded.contains("revision=\"test-revision\""));
    Ok(())
}

#[test]
fn method_and_status_labels_are_bounded() {
    assert_eq!(MethodClass::from_method("CONNECT"), MethodClass::Other);
    assert_eq!(StatusClass::from_status(599), StatusClass::ServerError);
    assert_eq!(StatusClass::from_status(700), StatusClass::Other);
}

#[test]
fn tracing_rejects_invalid_filter_without_panicking() {
    let result =
        tmdb_observability::init_tracing_with_filter("test", LogFormat::Pretty, Some("[not valid"));
    assert!(result.is_err());
}

#[test]
fn structured_request_logs_are_json_and_redact_raw_route_values()
-> Result<(), Box<dyn std::error::Error>> {
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_ansi(false)
        .with_target(false)
        .with_writer(SharedWriter(bytes.clone()))
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        tmdb_observability::log_http_request(&HttpRequestLog {
            service: "tmdb-api",
            version: "0.1.0",
            request_id: "01912345-6789-7abc-8def-0123456789ab",
            listener: Listener::Public,
            method: MethodClass::Get,
            route: "/health/ready?authorization=sentinel-route-secret",
            status: 503,
            duration: Duration::from_millis(4),
            outcome: "error",
        });
    });
    let output = String::from_utf8(bytes.lock().map_err(|_| "writer mutex poisoned")?.clone())?;
    let line = output.lines().next().ok_or("missing JSON log line")?;
    let parsed: serde_json::Value = serde_json::from_str(line)?;
    assert_eq!(parsed["fields"]["event"], "http_request_complete");
    assert_eq!(parsed["fields"]["status_class"], "5xx");
    assert!(!output.contains("sentinel-route-secret"));
    assert!(output.contains("<unmatched>"));
    Ok(())
}

#[test]
fn histogram_samples_have_exact_count_and_bounded_route() -> Result<(), Box<dyn std::error::Error>>
{
    let metrics = Metrics::new("tmdb-api", "0.1.0", "test-revision");
    metrics.observe_http(
        &HttpRequestLabels::new(
            Listener::Public,
            MethodClass::Get,
            "/health/live?query=secret",
            StatusClass::Success,
        ),
        Duration::from_millis(12),
    );
    let encoded = metrics.encode()?;
    assert!(encoded.contains("tmdb_http_request_duration_seconds_count"));
    assert!(encoded.contains("tmdb_http_request_duration_seconds_count{listener=\"public\",method=\"GET\",route=\"<unmatched>\",status_class=\"2xx\"} 1"));
    assert!(!encoded.contains("query=secret"));
    Ok(())
}

#[test]
fn operational_metrics_are_bounded_and_expose_worker_media_queue_and_backup_state()
-> Result<(), Box<dyn std::error::Error>> {
    let metrics = Metrics::new("tmdb-api", "0.1.0", "test-revision");
    metrics.set_component_state(Component::Worker, ComponentState::Ready);
    metrics.set_component_state(Component::Media, ComponentState::Stale);
    metrics.set_component_state(Component::Upstream, ComponentState::Degraded);
    metrics.set_component_state(Component::Backup, ComponentState::Ready);
    metrics.set_queue_depth("ingest.trending", QueueState::Queued, 7);
    metrics.set_queue_depth("image.download", QueueState::Running, 2);
    metrics.set_queue_depth("unbounded.untrusted.job", QueueState::DeadLetter, 1);
    metrics.set_catalog_count(CatalogScope::Movies, 10);
    metrics.set_backup_timestamps(Some(1_700_000_000), None);

    let encoded = metrics.encode()?;
    assert!(encoded.contains("tmdb_component_state"));
    assert!(encoded.contains("component=\"worker\",state=\"ready\"} 1"));
    assert!(encoded.contains("component=\"media\",state=\"stale\"} 1"));
    assert!(encoded.contains("tmdb_queue_depth"));
    assert!(encoded.contains("job_type=\"ingest.trending\",state=\"queued\"} 7"));
    assert!(encoded.contains("job_type=\"image.download\",state=\"running\"} 2"));
    assert!(encoded.contains("job_type=\"other\",state=\"dead_letter\"} 1"));
    assert!(encoded.contains("tmdb_catalog_titles"));
    assert!(encoded.contains("scope=\"movies\"} 10"));
    assert!(encoded.contains("tmdb_backup_last_success_timestamp_seconds 1700000000"));
    Ok(())
}
