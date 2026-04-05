use tracing::info;

pub struct MetricsExporter {
}

impl MetricsExporter {
    pub fn new() -> Self {
        Self { }
    }

    pub fn start(&self) {
        metrics_exporter_prometheus::PrometheusBuilder::new()
            .with_http_listener(([0, 0, 0, 0], 9000))
            .set_buckets_for_metric(
                metrics_exporter_prometheus::Matcher::Full("streamer_processing_latency_seconds".to_string()),
                &[0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1],
            ).unwrap()
            .install()
            .expect("Failed to install Prometheus recorder");
        info!("Prometheus metrics exported on http://0.0.0.0:9000/metrics");
    }
}