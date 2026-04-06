use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::broadcast;
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, trace};

use crate::ProcessingJob;
use crate::SinkTxJob;
use crate::DataPlaneEvent;

struct ProcessorMetrics {
    cache: HashMap<u16, (metrics::Counter, metrics::Histogram, metrics::Counter)>,
    last_sequence_numbers: HashMap<u16, u16>,
}

pub struct Processor {
    proc_rx: Arc<Mutex<mpsc::Receiver<ProcessingJob>>>,
    sink_tx: mpsc::Sender<SinkTxJob>,
    event_tx: broadcast::Sender<DataPlaneEvent>
}

impl Processor {
    pub fn new(proc_rx: mpsc::Receiver<ProcessingJob>, sink_tx: mpsc::Sender<SinkTxJob>, event_tx: broadcast::Sender<DataPlaneEvent>) -> Self {
        Self {
            proc_rx: Arc::new(Mutex::new(proc_rx)),
            sink_tx,
            event_tx
        }
    }

    pub fn start(&self) {
        let proc_rx = Arc::clone(&self.proc_rx);
        let sink_tx = self.sink_tx.clone();

        tokio::spawn(async move {
            let mut proc_rx = proc_rx.lock().await;
            let mut metrics = ProcessorMetrics::new();
            
            while let Some(job) = proc_rx.recv().await {
                trace!("Received ProcessingJob: source={}, bytes={}", job.source, job.data.len());

                let sink_tx = sink_tx.clone();

                let (counter, histogram) = metrics.record_packet(job.source, job.sequence_number);

                tokio::spawn(async move {
                    let start_time = std::time::Instant::now();
                    // OFF-LOAD CPU BOUND WORK
                    let (job, data_len) = tokio::task::spawn_blocking(move || {
                        debug!("Processing data from: source={}, bytes={}", job.source, job.n);

                        // let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                        // encoder.write_all(&data_chunk).unwrap();
                        // let compressed = encoder.finish().unwrap();
                    
                        let processed = job.data;
                        let source = job.source;
                        let data_len = processed.len();

                        let job = SinkTxJob {
                            data: processed,
                            source
                        };
                        (job, data_len)
                    }).await.unwrap();

                    counter.increment(data_len as u64);
                    histogram.record(start_time.elapsed().as_secs_f64());

                    trace!("Submitting SinkTxJob: source={}, bytes={}", job.source, data_len);
                    sink_tx.send(job).await.unwrap();
                });
            }
        });
    }
}

impl ProcessorMetrics {
    fn new() -> Self {
        Self { 
            cache: HashMap::new(),
            last_sequence_numbers: HashMap::new(),
        }
    }

    fn record_packet(&mut self, source: u16, sequence_number: u16) -> (metrics::Counter, metrics::Histogram) {
        let (counter, histogram, drop_counter) = self.get_or_register(source);

        if let Some(last_seq) = self.last_sequence_numbers.get(&source) {
            let diff = sequence_number.wrapping_sub(*last_seq);
            // A gap > 1 but < 32768 implies forward packet loss (accounting for 16-bit wrap-around)
            if diff > 1 && diff < 32768 {
                let drops = (diff - 1) as u64;
                drop_counter.increment(drops);
                tracing::warn!("Packet loss detected on source {}: {} packets dropped", source, drops);
            }
        }
        self.last_sequence_numbers.insert(source, sequence_number);

        (counter, histogram)
    }

    fn get_or_register(&mut self, source: u16) -> (metrics::Counter, metrics::Histogram, metrics::Counter) {
        self.cache.entry(source).or_insert_with(|| {
            let port_str = source.to_string();
            (
                metrics::counter!("streamer_processed_bytes_total", "source_port" => port_str.clone()),
                metrics::histogram!("streamer_processing_latency_seconds", "source_port" => port_str.clone()),
                metrics::counter!("streamer_dropped_packets_total", "source_port" => port_str)
            )
        }).clone()
    }
}