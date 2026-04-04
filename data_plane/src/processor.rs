use std::sync::Arc;

use tokio::sync::{mpsc, Mutex};
use tracing::{debug, trace};

use crate::ProcessingJob;
use crate::SinkWriteJob;

pub struct Processor {
    proc_rx: Arc<Mutex<mpsc::Receiver<ProcessingJob>>>,
    sink_tx: mpsc::Sender<SinkWriteJob>,
}

impl Processor {
    pub fn new(proc_rx: mpsc::Receiver<ProcessingJob>, sink_tx: mpsc::Sender<SinkWriteJob>) -> Self {
        Self {
            proc_rx: Arc::new(Mutex::new(proc_rx)),
            sink_tx
        }
    }

    pub fn start(&self) {
        let proc_rx = Arc::clone(&self.proc_rx);
        let sink_tx = self.sink_tx.clone();

        tokio::spawn(async move {
            let mut proc_rx = proc_rx.lock().await;
            
            while let Some(job) = proc_rx.recv().await {
                trace!("Received ProcessingJob: source={}, bytes={}", job.source, job.data.len());

                let sink_tx = sink_tx.clone();

                // HAND OFF TO RAYON (CPU bound)
                // We use a oneshot-style handoff or spawn into the global pool
                rayon::spawn(move || {
                    debug!("Processing data from: source={}, bytes={}", job.source, job.n);

                    // let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                    // encoder.write_all(&data_chunk).unwrap();
                    // let compressed = encoder.finish().unwrap();
                
                    let processed = job.data;
                    let source = job.source;
                    let data_len = processed.len();

                    let job = SinkWriteJob {
                        data: processed,
                        n: data_len,
                        source: job.source
                    };
                    let _ = sink_tx.blocking_send(job);
                    trace!("Submitted SinkWriteJob: source={}, bytes={}", source, data_len);
                });
            }
        });
    }
}