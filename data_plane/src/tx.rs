use std::sync::Arc;

use tokio::sync::{mpsc, Mutex};
use tokio::sync::broadcast;
use tracing::trace;

use crate::SinkTxJob;
use crate::StreamRegistry;
use crate::ConnectionManager;
use crate::DataPlaneEvent;

pub struct DataTx {
    stream_registry: Arc<StreamRegistry>,
    connection_manager: Arc<ConnectionManager>,
    sink_rx: Arc<Mutex<mpsc::Receiver<SinkTxJob>>>,
    event_tx: broadcast::Sender<DataPlaneEvent>,
}

impl DataTx {
    pub fn new(stream_registry: Arc<StreamRegistry>, connection_manager: Arc<ConnectionManager>, sink_rx: mpsc::Receiver<SinkTxJob>, event_tx: broadcast::Sender<DataPlaneEvent>) -> Self {
        Self {
            stream_registry,
            connection_manager,
            sink_rx: Arc::new(Mutex::new(sink_rx)),
            event_tx,
        }
    }

    pub fn start(&self) { 
        let sink_rx = Arc::clone(&self.sink_rx);
        let stream_registry = Arc::clone(&self.stream_registry);
        let connection_manager = Arc::clone(&self.connection_manager);
        
        // spawn a Tokio task to send data to the sink of the stream (also I/O bound)
        tokio::spawn(async move {
            let mut sink_rx = sink_rx.lock().await;

            while let Some(job) = sink_rx.recv().await {
                trace!("Received SinkTxJob, source={}, bytes={}", job.source, job.data.len());

                Self::write_egress_udp_socket(&stream_registry, &connection_manager, job);
            }
        });
    }

    fn write_egress_udp_socket(stream_registry: &StreamRegistry, connection_manager: &ConnectionManager, job: SinkTxJob) {
        stream_registry.for_each_stream_by_source(job.source, |stream| {
            let sink_port = stream.sink;
            let sink_socket = connection_manager.get_connection(&sink_port).unwrap();
            let data = job.data.clone(); // `Bytes::clone` is an extremely cheap pointer copy
        
            tokio::spawn(async move {
                let sink_socket = sink_socket.lock().await;
                let addr = format!("0.0.0.0:{}", sink_port);
                let _ = sink_socket.send_to(&data, addr).await;

                trace!("Data flushed on sink {}", sink_port);
            });
        });
    }
}