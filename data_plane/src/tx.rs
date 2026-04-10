use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::sync::broadcast;
use std::sync::Mutex;
use tracing::{trace, debug};
use dashmap::DashMap;

use crate::SinkTxJob;
use crate::StreamDescription;
use crate::ConnectionManager;
use crate::DataPlaneEvent;
use crate::Port;

#[derive(Clone)]
struct Route {
    id: String,
    sink_port: Port,
    socket: Arc<tokio::net::UdpSocket>,
}

pub struct DataTx {
    connection_manager: Arc<ConnectionManager>,
    fanout_table: Arc<DashMap<Port, Vec<Route>>>,
    sink_rx: Mutex<Option<mpsc::Receiver<SinkTxJob>>>,
    event_tx: broadcast::Sender<DataPlaneEvent>,
}

impl DataTx {
    pub fn new(connection_manager: Arc<ConnectionManager>, sink_rx: mpsc::Receiver<SinkTxJob>, event_tx: broadcast::Sender<DataPlaneEvent>) -> Self {
        Self {
            connection_manager,
            sink_rx: Mutex::new(Some(sink_rx)),
            event_tx,
            fanout_table: Arc::new(DashMap::new()),
        }
    }

    pub fn start(&self) { 
        let mut sink_rx = self.sink_rx.lock().unwrap().take().expect("DataTx started more than once");
        let fanout_table = Arc::clone(&self.fanout_table);
        
        // spawn a Tokio task to send data to the sink of the stream (also I/O bound)
        tokio::spawn(async move {
            while let Some(job) = sink_rx.recv().await {
                trace!("Received SinkTxJob, source={}, bytes={}", job.source, job.data.len());

                Self::write_egress_udp_socket(&fanout_table, job);
            }
        });
    }

    pub fn add_sink(&self, stream: &StreamDescription) {
        if let Some(socket) = self.connection_manager.get_connection(&stream.sink) {
            let mut entry = self.fanout_table.entry(stream.source).or_default();
            if !entry.iter().any(|r| r.id == stream.id) {
                entry.push(Route { 
                    id: stream.id.clone(),
                    sink_port: stream.sink,
                    socket 
                });
                debug!("Added route to DataTx fast-path {} -> {}", stream.source, stream.sink);
            }
        }
    }

    pub fn remove_sink(&self, stream: &StreamDescription) {
        let mut is_empty = false;
        if let Some(mut routes) = self.fanout_table.get_mut(&stream.source) {
            routes.retain(|r| r.id != stream.id);
            is_empty = routes.is_empty();
        }

        if is_empty {
            self.fanout_table.remove(&stream.source);
        }
        
        debug!("Removed route from DataTx fast-path {} -> {}", stream.source, stream.sink);
    }

    fn write_egress_udp_socket(routes: &DashMap<Port, Vec<Route>>, job: SinkTxJob) {
        if let Some(destinations) = routes.get(&job.source) {
            for route in destinations.value() {
                let sink_port = route.sink_port;
                let sink_socket = Arc::clone(&route.socket);
                let data = job.data.clone(); // `Bytes::clone` is an extremely cheap pointer copy
            
                tokio::spawn(async move {
                    let addr = format!("0.0.0.0:{}", sink_port);
                    let _ = sink_socket.send_to(&data, addr).await;

                    trace!("Data flushed on sink {}", sink_port);
                });
            }
        }
    }
}