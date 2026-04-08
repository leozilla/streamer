use std::sync::Arc;

use rtp::packet::Packet;
use webrtc_util::marshal::Unmarshal;
use tokio::sync::{mpsc, Mutex};
use tokio::sync::broadcast;
use tracing::{debug, trace, warn, error};

use crate::ProcessingJob;
use crate::SourceRxTask;
use crate::DataPlaneEvent;

struct RxMetrics {
    dropped_zero_byte: metrics::Counter,
    dropped_io_error: metrics::Counter,
    packets_received: metrics::Counter,
    bytes_received: metrics::Counter,
}

pub struct DataRx {
    con_rx: Arc<Mutex<mpsc::Receiver<SourceRxTask>>>,
    proc_tx: mpsc::Sender<ProcessingJob>,
    event_tx: broadcast::Sender<DataPlaneEvent>,
}

impl DataRx {
    pub fn new(con_rx: mpsc::Receiver<SourceRxTask>, proc_tx: mpsc::Sender<ProcessingJob>, event_tx: broadcast::Sender<DataPlaneEvent>) -> Self {
        Self { 
            con_rx: Arc::new(Mutex::new(con_rx)),            
            proc_tx,
            event_tx
        }
    }

    pub fn start(&self) {
        let con_rx = Arc::clone(&self.con_rx);
        let proc_tx = self.proc_tx.clone();

        // spawn a Tokio task to handle incoming connection jobs (I/O bound)
        tokio::spawn(async move {
            let mut con_rx = con_rx.lock().await;

            while let Some(job) = con_rx.recv().await {
                trace!("Received SourceRxTask, source: {}", job.source);

                let proc_tx = proc_tx.clone();

                tokio::spawn(Self::process_udp_socket(job, proc_tx));
            }
        });
    }

    async fn process_udp_socket(mut rx_task: SourceRxTask, proc_tx: mpsc::Sender<ProcessingJob>) {
        let mut buf = bytes::BytesMut::with_capacity(512);

        let metrics = RxMetrics::new(rx_task.source);
        let socket = rx_task.socket.lock().await;
        loop {
            buf.reserve(512); // Ensure enough contiguous capacity for the next UDP packet
            tokio::select! {
                _ = &mut rx_task.abort_rx => {
                    debug!("Source socket {} disconnected, aborting rx task", rx_task.source);
                    break;
                }
                res = socket.recv_buf_from(&mut buf) => {
                    match res {
                        Ok((n, addr)) if n > 0 => {
                            trace!("Received data on source {} from {:?}, bytes={}", rx_task.source, addr, n);
                            
                            metrics.packets_received.increment(1);
                            metrics.bytes_received.increment(n as u64);

                            let data = buf.split().freeze();
                            let mut data_reader = data.clone();
                            
                            match Packet::unmarshal(&mut data_reader) {
                                Ok(rtp_packet) => {
                                    let seq = rtp_packet.header.sequence_number;
                                    debug!("Received RTP packet on source {}, seq={}, bytes={}", rx_task.source, seq, n);
                                    
                                    let proc_job = ProcessingJob {
                                        data, // Forward the entire raw packet so sinks get a valid RTP stream
                                        sequence_number: seq,
                                        n,
                                        source: rx_task.source
                                    };
                                    trace!("Submitting ProcessingJob source {}, seq={}, bytes={}", rx_task.source, seq, n);
                                    proc_tx.send(proc_job).await.unwrap();
                                }
                                Err(err) => {
                                    error!("Failed to parse RTP packet: {}", err);
                                }
                            }
                        }
                        Ok((_, _)) => {
                            warn!("Received zero-byte packet on source {}", rx_task.source);
                            metrics.dropped_zero_byte.increment(1);
                            continue;
                        }
                        Err(err) => {
                            error!("Socket read error on source {}: {}", rx_task.source, err);
                            metrics.dropped_io_error.increment(1);
                            break;
                        }
                    }
                }    
            }
        }
    }
}

impl RxMetrics {
    fn new(source_port: crate::Port) -> Self {
        let port_str = source_port.to_string();
        Self {
            dropped_zero_byte: metrics::counter!(
                "streamer_rx_dropped_packets_total", 
                "reason" => "zero_byte",
                "source_port" => port_str.clone()
            ),
            dropped_io_error: metrics::counter!(
                "streamer_rx_dropped_packets_total", 
                "reason" => "io_error",
                "source_port" => port_str.clone()
            ),
            packets_received: metrics::counter!(
                "streamer_rx_packets_total",
                "source_port" => port_str.clone()
            ),
            bytes_received: metrics::counter!(
                "streamer_rx_bytes_total",
                "source_port" => port_str
            ),
        }
    }
}