use std::time::Instant;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use dashmap::DashMap;
use rtp::packet::Packet;
use webrtc_util::marshal::Unmarshal;
use tokio::sync::mpsc;
use tokio::sync::broadcast;
use tokio::time::Duration;
use std::sync::Mutex;
use tracing::{debug, trace, warn, error};

use crate::ProcessingJob;
use crate::SourceRxTask;
use crate::DataPlaneEvent;
use crate::Port;

pub struct DataRx {
    con_rx: Mutex<Option<mpsc::Receiver<SourceRxTask>>>,
    proc_tx: mpsc::Sender<ProcessingJob>,
    traced_ports: Arc<DashMap<Port, Arc<RxTracing>>>,
    event_tx: broadcast::Sender<DataPlaneEvent>,
}

struct RxMetrics {
    dropped_zero_byte: metrics::Counter,
    dropped_io_error: metrics::Counter,
    packets_received: metrics::Counter,
    bytes_received: metrics::Counter,
}

struct RxTracing {
    enabled: AtomicBool,
    subscribers: AtomicU32,
    rx_last_recv_time: Mutex<Option<Instant>>,
    rx_sum_bytes: AtomicU64,
    rx_sum_packets: AtomicU32,
}

impl DataRx {
    pub fn new(con_rx: mpsc::Receiver<SourceRxTask>, proc_tx: mpsc::Sender<ProcessingJob>, event_tx: broadcast::Sender<DataPlaneEvent>) -> Self {
        Self { 
            con_rx: Mutex::new(Some(con_rx)),            
            proc_tx,
            traced_ports: Arc::new(DashMap::new()),
            event_tx
        }
    }

    pub fn start(&self) {
        let mut con_rx = self.con_rx.lock().unwrap().take().expect("DataRx started more than once");
        let proc_tx = self.proc_tx.clone();
        let traced_ports = Arc::clone(&self.traced_ports);
        let event_tx = self.event_tx.clone();
        let traced_ports_tick = Arc::clone(&self.traced_ports);

        // spawn a Tokio task to handle incoming connection jobs (I/O bound)
        tokio::spawn(async move {
            while let Some(job) = con_rx.recv().await {
                trace!("Received SourceRxTask, source: {}", job.source);

                let proc_tx = proc_tx.clone();
                let traced_ports = Arc::clone(&traced_ports);
                let tracing = Arc::new(RxTracing::new());
                traced_ports.insert(job.source, Arc::clone(&tracing));

                tokio::spawn(Self::process_udp_socket(job, proc_tx, tracing, traced_ports));
            }
        });

        // tick every second to emit events
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                let now = Instant::now();

                for entry in traced_ports_tick.iter() {
                    let port = *entry.key();
                    let trace_data = entry.value();

                    if trace_data.enabled.load(Ordering::Relaxed) 
                        && trace_data.rx_last_recv_time.lock().unwrap().map_or(false, |last| now.duration_since(last) < Duration::from_secs(1)) {                                                
                        let _ = event_tx.send(DataPlaneEvent::RxPortTraceUpdate {
                            port,
                            sum_bytes_recv: trace_data.rx_sum_bytes.load(Ordering::Relaxed),
                            sum_packets_recv: trace_data.rx_sum_packets.load(Ordering::Relaxed),
                        });

                        trace_data.rx_sum_bytes.swap(0, Ordering::Relaxed);
                        trace_data.rx_sum_packets.swap(0, Ordering::Relaxed);
                    }
                }
            }
        });
    }

    async fn process_udp_socket(
        mut rx_task: SourceRxTask, 
        proc_tx: mpsc::Sender<ProcessingJob>,
        tracing: Arc<RxTracing>,
        traced_ports: Arc<DashMap<Port, Arc<RxTracing>>>
    ) {    
        let metrics = RxMetrics::new(rx_task.source);

        let mut buf = bytes::BytesMut::with_capacity(512);
        let socket = rx_task.socket;
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
                            tracing.trace_rx_activity(Instant::now(), n);

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
        
        // Clean up the tracing state when the socket task ends
        traced_ports.remove(&rx_task.source);
    }

    pub fn enable_tracing(&self, port: Port) {
        debug!("Enabling tracing for port {}", port);

        if let Some(tracing) = self.traced_ports.get(&port) {
            let prev = tracing.subscribers.fetch_add(1, Ordering::SeqCst);
            if prev == 0 {
                tracing.enabled.store(true, Ordering::SeqCst);
            }
        }
    }

    pub fn disable_tracing(&self, port: Port) {
        debug!("Disabling tracing for port {}", port);

        if let Some(tracing) = self.traced_ports.get(&port) {
            let prev = tracing.subscribers.fetch_sub(1, Ordering::SeqCst);
            if prev == 1 {
                tracing.enabled.store(false, Ordering::SeqCst);
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

impl RxTracing {
    fn new() -> Self {
        Self {
            enabled: AtomicBool::new(false),
            subscribers: AtomicU32::new(0),
            rx_last_recv_time: Mutex::new(None),
            rx_sum_bytes: AtomicU64::new(0),
            rx_sum_packets: AtomicU32::new(0),
        }
    }

    #[inline]
    fn trace_rx_activity(&self, recv_time: Instant, bytes: usize) {
        if self.enabled.load(Ordering::Relaxed) {
            if let Ok(mut last_time) = self.rx_last_recv_time.lock() {
                *last_time = Some(recv_time);
            }
            self.rx_sum_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
            self.rx_sum_packets.fetch_add(1, Ordering::Relaxed);
        }
    }
}