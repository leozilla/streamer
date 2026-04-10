use std::time::Instant;
use std::sync::Arc;

use dashmap::DashMap;
use rtp::packet::Packet;
use webrtc_util::marshal::Unmarshal;
use tokio::sync::mpsc;
use tokio::sync::broadcast;
use std::sync::Mutex;
use tracing::{debug, trace, warn, error};

use crate::ProcessingJob;
use crate::SourceRxTask;
use crate::DataPlaneEvent;
use crate::Port;

pub struct DataRx {
    con_rx: Mutex<Option<mpsc::Receiver<SourceRxTask>>>,
    proc_tx: mpsc::Sender<ProcessingJob>,
    traced_ports: Arc<DashMap<Port, RxTracing>>,
    ctrl_tx: broadcast::Sender<RxControlMessage>,
    event_tx: broadcast::Sender<DataPlaneEvent>,
}

struct RxMetrics {
    dropped_zero_byte: metrics::Counter,
    dropped_io_error: metrics::Counter,
    packets_received: metrics::Counter,
    bytes_received: metrics::Counter,
}

struct EnabledRxTracing {
    subscribers: u32,
    rx_last_recv_time: Option<Instant>,
    rx_sum_bytes: u64,
    rx_sum_packets: u32,
}

enum RxTracing {
    Enabled(EnabledRxTracing),
    Disabled,
}

#[derive(Clone, Debug)]
enum RxControlMessage {
    EnableTracing(Port),
    DisableTracing(Port),
}

impl DataRx {
    pub fn new(con_rx: mpsc::Receiver<SourceRxTask>, proc_tx: mpsc::Sender<ProcessingJob>, event_tx: broadcast::Sender<DataPlaneEvent>) -> Self {
        let (ctrl_tx, _) = broadcast::channel(100);
        Self { 
            con_rx: Mutex::new(Some(con_rx)),            
            proc_tx,
            traced_ports: Arc::new(DashMap::new()),
            ctrl_tx,
            event_tx
        }
    }

    pub fn start(&self) {
        let mut con_rx = self.con_rx.lock().unwrap().take().expect("DataRx started more than once");
        let proc_tx = self.proc_tx.clone();
        let traced_ports = Arc::clone(&self.traced_ports);
        let ctrl_tx = self.ctrl_tx.clone();
        let event_tx = self.event_tx.clone();
        let traced_ports_tick = Arc::clone(&self.traced_ports);

        // spawn a Tokio task to handle incoming connection jobs (I/O bound)
        tokio::spawn(async move {
            while let Some(job) = con_rx.recv().await {
                trace!("Received SourceRxTask, source: {}", job.source);

                let proc_tx = proc_tx.clone();
                let traced_ports = Arc::clone(&traced_ports);
                let ctrl_rx = ctrl_tx.subscribe();

                tokio::spawn(Self::process_udp_socket(job, proc_tx, traced_ports, ctrl_rx));
            }
        });

        // tick every second to update tracing info and emit events
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
            loop {
                interval.tick().await;
                for mut entry in traced_ports_tick.iter_mut() {
                    let port = *entry.key();
                    if let RxTracing::Enabled(ref mut trace_data) = *entry.value_mut() {
                        let rx_active = trace_data.rx_sum_packets > 0;
                        
                        let _ = event_tx.send(DataPlaneEvent::PortTraceUpdated {
                            port,
                            rx_active,
                            tx_active: false,
                        });
                        
                        // Reset counters for the next tick
                        trace_data.rx_sum_packets = 0;
                        trace_data.rx_sum_bytes = 0;
                    }
                }
            }
        });
    }

    async fn process_udp_socket(
        mut rx_task: SourceRxTask, 
        proc_tx: mpsc::Sender<ProcessingJob>,
        traced_ports: Arc<DashMap<Port, RxTracing>>,
        mut ctrl_rx: broadcast::Receiver<RxControlMessage>
    ) {
        let mut buf = bytes::BytesMut::with_capacity(512);

        let metrics = RxMetrics::new(rx_task.source);
        let mut is_traced = traced_ports.contains_key(&rx_task.source);

        let socket = rx_task.socket;
        loop {
            buf.reserve(512); // Ensure enough contiguous capacity for the next UDP packet
            tokio::select! {
                _ = &mut rx_task.abort_rx => {
                    debug!("Source socket {} disconnected, aborting rx task", rx_task.source);
                    break;
                }
                Ok(msg) = ctrl_rx.recv() => {
                    match msg {
                        RxControlMessage::EnableTracing(port) if port == rx_task.source => {
                            is_traced = true;
                        }
                        RxControlMessage::DisableTracing(port) if port == rx_task.source => {
                            is_traced = false;
                        }
                        _ => {}
                    }
                }
                res = socket.recv_buf_from(&mut buf) => {
                    match res {
                        Ok((n, addr)) if n > 0 => {
                            trace!("Received data on source {} from {:?}, bytes={}", rx_task.source, addr, n);
                            
                            metrics.packets_received.increment(1);
                            metrics.bytes_received.increment(n as u64);
                            if is_traced {
                                if let Some(mut tracing) = traced_ports.get_mut(&rx_task.source) {
                                    tracing.trace_rx_activity(Instant::now(), n);
                                }
                            }

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

    pub fn enable_tracing(&self, port: Port) {
        debug!("Enabling tracing for port {}", port);

        self.traced_ports.entry(port).and_modify(|t| {
            if let RxTracing::Enabled(e) = t {
                e.subscribers += 1;
            }
        }).or_insert(RxTracing::Enabled(EnabledRxTracing {
            subscribers: 1,
            rx_last_recv_time: None,
            rx_sum_bytes: 0,
            rx_sum_packets: 0,
        }));

        let _ = self.ctrl_tx.send(RxControlMessage::EnableTracing(port));
    }

    pub fn disable_tracing(&self, port: Port) {
        debug!("Disabling tracing for port {}", port);

        let mut remove = false;
        self.traced_ports.remove_if_mut(&port, |_, t| {
            if let RxTracing::Enabled(e) = t {
                e.subscribers -= 1;
                if e.subscribers == 0 {
                    remove = true;
                    return true;
                }
            }
            false
        });

        if remove {
            let _ = self.ctrl_tx.send(RxControlMessage::DisableTracing(port));
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
    fn trace_rx_activity(&mut self, recv_time: Instant, bytes: usize) {
        match self {
            RxTracing::Enabled(tracer) => {
                tracer.rx_last_recv_time = Some(recv_time);
                tracer.rx_sum_bytes += bytes as u64;
                tracer.rx_sum_packets += 1;
            }
            RxTracing::Disabled => {}
        }
    }
}