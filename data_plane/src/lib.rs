mod processor;

use std::io;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use tracing::{info, debug, trace, error};
use dashmap::DashMap;
use tokio::{net::UdpSocket, sync::{OnceCell, mpsc, Mutex}, time::{timeout, Duration}};

use rtp::packet::Packet;
use webrtc_util::marshal::Unmarshal;

use processor::Processor;

type Port = u16;

pub struct DataPlane {
    stream_registry: Arc<StreamRegistry>,
    connection_manager: Arc<ConnectionManager>,
    processor: Processor,
    data_rx: DataRx,
    data_tx: DataTx,
}

struct StreamRegistry {
    id_counter: AtomicU32,
    streams: DashMap<Port, Vec<StreamDescription>>,
}

struct ConnectionManager {
    listeners: Mutex<HashMap<Port, Arc<OnceCell<()>>>>,
    sockets: Arc<DashMap<Port, Arc<Mutex<UdpSocket>>>>,
    con_tx: mpsc::Sender<SourceRxJob>,
}

struct DataRx {
    con_rx: Arc<Mutex<mpsc::Receiver<SourceRxJob>>>,
    proc_tx: mpsc::Sender<ProcessingJob>,
    rx_timeout: Duration,
}

struct DataTx {
    stream_registry: Arc<StreamRegistry>,
    connection_manager: Arc<ConnectionManager>,
    sink_rx: Arc<Mutex<mpsc::Receiver<SinkTxJob>>>,
}

#[derive(Clone, Debug)]
pub struct ProcessingJob {
    data: bytes::Bytes,
    sequence_number: u16,
    n: usize,
    source: Port,
}

#[derive(Clone, Debug)]
pub struct SinkTxJob {
    data: bytes::Bytes,
    source: Port,
}

#[derive(Clone, Debug)]
struct SourceRxJob {
    source: Port,
    socket: Arc<Mutex<UdpSocket>>,
}

impl DataPlane {
    pub fn new() -> Self {
        let (proc_tx, proc_rx) = mpsc::channel(1024);
        let (sink_tx, sink_rx) = mpsc::channel(1024);
        let (con_tx, con_rx) = mpsc::channel(1024);

        let stream_registry = Arc::new(StreamRegistry::new());
        let connection_manager = Arc::new(ConnectionManager::new(con_tx));
        let processor: Processor = Processor::new(proc_rx, sink_tx);
        let data_rx = DataRx::new(con_rx, proc_tx);
        let data_tx = DataTx::new(Arc::clone(&stream_registry), Arc::clone(&connection_manager), sink_rx);

        Self { 
            stream_registry,
            connection_manager,
            processor,
            data_rx,
            data_tx,
        }
    }
    
    pub fn start(&self) -> Result<(), Box<dyn std::error::Error>> { 
        info!("Starting data plane.");
  
        self.data_rx.start();
        self.data_tx.start();
        self.processor.start();

        Ok(())
    }

    pub fn list_provisioned_streams(&self) -> Vec<StreamDescription> {
        self.stream_registry.list_streams()
    }

    pub async fn provision_stream(&self, source: Port, sink: Port) -> io::Result<StreamDescription> {
        info!("Provisioning stream {} -> {}", source, sink);
        
        if let Some(stream) = self.stream_registry.find_stream(source, sink) {
            info!("Stream with id {} already provisioned {} -> {}", stream.id, source, sink);
            return Ok(stream);
        }

        let _ = self.connection_manager.connect_source(source).await?;
        let _ = self.connection_manager.connect_sink(sink).await?;

        let stream = self.stream_registry.add_stream(source, sink);
        info!("New stream with Id {} provisioned {} -> {}", stream.id, source, sink);
        Ok(stream)
    }
}

#[derive(Clone)]
pub struct StreamDescription {
    pub id: String,
    pub source: Port,
    pub sink: Port,
}

impl StreamRegistry {
    fn new() -> Self {
        Self { 
            id_counter: AtomicU32::new(1),
            streams: DashMap::new(),
        }
    }

    fn add_stream(&self, source: Port, sink: Port) -> StreamDescription {
        let stream = StreamDescription {
            id: self.id_counter.fetch_add(1, Ordering::SeqCst).to_string(),
            source,
            sink,
        };

        self.streams.entry(source).or_default().push(stream.clone());

        stream
    }

    fn list_streams(&self) -> Vec<StreamDescription> {
        let streams: Vec<StreamDescription> = self.streams
            .iter()
            .map(|entry| entry.value().clone())
            .flatten()
            .collect();

        streams
    }

    fn find_stream(&self, source: Port, sink: Port) -> Option<StreamDescription> {
        self.streams.get(&source)
            .and_then(|r| r.value().iter().find(|s| s.sink == sink).cloned())
    }

    fn for_each_stream_by_source<F>(&self, source: Port, mut f: F)
    where
        F: FnMut(&StreamDescription),
    {
        if let Some(r) = self.streams.get(&source) {
            for stream in r.value() {
                f(stream);
            }
        }
    }
}

impl ConnectionManager {
    fn new(con_tx: mpsc::Sender<SourceRxJob>) -> Self {    
        Self { 
            listeners: Mutex::new(HashMap::new()),
            con_tx,
            sockets: Arc::new(DashMap::new()),
        }
    }

    fn get_connection(&self, port: &Port) -> Option<Arc<Mutex<UdpSocket>>> {
        self.sockets.get(port).map(|r| Arc::clone(r.value()))
    }

    async fn bind<F, Fut>(&self, port: Port, handler: F) -> io::Result<()> 
    where
        F: Fn(Port, UdpSocket) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let cell = {
            let mut listeners = self.listeners.lock().await;
            let cell = listeners.entry(port).or_insert_with(|| Arc::new(OnceCell::new()));
            Arc::clone(cell)
        };

        cell.get_or_try_init(|| async move {
            debug!("Binding to port {}", port);

            let socket = tokio::net::UdpSocket::bind(format!("0.0.0.0:{}", port)).await?;

            tokio::spawn(async move {
                handler(port, socket).await;
            });

            Ok::<(), io::Error>(())
        }).await?;

        Ok(())
    }

    async fn connect_source(&self, source: Port) -> io::Result<()> {
        let con_tx = self.con_tx.clone(); // Sender clones cleanly natively
        let sockets = Arc::clone(&self.sockets);

        self.bind(source, move |port, src_socket| {
            let con_tx = con_tx.clone();
            let sockets = Arc::clone(&sockets);

            async move {
                let socket_arc = Arc::new(Mutex::new(src_socket));
                sockets.insert(port, Arc::clone(&socket_arc));

                let job = SourceRxJob {
                    source: port,
                    socket: socket_arc,
                };
                trace!("Submitting {:?}", job);
                let _ = con_tx.send(job).await;
            }
        }).await
    }

    async fn connect_sink(&self, sink: Port) -> io::Result<()> {
        if !self.sockets.contains_key(&sink) {
            let sink_socket = tokio::net::UdpSocket::bind("0.0.0.0:0").await?;
            self.sockets.entry(sink).or_insert_with(|| {
                info!("Egress socket for sink port {} opened", sink);
                Arc::new(Mutex::new(sink_socket))
            });
        }
        Ok(())
    }
}

impl DataRx {
    pub fn new(con_rx: mpsc::Receiver<SourceRxJob>, proc_tx: mpsc::Sender<ProcessingJob>) -> Self {
        Self { 
            con_rx: Arc::new(Mutex::new(con_rx)),            
            proc_tx,
            rx_timeout: Duration::from_secs(10)
        }
    }

    fn start(&self) { 
        let con_rx = Arc::clone(&self.con_rx);
        let proc_tx = self.proc_tx.clone();
        let rx_timeout = self.rx_timeout;

        // spawn a Tokio task to handle incoming connection jobs (I/O bound)
        tokio::spawn(async move {
            let mut con_rx = con_rx.lock().await;

            while let Some(job) = con_rx.recv().await {
                trace!("Received {:?}", job);

                let proc_tx = proc_tx.clone();

                tokio::spawn(Self::process_udp_socket(job, proc_tx, rx_timeout));
            }
        });
    }

    async fn process_udp_socket(job: SourceRxJob, proc_tx: mpsc::Sender<ProcessingJob>, rx_timeout: Duration) {
        let mut buf = bytes::BytesMut::with_capacity(512);

        let socket = job.socket.lock().await;
        loop {
            buf.reserve(512); // Ensure enough contiguous capacity for the next UDP packet
            match timeout(rx_timeout, socket.recv_buf_from(&mut buf)).await {
                Ok(Ok((n, addr))) if n > 0 => {
                    trace!("Received data on source {} from {:?}, bytes={}", job.source, addr, n);
                    
                    let data = buf.split().freeze();
                    let mut data_reader = data.clone();
                    
                    match Packet::unmarshal(&mut data_reader) {
                        Ok(rtp_packet) => {
                            let seq = rtp_packet.header.sequence_number;
                            debug!("Received RTP packet on source {}, seq={}, bytes={}", job.source, seq, n);
                            
                            let job = ProcessingJob {
                                data, // Forward the entire raw packet so sinks get a valid RTP stream
                                sequence_number: seq,
                                n,
                                source: job.source
                            };
                            trace!("Submitting ProcessingJob source {}, seq={}, bytes={}", job.source, seq, n);
                            proc_tx.send(job).await.unwrap();
                        }
                        Err(err) => {
                            error!("Failed to parse RTP packet: {}", err);
                        }
                    }
                }
                Err(_) => {
                    info!("Stream timed out on source {}", job.source);
                    break;
                }
                _ => break,
            }
        }
    }
}

impl DataTx {
    fn new(stream_registry: Arc<StreamRegistry>, connection_manager: Arc<ConnectionManager>, sink_rx: mpsc::Receiver<SinkTxJob>,) -> Self {
        Self {
            stream_registry,
            connection_manager,
            sink_rx: Arc::new(Mutex::new(sink_rx)),
        }
    }

    fn start(&self) { 
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