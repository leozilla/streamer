mod processor;

use std::io;
use std::collections::HashMap;
use std::sync::Arc;

use tracing::{info, debug, trace, error};
use dashmap::DashMap;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}, sync::{OnceCell, mpsc, Mutex}};

use processor::Processor;

type Port = u16;

pub struct DataPlane {
    stream_registry: Arc<StreamRegistry>,
    connection_manager: Arc<ConnectionManager>,
    processor: Processor,
    data_rx: DataRx,
    data_tx: DataTx,
}

pub struct StreamRegistry {
    streams: Arc<DashMap<Port, StreamDescription>>,
    connected_ports: DashMap<Port, Vec<Port>>,
}

pub struct ConnectionManager {
    port_listeners: Arc<Mutex<HashMap<Port, Arc<OnceCell<io::Result<()>>>>>>,
    processing_job_subscribers: Vec<mpsc::Sender<ProcessingJob>>,
    connections: Arc<DashMap<Port, Arc<Mutex<TcpStream>>>>,
    rx_con_tx: Arc<mpsc::Sender<HandleConnectionJob>>,
}

struct DataRx {
    con_rx: Arc<Mutex<mpsc::Receiver<HandleConnectionJob>>>,
    proc_tx: Arc<mpsc::Sender<ProcessingJob>>,
}

struct DataTx {
    stream_registry: Arc<StreamRegistry>,
    connection_manager: Arc<ConnectionManager>,
    sink_rx: Arc<Mutex<mpsc::Receiver<SinkWriteJob>>>,
}

#[derive(Clone, Debug)]
pub struct ProcessingJob {
    data: Vec<u8>,
    n: usize,
    source: Port
}

#[derive(Clone, Debug)]
pub struct SinkWriteJob {
    data: Vec<u8>,
    n: usize,
    source: Port
}

#[derive(Debug)]
struct HandleConnectionJob {
    source: Port,
    socket: Arc<Mutex<TcpStream>>,
}

impl DataPlane {
    pub fn new() -> Self {
        let (proc_tx, proc_rx) = mpsc::channel(1024);
        let (sink_tx, sink_rx) = mpsc::channel(1024);
        let (rx_con_tx, rx_con_rx) = mpsc::channel(1024);

        let stream_registry = Arc::new(StreamRegistry::new());
        let connection_manager = Arc::new(ConnectionManager::new(rx_con_tx));
        let processor: Processor = Processor::new(proc_rx, sink_tx);
        let data_rx = DataRx::new(rx_con_rx, proc_tx);
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
    id: String,
    pub source: Port,
    pub sink: Port,
}

impl StreamRegistry {
    pub fn new() -> Self {
        Self { 
            streams: Arc::new(DashMap::new()),
            connected_ports: DashMap::new(),
        }
    }

    pub fn add_stream(&self, source: Port, sink: Port) -> StreamDescription {
        let stream = StreamDescription {
            id: "todo".to_string(),
            source,
            sink,
        };

        self.streams.insert(source, stream.clone());

        if self.connected_ports.contains_key(&source) {
            self.connected_ports.alter(&source, |_, mut v| {
                v.push(sink);
                v
            });
        } else {
            self.connected_ports.insert(source, vec![sink]);
        }

        stream
    }

    pub fn list_streams(&self) -> Vec<StreamDescription> {
        let streams: Vec<StreamDescription> = Arc::clone(&self.streams)
            .iter()
            .map(|entry| entry.value().clone())
            .collect();

        streams
    }

    fn find_stream(&self, source: Port, sink: Port) -> Option<StreamDescription> {
        self.streams.get(&source)
            .map(|r| r.value().clone())
            .filter(|s| s.sink == sink)
    }

    fn get_connected_sinks(&self, source: Port) -> Vec<Port> {
        self.connected_ports.get(&source).unwrap().clone()
    }
}

impl ConnectionManager {
    pub fn new(rx_con_tx: mpsc::Sender<HandleConnectionJob>) -> Self {
        
        Self { 
            port_listeners: Arc::new(Mutex::new(HashMap::new())),
            processing_job_subscribers: Vec::new(), // todo
            rx_con_tx: Arc::new(rx_con_tx),
            connections: Arc::new(DashMap::new()),
        }
    }

    fn subscribe(&mut self, sub: mpsc::Sender<ProcessingJob>) { // todo
        self.processing_job_subscribers.push(sub);
    }

    async fn notify_job(&mut self, job: ProcessingJob) { // todo
        for tx in self.processing_job_subscribers.iter() {
            tx.send(job.clone()).await.is_ok();
        }    
    }

    pub fn add_connection(&self, port: Port, socket: Arc<Mutex<TcpStream>>) {
        self.connections.insert(port, socket);
    }

    fn get_connection(&self, port: &Port) -> Option<Arc<Mutex<TcpStream>>> {
        self.connections.get(port).map(|r| Arc::clone(r.value()))
    }

    async fn bind<F, Fut>(&self, port: Port, handler: F) -> io::Result<()> 
    where
        F: Fn(Port, TcpListener) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let cell = {
            let mut map = self.port_listeners.lock().await;
            let cell = map.entry(port).or_insert_with(|| Arc::new(OnceCell::new()));
            Arc::clone(cell)
        };

        cell.get_or_init(|| async {
            info!("Binding to port {}", port);

            let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;

            tokio::spawn(async move {
                handler(port, listener).await;
            });

            Ok(())
        }).await;

        Ok(()) // todo
    }

    async fn connect_source(&self, source: Port) -> io::Result<()> {
        let rx_con_tx = Arc::clone(&self.rx_con_tx);
        let connections = Arc::clone(&self.connections);

        self.bind(source, move |port, listener| {
            let rx_con_tx = Arc::clone(&rx_con_tx);
            let connections = Arc::clone(&connections);

            async move {
                info!("Awaiting connections on source {}", port);
                
                loop {
                    match listener.accept().await {
                        Ok((src_socket, addr)) => {
                            info!("New connection on source port {} from {:?} accepted", port, addr);
                            
                            let socket_arc = Arc::new(Mutex::new(src_socket));
                            connections.insert(port, Arc::clone(&socket_arc));

                            let job = HandleConnectionJob {
                                source: port,
                                socket: socket_arc,
                            };
                            let _ = rx_con_tx.send(job).await;
                            // trace!("HandleConnectionJob submitted {:?}", job);
                        }
                        Err(e) => error!("Failed to accept connection on port {}: {}", port, e),
                    }                   
                }
            }
        }).await
    }

    async fn connect_sink(&self, sink: Port) -> io::Result<()> {
        let connections = Arc::clone(&self.connections);

        self.bind(sink, move |port, listener| {
            let connections = Arc::clone(&connections);

            async move {
                info!("Awaiting connections on sink port {}", port);
                
                loop {
                    match listener.accept().await {
                        Ok((sink_socket, addr)) => {
                            info!("New connection on sink port {} from {:?} accepted", port, addr);
                            
                            connections.insert(port, Arc::new(Mutex::new(sink_socket)));
                        }
                        Err(e) => error!("Failed to accept connection on port {}: {}", port, e),
                    }               
                }
            }
        }).await
    }
}

impl DataRx {
    pub fn new(con_rx: mpsc::Receiver<HandleConnectionJob>, proc_tx: mpsc::Sender<ProcessingJob>) -> Self {
        Self { 
            con_rx: Arc::new(Mutex::new(con_rx)),            
            proc_tx: Arc::new(proc_tx),
        }
    }

    fn start(&self) { 
        let con_rx = Arc::clone(&self.con_rx);
        let source_tx = Arc::clone(&self.proc_tx);

        // spawn a Tokio task to handle incoming connection jobs (I/O bound)
        tokio::spawn(async move {
            let mut con_rx = con_rx.lock().await;

            while let Some(job) = con_rx.recv().await {
                trace!("HandleConnectionJob received {:?}", job);

                let source_tx = Arc::clone(&source_tx);

                tokio::spawn(async move {
                    let mut buffer = [0; 1024];

                    let mut socket = job.socket.lock().await;
                    while let Ok(n) = socket.read(&mut buffer).await {
                        if n == 0 { break; }

                        trace!("Data received on source {:?} from {:?}", socket.local_addr(), socket.peer_addr());
                        
                        let data = buffer[..n].to_vec();
                        let job = ProcessingJob {
                            data,
                            n,
                            source: job.source
                        };
                        source_tx.send(job).await.unwrap();
                        // trace!("ProcessingJob submitted {:?}", job);
                    }
                });
            }
        });
    }
}

impl DataTx {
    fn new(stream_registry: Arc<StreamRegistry>, connection_manager: Arc<ConnectionManager>, sink_rx: mpsc::Receiver<SinkWriteJob>,) -> Self {
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
                trace!("SinkWriteJob received {:?}", job);

                let sink_ports = stream_registry.get_connected_sinks(job.source);
                for sink_port in sink_ports {
                    let sink_socket = connection_manager.get_connection(&sink_port).unwrap();
                    let data = job.data.clone();
                    
                    tokio::spawn(async move {
                        let mut sink_socket = sink_socket.lock().await;
                        let _ = sink_socket.write_all(&data).await;
                        let _ = sink_socket.flush().await;

                        trace!("Data flushed on sink {:?} to {:?}", sink_socket.local_addr(), sink_socket.peer_addr());
                    });
                }
            }
        });
    }
}