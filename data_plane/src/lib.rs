mod processor;
mod rx;
mod tx;

use std::io;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use tracing::{info, debug, trace, error};
use dashmap::DashMap;
use tokio::{net::UdpSocket, sync::{OnceCell, mpsc, Mutex, broadcast}};

use processor::Processor;
use rx::DataRx;
use tx::DataTx;

type Port = u16;

pub struct DataPlane {
    stream_registry: Arc<StreamRegistry>,
    connection_manager: Arc<ConnectionManager>,
    processor: Processor,
    data_rx: DataRx,
    data_tx: DataTx,
    event_tx: broadcast::Sender<DataPlaneEvent>,
}

pub trait ObservableDataPlane {
    fn subscribe_events(&self) -> broadcast::Receiver<DataPlaneEvent>;
}

struct StreamRegistry {
    id_counter: AtomicU32,
    streams_by_source_port: DashMap<Port, Vec<Arc<StreamDescription>>>,
    streams_by_id: DashMap<String, Arc<StreamDescription>>,
}

struct ConnectionManager {
    listeners: Mutex<HashMap<Port, Arc<OnceCell<()>>>>,
    sockets: Arc<DashMap<Port, Arc<Mutex<UdpSocket>>>>,
    con_tx: mpsc::Sender<SourceRxTask>,
    rx_abort_txs: Arc<DashMap<Port, tokio::sync::oneshot::Sender<()>>>,
}

#[derive(Debug)]
pub struct ProcessingJob {
    data: bytes::Bytes,
    sequence_number: u16,
    n: usize,
    source: Port,
}

#[derive(Debug)]
pub struct SinkTxJob {
    data: bytes::Bytes,
    source: Port,
}

#[derive(Debug)]
struct SourceRxTask {
    source: Port,
    socket: Arc<Mutex<UdpSocket>>,
    abort_rx: tokio::sync::oneshot::Receiver<()>,
}

#[derive(Clone, Debug)]
pub enum DataPlaneEvent {
    StreamProvisioned {
        id: String,
        source_port: u16,
        sink_port: u16,
    },
    StreamDeprovisioned {
        id: String,
    },
    StreamUpdated {
        id: String,
        rx_active: bool,
        tx_active: bool,
    }
}

impl DataPlane {
    pub fn new() -> Self {
        let (proc_tx, proc_rx) = mpsc::channel(1024);
        let (sink_tx, sink_rx) = mpsc::channel(1024);
        let (con_tx, con_rx) = mpsc::channel(1024);
        let (event_tx, _event_rx) = broadcast::channel(100);

        let stream_registry = Arc::new(StreamRegistry::new());
        let connection_manager = Arc::new(ConnectionManager::new(con_tx));
        let processor: Processor = Processor::new(proc_rx, sink_tx, event_tx.clone());
        let data_rx = DataRx::new(con_rx, proc_tx, event_tx.clone());
        let data_tx = DataTx::new(Arc::clone(&connection_manager), sink_rx, event_tx.clone());

        Self {
            stream_registry,
            connection_manager,
            processor,
            data_rx,
            data_tx,
            event_tx,
        }
    }
    
    pub fn start(&self) -> Result<(), Box<dyn std::error::Error>> { 
        info!("Starting data plane");
  
        self.data_tx.start();
        self.data_rx.start();        
        self.processor.start();

        Ok(())
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<DataPlaneEvent> {
        self.event_tx.subscribe()
    }

    pub fn list_provisioned_streams(&self) -> Vec<StreamDescription> {
        self.stream_registry.list_streams()
    }

    pub async fn provision_stream(&self, source: Port, sink: Port) -> io::Result<StreamDescription> {
        debug!("Provisioning stream {} -> {}", source, sink);
        
        if let Some(stream) = self.stream_registry.find_stream(source, sink) {
            info!("Stream already provisioned, Id {}, {} -> {}", stream.id, source, sink);
            return Ok(stream);
        }

        let _ = self.connection_manager.connect_source(source).await?;
        let _ = self.connection_manager.connect_sink(sink).await?;

        let stream = self.stream_registry.add_stream(source, sink);
        info!("New stream with Id {} provisioned {} -> {}", stream.id, source, sink);

        self.notify_stream_provisioned(&stream);
        self.data_tx.add_sink(&stream);

        Ok(stream)
    }

    pub async fn deprovision_stream(&self, id: &str) -> Option<StreamDescription> {
         debug!("Deprovisioning stream with Id {}", id);

        if let Some(stream) = self.stream_registry.remove_stream(id) {
            
            if !self.stream_registry.has_source(stream.source) {
                let _ = self.connection_manager.disconnect_source(stream.source).await;
            }
            if !self.stream_registry.has_sink(stream.sink) {
                let _ = self.connection_manager.disconnect_sink(stream.sink).await;
            }

            info!("Stream deprovisioned, Id {}, {} -> {}", stream.id, stream.source, stream.sink);
            self.notify_stream_deprovisioned(&stream);
            self.data_tx.remove_sink(&stream);

            Some(stream)
        } else {
            debug!("Stream with id {} not found", id);
            None
        }
    }
        
    fn notify_stream_provisioned(&self, stream: &StreamDescription) {
        let event = DataPlaneEvent::StreamProvisioned { 
            id: stream.id.clone(), 
            source_port: stream.source, 
            sink_port: stream.sink
        };
        let _ = self.event_tx.send(event);
    }

    fn notify_stream_deprovisioned(&self, stream: &StreamDescription) {
        let event = DataPlaneEvent::StreamDeprovisioned { 
            id: stream.id.clone(),
        };
        let _ = self.event_tx.send(event);
    }
    
    pub fn add_stream_events_subscriber(&self, stream_id: String) {
        todo!()
    }

    pub fn remove_stream_events_subscriber(&self, stream_id: String) {
        todo!()
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
            streams_by_source_port: DashMap::new(),
            streams_by_id: DashMap::new(),
        }
    }

    fn add_stream(&self, source: Port, sink: Port) -> StreamDescription {
        let stream = Arc::new(StreamDescription {
            id: self.id_counter.fetch_add(1, Ordering::SeqCst).to_string(),
            source,
            sink,
        });

        self.streams_by_source_port.entry(source).or_default().push(Arc::clone(&stream));
        self.streams_by_id.insert(stream.id.clone(), Arc::clone(&stream));

        (*stream).clone()
    }

    fn remove_stream(&self, id: &str) -> Option<StreamDescription> {
        if let Some((_, stream)) = self.streams_by_id.remove(id) {
            let mut is_empty = false;
            if let Some(mut vec_ref) = self.streams_by_source_port.get_mut(&stream.source) {
                vec_ref.retain(|s| s.id != id);
                is_empty = vec_ref.is_empty();
            }
            if is_empty {
                self.streams_by_source_port.remove(&stream.source);
            }
            Some((*stream).clone())
        } else {
            None
        }
    }

    fn list_streams(&self) -> Vec<StreamDescription> {
        self.streams_by_id
            .iter()
            .map(|entry| (**entry.value()).clone())
            .collect()
    }

    fn find_stream(&self, source: Port, sink: Port) -> Option<StreamDescription> {
        self.streams_by_source_port.get(&source)
            .and_then(|r| r.value().iter().find(|s| s.sink == sink).map(|s| (**s).clone()))
    }

    fn for_each_stream_by_source<F>(&self, source: Port, mut f: F)
    where
        F: FnMut(&StreamDescription),
    {
        if let Some(r) = self.streams_by_source_port.get(&source) {
            for stream in r.value() {
                f(&**stream);
            }
        }
    }

    fn has_source(&self, source: Port) -> bool {
        self.streams_by_source_port.contains_key(&source)
    }

    fn has_sink(&self, sink: Port) -> bool {
        self.streams_by_id.iter().any(|entry| entry.value().sink == sink)
    }
}

impl ConnectionManager {
    fn new(con_tx: mpsc::Sender<SourceRxTask>) -> Self {    
        Self { 
            listeners: Mutex::new(HashMap::new()),
            con_tx,
            sockets: Arc::new(DashMap::new()),
            rx_abort_txs: Arc::new(DashMap::new()),
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
            info!("Binding to port {}", port);

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
        let rx_abort_txs = Arc::clone(&self.rx_abort_txs);

        self.bind(source, move |port, src_socket| {
            let con_tx = con_tx.clone();
            let sockets = Arc::clone(&sockets);
            let rx_abort_txs = Arc::clone(&rx_abort_txs);

            async move {
                let socket_arc = Arc::new(Mutex::new(src_socket));
                sockets.insert(port, Arc::clone(&socket_arc));

                let (abort_tx, abort_rx) = tokio::sync::oneshot::channel();
                rx_abort_txs.insert(port, abort_tx);

                let job = SourceRxTask {
                    source: port,
                    socket: socket_arc,
                    abort_rx,
                };
                trace!("Submitting SourceRxTask, source: {}", job.source);
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

    async fn disconnect_source(&self, source: Port) -> io::Result<()> {
        self.listeners.lock().await.remove(&source);
        self.sockets.remove(&source);
        if let Some((_, abort_tx)) = self.rx_abort_txs.remove(&source) {
            let _ = abort_tx.send(());
        }
        Ok(())
    }

    async fn disconnect_sink(&self, sink: Port) -> io::Result<()> {
        self.sockets.remove(&sink);
        Ok(())
    }
}
