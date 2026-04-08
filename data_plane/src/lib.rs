mod processor;
mod rx;
mod tx;

use std::io;
use std::collections::HashMap;
use std::sync::Arc;

use tracing::{info, debug, trace, error};
use dashmap::DashMap;
use tokio::{net::UdpSocket, sync::{OnceCell, mpsc, Mutex, broadcast}};

use processor::Processor;
use rx::DataRx;
use tx::DataTx;

pub type Port = u16;

pub struct DataPlane {
    connection_manager: Arc<ConnectionManager>,
    processor: Processor,
    data_rx: DataRx,
    data_tx: DataTx,
    event_tx: broadcast::Sender<DataPlaneEvent>,
}

pub trait ObservableDataPlane {
    fn subscribe_events(&self) -> broadcast::Receiver<DataPlaneEvent>;
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
    StreamUpdated {
        id: String,
        rx_active: bool,
        tx_active: bool,
    }
}

impl DataPlane {
    pub fn new(event_tx: broadcast::Sender<DataPlaneEvent>) -> Self {
        let (proc_tx, proc_rx) = mpsc::channel(1024);
        let (sink_tx, sink_rx) = mpsc::channel(1024);
        let (con_tx, con_rx) = mpsc::channel(1024);
        
        let connection_manager = Arc::new(ConnectionManager::new(con_tx));
        let processor: Processor = Processor::new(proc_rx, sink_tx, event_tx.clone());
        let data_rx = DataRx::new(con_rx, proc_tx, event_tx.clone());
        let data_tx = DataTx::new(Arc::clone(&connection_manager), sink_rx, event_tx.clone());

        Self {
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

    pub async fn provision_stream(&self, stream: &StreamDescription) -> io::Result<()> {
        let _ = self.connection_manager.connect_source(stream.source).await?;
        let _ = self.connection_manager.connect_sink(stream.sink).await?;

        self.data_tx.add_sink(&stream);

        Ok(())
    }

    pub async fn deprovision_stream(&self, stream: &StreamDescription, has_source: bool, has_sink: bool) -> io::Result<()> {
        if !has_source {
            let _ = self.connection_manager.disconnect_source(stream.source).await;
        }
        if !has_sink {
            let _ = self.connection_manager.disconnect_sink(stream.sink).await;
        }

        self.data_tx.remove_sink(&stream);

        Ok(())
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
