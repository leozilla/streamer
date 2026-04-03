use std::io;
use std::sync::Arc;

use tracing::{info, debug, span, instrument, Level};

use dashmap::DashMap;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream};

type Port = u32; // todo should be u16

pub struct DataPlane {
    stream_registry: StreamRegistry,
    connection_manager: ConnectionManager,
}

pub struct StreamRegistry {
    streams: Arc<DashMap<Port, StreamDescription>>,
}

pub struct ConnectionManager {
    connections: Arc<DashMap<Port, Arc<tokio::sync::Mutex<Connection>>>>,
}

impl DataPlane {
    pub fn new() -> Self {
        Self { 
            stream_registry: StreamRegistry::new(),
            connection_manager: ConnectionManager::new()
        }
    }
    
    pub fn start(&self) -> Result<(), Box<dyn std::error::Error>> { 
        info!("Starting data plane.");
  
        Ok(())
    }

    pub fn list_provisioned_streams(&self) -> Vec<StreamDescription> {
        self.stream_registry.list_provisioned_streams()
    }

    pub async fn provision_stream(&self, source: Port, sink: Port) -> io::Result<StreamDescription> {
        info!("Provisioning stream {} -> {}", source, sink);
        
        let _ = self.connection_manager.connect_stream(source, sink).await?;

        let stream = self.stream_registry.add_provisioned_stream(source, sink);
        info!("New stream with Id {} provisioned and running {} -> {}", stream.id, source, sink);
        Ok(stream)
    }

}

#[derive(Clone)]
pub struct StreamDescription {
    id: String,
    pub source: Port,
    pub sink: Port,
}

struct Connection {
    port: Port,
    stream: TcpStream,
}

struct Stream {
    source: Arc<tokio::sync::Mutex<Connection>>,
    sink: Arc<tokio::sync::Mutex<Connection>>,
}

impl StreamRegistry {
    pub fn new() -> Self {
        Self { 
            streams: Arc::new(DashMap::new())
        }
    }

    pub fn add_provisioned_stream(&self, source: Port, sink: Port) -> StreamDescription {
        let stream = StreamDescription {
            id: "todo".to_string(),
            source,
            sink,
        };

        self.streams.insert(source, stream.clone());

        stream
    }

    pub fn list_provisioned_streams(&self) -> Vec<StreamDescription> {
        let streams: Vec<StreamDescription> = Arc::clone(&self.streams)
            .iter()
            .map(|entry| entry.value().clone())
            .collect();

        streams
    }
}

impl ConnectionManager {
    pub fn new() -> Self {
        Self { 
            connections: Arc::new(DashMap::new())
        }
    }

    async fn bind(&self, port: Port) -> io::Result<Arc<tokio::sync::Mutex<Connection>>> {
        debug!("Binding to port {}", port);
        let listener  = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
        
        let (stream, _) = listener.accept().await?;

        let con = Arc::new(tokio::sync::Mutex::new(Connection { 
            port,
            stream,
        }));

        self.connections.insert(port, Arc::clone(&con));

        Ok(con)
    }

    async fn connect_stream(&self, source: Port, sink: Port) -> io::Result<Stream> {
        let (src_socket, sink_socket) = tokio::try_join!(self.bind(source), self.bind(sink))?;

        let (tx_chan, mut rx_chan) = tokio::sync::mpsc::unbounded_channel();

        let connect_span = span!(Level::INFO, "connect_stream");
        let _enter = connect_span.enter();

        let src_socket_clone = Arc::clone(&src_socket);
        // spawn a Tokio task for each connection (I/O bound)
        tokio::spawn(async move {
            let mut buffer = [0; 1024];
            
            while let Ok(n) = src_socket_clone.lock().await.stream.read(&mut buffer).await {
                if n == 0 { break; }
                
                let data = buffer[..n].to_vec();
                let tx_inner = tx_chan.clone();

                // HAND OFF TO RAYON (CPU bound)
                // We use a oneshot-style handoff or spawn into the global pool
                rayon::spawn(move || {
                    // let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                    // encoder.write_all(&data_chunk).unwrap();
                    // let compressed = encoder.finish().unwrap();
                    
                    // Send to further processing
                    let _ = tx_inner.send(data);
                });
            }
        });

        let sink_socket_clone = Arc::clone(&sink_socket);
        // spawn a Tokio task to send data to the sink stream
        tokio::spawn(async move {
            while let Some(data) = rx_chan.recv().await {
                let mut sink_socket_guard = sink_socket_clone.lock().await;
                let _ = sink_socket_guard.stream.write_all(&data).await;
                let _ = sink_socket_guard.stream.flush().await;
            }
        });

        let stream = Stream {
            source: src_socket,
            sink: sink_socket,
        };

        Ok(stream)
    }
}