mod processor;

use std::io;
use std::sync::Arc;

use tracing::{info, debug, trace};

use dashmap::DashMap;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::{TcpStream, TcpListener}};

type Port = u32; // todo should be u16

pub struct DataPlane {
    stream_registry: StreamRegistry,
    connection_manager: ConnectionManager,
}

pub struct StreamRegistry {
    streams: Arc<DashMap<Port, StreamDescription>>,
}

pub struct ConnectionManager {
    listeners: DashMap<Port, Arc<TcpListener>>,
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
        self.stream_registry.list_streams()
    }

    pub async fn provision_stream(&self, source: Port, sink: Port) -> io::Result<StreamDescription> {
        info!("Provisioning stream {} -> {}", source, sink);
        
        if let Some(stream) = self.stream_registry.find_stream(source, sink) {
            info!("Stream with id {} already provisioned {} -> {}", stream.id, source, sink);
            return Ok(stream);
        }

        let _ = self.connection_manager.connect_stream(source, sink).await?;

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
            streams: Arc::new(DashMap::new())
        }
    }

    pub fn add_stream(&self, source: Port, sink: Port) -> StreamDescription {
        let stream = StreamDescription {
            id: "todo".to_string(),
            source,
            sink,
        };

        self.streams.insert(source, stream.clone());

        stream
    }

    pub fn find_stream(&self, source: Port, sink: Port) -> Option<StreamDescription> {
        self.streams.get(&source)
            .map(|r| r.value().clone())
            .filter(|s| s.sink == sink)
    } 

    pub fn list_streams(&self) -> Vec<StreamDescription> {
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
            listeners: DashMap::new()
        }
    }

    async fn bind(&self, port: Port) -> io::Result<TcpListener> {
        info!("Binding to port {}", port);

        let listener: tokio::net::TcpListener  = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
        Ok(listener)
    }

    async fn connect_stream(&self, source: Port, sink: Port) -> io::Result<()> {
        let src_listener = Arc::new(self.bind(source).await?);
        self.listeners.insert(source, Arc::clone(&src_listener));

        let sink_listener = Arc::new(self.bind(sink).await?);
        self.listeners.insert(sink, Arc::clone(&sink_listener));

        tokio::spawn(async move {
            info!("Awaiting connections on source port {} and sink port {}", source, sink);
            
            loop {
                let ((mut src_socket, _), (mut sink_socket, _)) = tokio::try_join!(src_listener.accept(), sink_listener.accept()).unwrap();
                info!("Connection on source port {} and sink port {} established", source, sink);

                let (tx_chan, mut rx_chan) = tokio::sync::mpsc::unbounded_channel();

                // spawn a Tokio task for each connection (I/O bound)
                tokio::spawn(async move {
                    let mut buffer = [0; 1024];
                    
                    while let Ok(n) = src_socket.read(&mut buffer).await {
                        if n == 0 { break; }

                        trace!("Data received on source {}", source);
                        
                        processor::process_stream_data(sink, &tx_chan, buffer, n);
                    }
                });

                // spawn a Tokio task to send data to the sink of the stream (also I/O bound)
                tokio::spawn(async move {
                    while let Some(data) = rx_chan.recv().await {
                        trace!("Processed data received, writting data out to sink {}", sink);

                        let _ = sink_socket.write_all(&data).await;
                        let _ = sink_socket.flush().await;

                        trace!("Data flushed on sink {}", sink);
                    }
                });
            }
        });

        Ok(())
    }
}