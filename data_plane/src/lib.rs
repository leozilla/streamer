use dashmap::DashMap;
use std::io;
use std::sync::Arc;
use std::net::{TcpStream, TcpListener};

use thiserror::Error;

type Port = u32; // todo should be u16

pub struct DataPlane {
    stream_registry: StreamRegistry,
    connection_manager: ConnectionManager,
}

pub struct StreamRegistry {
    streams: Arc<DashMap<Port, ProvisionedStream>>,
}

pub struct ConnectionManager {
    connections: Arc<DashMap<Port, Connection>>,
}

impl DataPlane {
    pub fn new() -> Self {
        Self { 
            stream_registry: StreamRegistry::new(),
            connection_manager: ConnectionManager::new()
        }
    }
    
    pub fn start(&self) -> Result<(), Box<dyn std::error::Error>> { 
        println!("Starting data plane.");
  
        Ok(())
    }

    pub fn list_provisioned_streams(&self) -> Vec<ProvisionedStream> {
        self.stream_registry.list_provisioned_streams()
    }

    pub fn provision_stream(&self, source: Port, sink: Port) -> io::Result<ProvisionedStream> {
        let _ = self.connection_manager.connect_stream(source, sink)?;

        let stream = self.stream_registry.add_provisioned_stream(source, sink);
        Ok(stream)
    }

}

#[derive(Clone)]
pub struct ProvisionedStream {
    pub source: Port,
    pub sink: Port,
}

struct Connection {
    port: Port,
    mut stream: TcpStream,
}

struct Stream {
    source: Connection,
    sink: Connection,
}

impl StreamRegistry {
    pub fn new() -> Self {
        Self { 
            streams: Arc::new(DashMap::new())
        }
    }

    pub fn add_provisioned_stream(&self, source: Port, sink: Port) -> ProvisionedStream {
        let stream = ProvisionedStream {
            source,
            sink,
        };

        self.streams.insert(source, stream.clone());

        stream
    }

    pub fn list_provisioned_streams(&self) -> Vec<ProvisionedStream> {
        let streams: Vec<ProvisionedStream> = Arc::clone(&self.streams)
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

    fn bind(&self, port: Port) -> io::Result<Connection> {
        let listener = TcpListener::bind(format!("0.0.0.0:{}", port))?;
        let (mut stream, _) = listener.accept()?;
        
        let con = Connection { 
            port,
            stream,
        };

        Ok(con)
    }

    fn connect_stream(&self, source: Port, sink: Port) -> io::Result<Stream> {
        let src_con = self.bind(source)?;
        let dst_con = self.bind(sink)?;

        let stream = Stream {
            source: src_con,
            sink: dst_con,
        };

        Ok(stream)
    }
}