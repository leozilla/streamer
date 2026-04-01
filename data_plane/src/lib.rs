use dashmap::DashMap;
use std::sync::Arc;

type Port = u32; // todo should be u16

pub struct DataPlane {
    stream_manager: StreamManager,
}

pub struct StreamManager {
    streams: Arc<DashMap<Port, Stream>>,
}

impl DataPlane {
    pub fn new() -> Self {
        Self { 
            stream_manager: StreamManager::new()
        }
    }
    
    pub fn start(&self) -> Result<(), Box<dyn std::error::Error>> { 
        println!("Starting data plane.");
  
        Ok(())
    }

    pub fn list_provisioned_streams(&self) -> Vec<Stream> {
        self.stream_manager.list_provisioned_streams()
    }

}

#[derive(Clone)]
pub struct Stream {
    pub source: Port,
    pub sinks: Vec<Port>,
}

impl StreamManager {
    pub fn new() -> Self {
        Self { 
            streams: Arc::new(DashMap::new())
        }
    }

    pub fn provision_stream(&self, source: Port, sinks: Vec<Port>) -> Stream {
        let stream = Stream {
            source,
            sinks,
        };

        self.streams.insert(source, stream.clone());

        stream
    }

    pub fn list_provisioned_streams(&self) -> Vec<Stream> {
        let streams: Vec<Stream> = Arc::clone(&self.streams)
            .iter()
            .map(|entry| entry.value().clone())
            .collect();

        streams
    }
}