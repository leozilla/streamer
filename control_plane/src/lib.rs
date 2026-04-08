mod grpc;
mod web;
pub mod config_store;
pub use grpc::grpc_api;

use std::io;
use std::sync::Arc;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};

use dashmap::DashMap;
use tokio::sync::broadcast;

use tracing::{info, debug};
use tonic::transport::Server;

use grpc::grpc_api::streamer_server::StreamerServer;
use grpc::StreamerImpl;
use web::WebServer;
use data_plane::DataPlane;
use config_store::ConfigStore;
use data_plane::StreamDescription;
use data_plane::Port;

pub struct ControlPlane<C: ConfigStore> {
    config_store: Arc<C>,
    data_plane: Arc<DataPlane>,
    stream_registry: StreamRegistry,
    event_tx: broadcast::Sender<ControlPlaneEvent>,
}

struct StreamRegistry {
    id_counter: AtomicU32,
    streams_by_source_port: DashMap<Port, Vec<Arc<StreamDescription>>>,
    streams_by_id: DashMap<String, Arc<StreamDescription>>,
}

#[derive(Clone, Debug)]
pub enum ControlPlaneEvent {
    StreamProvisioned {
        id: String,
        source_port: u16,
        sink_port: u16,
    },
    StreamDeprovisioned {
        id: String,
    }
}

impl<C: ConfigStore + 'static> ControlPlane<C> {
    pub fn new(config_store: Arc<C>, data_plane: Arc<DataPlane>, event_tx: broadcast::Sender<ControlPlaneEvent>) -> Self {
        let stream_registry = StreamRegistry::new();
        
        Self {
            config_store,
            data_plane,
            stream_registry,
            event_tx
        }
    }
    
    pub async fn start(self: Arc<Self>, grpc_server_addr: SocketAddr, web_server_addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
        info!("Starting control plane");

        let streamer = StreamerImpl::new(Arc::clone(&self.config_store), Arc::clone(&self));
        let reflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(include_bytes!(concat!(env!("OUT_DIR"), "/api_descriptor.bin")))
            .build_v1()?; 
        let grpc_future = async move {
            info!("gRPC server listening on {}", grpc_server_addr);
            
            Server::builder()
                .add_service(reflection_service)
                .add_service(StreamerServer::new(streamer))
                .serve(grpc_server_addr)
                .await
                .map_err(Into::<Box<dyn std::error::Error>>::into)
        };

        let self_clone = Arc::clone(&self);
        let data_plane = Arc::clone(&self.data_plane);
        let ws_future = async move {
            info!("Web server listening on {}", web_server_addr);
            
            WebServer::new(self_clone, data_plane)
                .start(web_server_addr)
                .await
                .map_err(Into::<Box<dyn std::error::Error>>::into)
        };

        tokio::try_join!(grpc_future, ws_future)?;
 
        Ok(())
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

        let stream = self.stream_registry.add_stream(source, sink);
        self.data_plane.provision_stream(&stream).await?;
        info!("New stream with Id {} provisioned {} -> {}", stream.id, source, sink);

        self.notify_stream_provisioned(&stream);

        Ok(stream)
    }

    pub async fn deprovision_stream(&self, id: &str) -> io::Result<StreamDescription> {
        debug!("Deprovisioning stream with Id {}", id);

        if let Some(stream) = self.stream_registry.remove_stream(id) {
            
            let has_source = self.stream_registry.has_source(stream.source);
            let has_sink = self.stream_registry.has_sink(stream.sink);

            self.data_plane.deprovision_stream(&stream, has_source, has_sink).await?;

            info!("Stream deprovisioned, Id {}, {} -> {}", stream.id, stream.source, stream.sink);
            self.notify_stream_deprovisioned(&stream);          
            
            Ok(stream)
        } else {
            debug!("Stream with id {} not found", id);
            Err(io::Error::new(io::ErrorKind::NotFound, "Stream not found"))
        }
    }

    fn notify_stream_provisioned(&self, stream: &StreamDescription) {
        let event = ControlPlaneEvent::StreamProvisioned { 
            id: stream.id.clone(), 
            source_port: stream.source, 
            sink_port: stream.sink
        };
        let _ = self.event_tx.send(event);
    }

    fn notify_stream_deprovisioned(&self, stream: &StreamDescription) {
        let event = ControlPlaneEvent::StreamDeprovisioned { 
            id: stream.id.clone(),
        };
        let _ = self.event_tx.send(event);
    }
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