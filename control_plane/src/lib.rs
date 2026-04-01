mod grpc;
pub mod config_store;

use std::net::SocketAddr;
use std::sync::Arc;

use tonic::transport::Server;

use grpc::api::streamer_server::StreamerServer;
use grpc::StreamerImpl;

use data_plane::DataPlane;
use config_store::ConfigStore;

pub struct ControlPlane<C: ConfigStore> {
    config_store: Arc<C>,
    data_plane: Arc<DataPlane>,
}

impl<C: ConfigStore + 'static> ControlPlane<C> {
    pub fn new(config_store: Arc<C>, data_plane: Arc<DataPlane>) -> Self {
        Self { 
            config_store,
            data_plane
        }
    }
    
    pub async fn start(&self, addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
        let streamer = StreamerImpl::new(Arc::clone(&self.config_store), Arc::clone(&self.data_plane));
 
        let reflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(include_bytes!(concat!(env!("OUT_DIR"), "/api_descriptor.bin")))
            .build_v1()?;
 
        println!("Starting control plane. gRPC server listening on {}", addr);
 
        Server::builder()
            .add_service(reflection_service)
            .add_service(StreamerServer::new(streamer))
            .serve(addr)
            .await?;
 
        Ok(())
    }
}