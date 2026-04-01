mod grpc;
pub mod config;

pub use config::Config;
use data_plane::DataPlane;
use std::sync::Arc;
 
use tonic::transport::Server;

use grpc::api::streamer_server::StreamerServer;
use grpc::StreamerImpl;

pub struct ControlPlane<'a> {
    config: &'a Config,
    data_plane: Arc<DataPlane>,
}

impl<'a> ControlPlane<'a> {
    pub fn new(config: &'a Config, data_plane: Arc<DataPlane>) -> Self {
        Self { 
            config,
            data_plane
        }
    }
    
    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        let addr = self.config.server.bind_address;
        let streamer = StreamerImpl::new(self.config.clone(), Arc::clone(&self.data_plane));
 
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