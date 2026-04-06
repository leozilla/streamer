mod grpc;
mod web;
pub mod config_store;
use axum::extract::ws::WebSocket;
pub use grpc::grpc_api;

use std::net::SocketAddr;
use std::sync::Arc;

use tracing::info;
use tonic::transport::Server;

use grpc::grpc_api::streamer_server::StreamerServer;
use grpc::StreamerImpl;
use web::WebServer;
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
    
    pub async fn start(&self, grpc_server_addr: SocketAddr, web_server_addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
        let streamer = StreamerImpl::new(Arc::clone(&self.config_store), Arc::clone(&self.data_plane));
 
        let reflection_service = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(include_bytes!(concat!(env!("OUT_DIR"), "/api_descriptor.bin")))
            .build_v1()?;
 
        info!("Starting control plane.");
 
        let grpc_future = async {
            info!("gRPC server listening on {}", grpc_server_addr);
            
            Server::builder()
                .add_service(reflection_service)
                .add_service(StreamerServer::new(streamer))
                .serve(grpc_server_addr)
                .await
                .map_err(Into::<Box<dyn std::error::Error>>::into)
        };

        let ws_future = async {
            info!("Web server listening on {}", web_server_addr);
            
            WebServer::new()
                .start(web_server_addr)
                .await
                .map_err(Into::<Box<dyn std::error::Error>>::into)
        };

        tokio::try_join!(grpc_future, ws_future)?;
 
        Ok(())
    }
}