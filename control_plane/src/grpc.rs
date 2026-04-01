use tonic::{Request, Response, Status};

pub mod api {
    tonic::include_proto!("api");
}

use crate::Config;
use std::sync::Arc;

use data_plane::DataPlane;

use api::streamer_server::Streamer;
use api::{GetConfigRequest, GetConfigReply};

pub struct StreamerImpl {
    config: Config,
    data_plane: Arc<DataPlane>,
}

impl StreamerImpl {
    pub fn new(config: Config, data_plane: Arc<DataPlane>) -> Self {
        Self { 
            config,
            data_plane,
        }
    }
}

#[tonic::async_trait]
impl Streamer for StreamerImpl {
    async fn get_config(
        &self,
        _: Request<GetConfigRequest>,
    ) -> Result<Response<GetConfigReply>, Status> {
        let reply = GetConfigReply {
            total_supported_streams: self.config.total_supported_streams
        };
        Ok(Response::new(reply))
    }
}