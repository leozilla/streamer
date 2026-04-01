use tonic::{Request, Response, Status};

pub mod api {
    tonic::include_proto!("api");
}

use crate::Config;

use api::streamer_server::Streamer;
use api::{GetConfigRequest, GetConfigReply};

pub struct StreamerImpl {
    config: Config,
}

impl StreamerImpl {
    pub fn new(config: Config) -> Self {
        Self { 
            config
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