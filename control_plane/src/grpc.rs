pub mod api {
    tonic::include_proto!("api");
}

use std::sync::Arc;

use tonic::{Request, Response, Status};

use data_plane::DataPlane;
use api::streamer_server::Streamer;
use api::{GetConfigRequest, GetConfigReply, SetConfigRequest, SetConfigReply};
use crate::config_store::{ConfigStore, ConfigStoreError};

pub struct StreamerImpl<C: ConfigStore> {
    config_store: Arc<C>,
    data_plane: Arc<DataPlane>,
}

impl<C: ConfigStore> StreamerImpl<C> {
    pub fn new(config_store: Arc<C>, data_plane: Arc<DataPlane>) -> Self {
        Self {
            config_store,
            data_plane,
        }
    }
}

#[tonic::async_trait]
impl<C: ConfigStore + 'static> Streamer for StreamerImpl<C> {
    async fn get_config(
        &self,
        _: Request<GetConfigRequest>,
    ) -> Result<Response<GetConfigReply>, Status> {
        let reply = GetConfigReply {
            total_supported_streams: self.config_store.total_supported_streams()
        };
        Ok(Response::new(reply))
    }

    async fn set_config(
        &self,
        request: Request<SetConfigRequest>,
    ) -> Result<Response<SetConfigReply>, Status> {
        let result = match self.config_store.set_new_config(request.into_inner().total_supported_streams) {  
            Ok(_) => {
                let reply = SetConfigReply {
                };
                Ok(Response::new(reply))
            },
            Err(ConfigStoreError::OutOfBonds) => Err(Status::invalid_argument("invalid supported streams (expected to be in range 10-100)")),
            Err(ConfigStoreError::Unknown) => Err(Status::internal("unknown error")),
        };

        result
    }
}