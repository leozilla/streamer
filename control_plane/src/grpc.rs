pub mod api {
    tonic::include_proto!("api");
}

use std::sync::Arc;

use tonic::{Request, Response, Status};

use data_plane::DataPlane;
use api::streamer_server::Streamer;
use api::*;
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
            Err(error) => match error {
                ConfigStoreError::InvalidArg => Err(Status::invalid_argument(error.to_string())),
                ConfigStoreError::Unknown => Err(Status::internal(error.to_string())),
            }
        };

        result
    }

    async fn list_provisioned_streams(
        &self,
        _: Request<ListProvisionedStreamsRequest>,
    ) -> Result<Response<ListProvisionedStreamsReply>, Status> {
        let reply = ListProvisionedStreamsReply {
            streams: self.data_plane.list_provisioned_streams()
                .into_iter()
                .map(|stream|
                    ShortStreamDescription {
                        id: "todo".to_string(),
                        source_port: stream.source,
                        sink_ports: stream.sinks,
                    }
                )
                .collect(),
        };
        Ok(Response::new(reply))
    }

    async fn provision_stream(
        &self,
        _: Request<ProvisionStreamRequest>,
    ) -> Result<Response<ProvisionStreamReply>, Status> {
        let p: Vec<u32> = Vec::new();
        let reply = ProvisionStreamReply {
            stream: Some(FullStreamDescription {
                        id: "todo".to_string(),
                        source_port: 1,
                        sink_ports: p,
                    })
        };
        Ok(Response::new(reply))
    }
}