use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::ControlPlane;
use crate::config_store::{ConfigStore, ConfigStoreError};

pub mod grpc_api {
    tonic::include_proto!("grpc_api");
}

use grpc_api::streamer_server::Streamer;
use grpc_api::*;

pub struct StreamerImpl<C: ConfigStore> {
    config_store: Arc<C>,
    ctrl_plane: Arc<ControlPlane<C>>,
}

impl<C: ConfigStore> StreamerImpl<C> {
    pub fn new(config_store: Arc<C>, ctrl_plane: Arc<ControlPlane<C>>) -> Self {
        Self {
            config_store,
            ctrl_plane,
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
            streams: self.ctrl_plane.list_provisioned_streams()
                .into_iter()
                .map(|stream|
                    ShortStreamDescription {
                        id: stream.id,
                        source_port: u32::from(stream.source),
                        sink_port: u32::from(stream.sink),
                    }
                )
                .collect(),
        };
        Ok(Response::new(reply))
    }

    async fn provision_stream(
        &self,
        request: Request<ProvisionStreamRequest>,
    ) -> Result<Response<ProvisionStreamReply>, Status> {
        let request  = request.into_inner();

        if u16::try_from(request.source_port).is_err() || u16::try_from(request.sink_port).is_err() {
            return Err(Status::invalid_argument("Invalid port number"));
        }

        let result = match self.ctrl_plane.provision_stream(request.source_port as u16, request.sink_port as u16).await {
             Ok(stream) => {
                let reply = ProvisionStreamReply {
                    stream: Some(FullStreamDescription {
                                id: stream.id,
                                source_port: u32::from(stream.source),
                                sink_port: u32::from(stream.sink),
                            })
                };
                Ok(Response::new(reply))
            },
            Err(error) => {
                match error.kind() {
                    std::io::ErrorKind::AddrInUse => Err(Status::already_exists(format!("Port already in use: {}", error))),
                    _ => Err(Status::internal(format!("Failed to provision stream: {}", error))),
                }
            },
        };

        result
    }

    async fn deprovision_stream(
        &self,
        request: Request<DeprovisionStreamRequest>,
    ) -> Result<Response<DeprovisionStreamReply>, Status> {
        let result = match self.ctrl_plane.deprovision_stream(&request.into_inner().stream_id).await {
            Ok(_) => {
                let reply = DeprovisionStreamReply {
                };
                Ok(Response::new(reply))
            },
            Err(_) => {
                Err(Status::not_found("Stream not found"))
            },
        };

        result
    }
}