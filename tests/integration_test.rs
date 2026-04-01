mod common;

use control_plane::api::*;

#[tokio::test]
async fn test_get_config_integration() {
    common::start_server().await;
    let mut client = common::connect_grpc_client().await;

    let request = tonic::Request::new(GetConfigRequest {
    });
    let response = client.get_config(request).await.unwrap();

    assert_eq!(response.into_inner().total_supported_streams, 100);
}

#[tokio::test]
async fn test_list_streams_integration() {
    common::start_server().await;
    let mut client = common::connect_grpc_client().await;

    let request = tonic::Request::new(ListProvisionedStreamsRequest {
    });
    let response = client.list_provisioned_streams(request).await.unwrap();

    assert_eq!(response.into_inner().streams.len(), 0);

    let request = tonic::Request::new(ProvisionStreamRequest {
        source_port: 1,
        sink_ports: Vec::new(),
        description: "My awesome stream".into(),
    });
    let response = client.provision_stream(request).await.unwrap();

    assert_eq!(response.into_inner().stream.is_some(), true);

    let request = tonic::Request::new(ListProvisionedStreamsRequest {
    });
    let response = client.list_provisioned_streams(request).await.unwrap();

    assert_eq!(response.into_inner().streams.len(), 1);
}