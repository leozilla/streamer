mod common;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpSocket;

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
        sink_port: 1,
        description: "My awesome stream".into(),
    });
    let response = client.provision_stream(request).await.unwrap();

    assert_eq!(response.into_inner().stream.is_some(), true);

    let request = tonic::Request::new(ListProvisionedStreamsRequest {
    });
    let response = client.list_provisioned_streams(request).await.unwrap();

    assert_eq!(response.into_inner().streams.len(), 1);
}

#[tokio::test]
async fn test_stream_data_flow_integration() {
    common::start_server().await;
    let mut client = common::connect_grpc_client().await;

    let _ = common::api_provision_stream(&mut client, 32000, 32001).await;

    let mut src_socket = common::connect(32000).await.expect("Bound to source port");
    let mut sink_socket = common::connect(32001).await.expect("Connected to sink port");

    let data = "Hello from the Rust test!".as_bytes();
    src_socket.write_all(&data).await.expect("Failed to write to stream");
    src_socket.flush().await.expect("Failed to flush stream");

    let mut buf = Vec::new();
    sink_socket.read_to_end(&mut buf).await.expect("Failed to read from stream");

    assert_eq!(buf, data);
}