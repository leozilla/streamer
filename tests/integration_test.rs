mod common;

use std::io::{Read, Write};
use std::net::{SocketAddrV4, TcpListener, TcpStream};
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

#[tokio::test]
async fn test_stream_data_flow_integration() {
    common::start_server().await;
    let mut client = common::connect_grpc_client().await;

    let _ = common::provision_stream(&mut client, 32000, vec![32001]).await;

    let message = "Hello from the Rust test!";
    let addr = SocketAddrV4::new("127.0.0.1".parse().unwrap(), 32000);
    let mut client_stream = TcpStream::connect(addr).expect("Could not connect to server");
    
    client_stream.write_all(message.as_bytes()).expect("Failed to write to stream");
    client_stream.flush().expect("Failed to flush stream");
}