mod common;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpSocket;

use control_plane::api::*;

#[tokio::test]
#[ignore]
async fn test_get_config_integration() {
    common::start_server().await;
    let mut client = common::connect_grpc_client().await;

    let request = tonic::Request::new(GetConfigRequest {
    });
    let response = client.get_config(request).await.unwrap();

    assert_eq!(response.into_inner().total_supported_streams, 100);
}

#[tokio::test]
#[ignore]
async fn test_list_streams_integration() {
    common::start_server().await;
    let mut client = common::connect_grpc_client().await;

    let request = tonic::Request::new(ListProvisionedStreamsRequest {
    });
    let response = client.list_provisioned_streams(request).await.unwrap();

    assert_eq!(response.into_inner().streams.len(), 0);

    let request = tonic::Request::new(ProvisionStreamRequest {
        source_port: 32000,
        sink_port: 32001,
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
#[ignore]
async fn test_stream_tcp_data_flow_integration() {
    common::start_server().await;
    let mut client = common::connect_grpc_client().await;

    let src_port = 32100;
    let sink_port = 32101;
    let _ = common::api_provision_stream(&mut client, src_port, sink_port).await;

    let mut src_socket = common::connect_tcp(src_port).await.expect("Bound to source port");
    let mut sink_socket = common::connect_tcp(sink_port).await.expect("Connected to sink port");

    let data = "Hello".as_bytes();
    src_socket.write_all(&data).await.expect("Failed to write to stream");
    src_socket.flush().await.expect("Failed to flush stream");

    let mut buf = [0u8; 5];
    sink_socket.read_exact(&mut buf).await.expect("Failed to read from stream");

    assert_eq!(buf, data);
}

#[tokio::test]
async fn test_stream_udp_data_flow_integration() {
    common::start_server().await;
    let mut client = common::connect_grpc_client().await;

    let src_port = 32100;
    let sink_port = 32101;
    let _ = common::api_provision_stream(&mut client, src_port, sink_port).await;

    let mut src_socket = common::bind_udp(0).await.expect("UDP socket");
    let mut sink_socket = common::bind_udp(sink_port).await.expect("UDP socket");

    let addr = format!("0.0.0.0:{}", src_port);
    let data = "Hello".as_bytes();
    src_socket.send_to(&data, addr).await.expect("Failed to write to stream");

    let mut buf = [0u8; 5];
    let (len, _) = sink_socket.recv_from(&mut buf).await.expect("Failed to read from stream");

    assert_eq!(len, 5);
    assert_eq!(buf, data);
}