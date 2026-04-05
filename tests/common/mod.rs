use std::io;

use tokio::net::{TcpListener, TcpSocket, TcpStream, UdpSocket};
use tokio::time::{timeout, Duration};

use control_plane::api::streamer_client::StreamerClient;
use control_plane::api::*;

pub async fn start_server() {
}

pub async fn connect_grpc_client() -> StreamerClient<tonic::transport::Channel> {
    let mut client = StreamerClient::connect("http://0.0.0.0:50051")
        .await
        .expect("Failed to connect to server");

    client
}

pub async fn api_provision_stream(client: &mut StreamerClient<tonic::transport::Channel>, source: u16, sink: u16) {
    let request = tonic::Request::new(ProvisionStreamRequest {
        source_port: u32::from(source),
        sink_port: u32::from(sink),
        description: "My awesome stream".into(),
    });
    let response = client.provision_stream(request).await.unwrap();

    assert_eq!(response.into_inner().stream.is_some(), true);
}

pub async fn connect_tcp(port: u16) -> io::Result<TcpStream> {
    let addr = format!("0.0.0.0:{}", port).parse().unwrap();
    
    let socket = TcpSocket::new_v4()?;
    let stream  = timeout(Duration::from_secs(5), socket.connect(addr)).await??;
    
    Ok(stream)
}

pub async fn bind_udp(port: u16) -> io::Result<UdpSocket> {
    let addr = format!("0.0.0.0:{}", port);    
    let stream  = timeout(Duration::from_secs(5), UdpSocket::bind(addr)).await??;
    
    Ok(stream)
}