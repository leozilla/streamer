use std::io;

use tokio::net::{TcpListener, TcpSocket, TcpStream};

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

pub async fn api_provision_stream(client: &mut StreamerClient<tonic::transport::Channel>, source: u32, sink: u32) {
    let request = tonic::Request::new(ProvisionStreamRequest {
        source_port: source,
        sink_port: sink,
        description: "My awesome stream".into(),
    });
    let response = client.provision_stream(request).await.unwrap();

    assert_eq!(response.into_inner().stream.is_some(), true);
}

pub async fn bind_and_accept(port: u32) -> io::Result<TcpStream> {
    let listener  = TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    
    let (mut stream, _) = listener.accept().await?;
    Ok(stream)
}

pub async fn connect(port: u32) -> io::Result<TcpStream> {
    let addr = format!("0.0.0.0:{}", port).parse().unwrap();
    
    let socket = TcpSocket::new_v4()?;
    let stream  = socket.connect(addr).await?;
    
    Ok(stream)
}