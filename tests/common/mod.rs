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

pub async fn provision_stream(client: &mut StreamerClient<tonic::transport::Channel>, source: u32, sinks: Vec<u32>) {
    let request = tonic::Request::new(ProvisionStreamRequest {
        source_port: source,
        sink_ports: sinks,
        description: "My awesome stream".into(),
    });
    let response = client.provision_stream(request).await.unwrap();

    assert_eq!(response.into_inner().stream.is_some(), true);
}