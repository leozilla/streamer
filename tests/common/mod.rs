use control_plane::api::streamer_client::StreamerClient;

pub async fn start_server() {
}

pub async fn connect_grpc_client() -> StreamerClient<tonic::transport::Channel> {
    let mut client = StreamerClient::connect("http://0.0.0.0:50051")
        .await
        .expect("Failed to connect to server");

    client
}