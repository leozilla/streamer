mod common;

use control_plane::api::*;
use control_plane::api::streamer_client::StreamerClient;

#[tokio::test]
async fn test_list_streams_integration() {
    common::start_server().await;

    let mut client = StreamerClient::connect("http://0.0.0.0:50051")
        .await
        .expect("Failed to connect to server");

    let request = tonic::Request::new(GetConfigRequest {
    });

    let response = client.get_config(request).await.unwrap();

    assert_eq!(response.into_inner().total_supported_streams, 100);
}