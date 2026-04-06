mod common;

use tokio::{io::{AsyncReadExt, AsyncWriteExt}};
use rtp::packet::Packet;
use rtp::header::Header;
use webrtc_util::marshal::Marshal;

use control_plane::grpc_api::*;

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

    let src_port: u16 = 32100;
    let sink_port1: u16 = 32101;
    let sink_port2: u16 = 32102;
    let _ = common::api_provision_stream(&mut client, src_port, sink_port1).await;
    let _ = common::api_provision_stream(&mut client, src_port, sink_port2).await;

    let tx_socket = common::bind_udp(0).await.expect("UDP tx socket");
    let rx_socket1 = common::bind_udp(sink_port1).await.expect("UDP rx socket 1");
    let rx_socket2 = common::bind_udp(sink_port2).await.expect("UDP rx socket 2");

    let addr = format!("0.0.0.0:{}", src_port);
    
    let rtp_packet = Packet {
        header: Header {
            version: 2,
            sequence_number: 1,
            ..Default::default()
        },
        payload: bytes::Bytes::from_static(b"Hello"),
    };
    let data = rtp_packet.marshal().expect("Failed to marshal RTP packet");
    tx_socket.send_to(&data, addr).await.expect("Failed to write to stream");

    let mut buf = [0u8; 1024];
    
    let (n, _) = rx_socket1.recv_from(&mut buf).await.expect("Failed to read from UDP socket 1");
    assert_eq!(&buf[..n], data.as_ref());
    
    let (n, _) = rx_socket2.recv_from(&mut buf).await.expect("Failed to read from UDP socket 1");
    assert_eq!(&buf[..n], data.as_ref());
}

// cargo test test_pipeline_throughput --release -- --ignored --nocapture
#[tokio::test]
#[ignore]
async fn test_pipeline_throughput() {
    common::start_server().await;
    let mut client = common::connect_grpc_client().await;

    let src_port: u16 = 33000;
    let sink_port: u16 = 33001;
    let _ = common::api_provision_stream(&mut client, src_port, sink_port).await;

    // Allow time for sockets to bind
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Sender socket blasting data to the streamer
    let tx_socket = std::sync::Arc::new(common::bind_udp(0).await.expect("UDP tx socket"));
    let target_addr = format!("127.0.0.1:{}", src_port);
    
    // Receiver socket listening on the sink port
    let rx_socket = common::bind_udp(sink_port).await.expect("UDP rx socket");

    let rtp_packet = Packet {
        header: Header {
            version: 2,
            sequence_number: 1,
            ..Default::default()
        },
        payload: bytes::Bytes::from(vec![0u8; 1000]),
    };
    let payload = rtp_packet.marshal().expect("Failed to marshal RTP packet");
    let num_packets = 100_000; // ~100 MB total

    let start_time = std::time::Instant::now();

    // 1. Spawn a task to rapidly pump data into the streamer
    let sender = std::sync::Arc::clone(&tx_socket);
    tokio::spawn(async move {
        for _ in 0..num_packets {
            let _ = sender.send_to(&payload, &target_addr).await;
        }
    });

    // 2. Receive the data to measure end-to-end throughput
    let mut buf = [0u8; 2048];
    let mut received_bytes = 0;
    let mut received_packets = 0;

    while received_packets < num_packets {
        match tokio::time::timeout(tokio::time::Duration::from_millis(500), rx_socket.recv_from(&mut buf)).await {
            Ok(Ok((n, _))) => {
                received_bytes += n;
                received_packets += 1;
            }
            _ => break, // Timeout means the pipeline drained or dropped packets
        }
    }

    let elapsed = start_time.elapsed();
    let mbps = (received_bytes as f64 / 1_024_000.0) / elapsed.as_secs_f64();
    
    println!("--- Throughput Test Results ---");
    println!("Received: {}/{} packets", received_packets, num_packets);
    println!("Elapsed time: {:.2?}", elapsed);
    println!("Throughput: {:.2} MB/s", mbps);
    println!("-------------------------------");

    assert!(received_packets > 0, "Did not receive any packets");
}