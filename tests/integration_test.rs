mod common;
mod rtp_tester;

use tokio::{io::{AsyncReadExt, AsyncWriteExt}};
use rtp::packet::Packet;
use rtp::header::Header;
use webrtc_util::marshal::Marshal;

use rtp_tester::RtpTx;
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

// cargo test test_list_streams_integration
#[tokio::test]
async fn test_list_streams_integration() {
    common::start_server().await;
    let mut client = common::connect_grpc_client().await;

    let list_reply = common::list_provisioned_streams(&mut client).await;
    assert_eq!(list_reply.streams.len(), 0);

    let provision1_reply = common::api_provision_stream(&mut client, 32000, 32001).await;
    let list_reply = common::list_provisioned_streams(&mut client).await;
    assert_eq!(list_reply.streams.len(), 1);

    let provision2_reply = common::api_provision_stream(&mut client, 32000, 32002).await;
    let list_reply = common::list_provisioned_streams(&mut client).await;
    assert_eq!(list_reply.streams.len(), 2);

    common::api_deprovision_stream(&mut client, provision1_reply.stream.unwrap().id.as_str()).await;
    let list_reply = common::list_provisioned_streams(&mut client).await;
    assert_eq!(list_reply.streams.len(), 1);

    common::api_deprovision_stream(&mut client, provision2_reply.stream.unwrap().id.as_str()).await;
    let list_reply = common::list_provisioned_streams(&mut client).await;
    assert_eq!(list_reply.streams.len(), 0);
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

    let rx1 = async {
        let mut buf = [0u8; 1024];
        let (n, _) = rx_socket1.recv_from(&mut buf).await.expect("Failed to read from UDP socket 1");
        assert_eq!(&buf[..n], data.as_ref());
    };

    let rx2 = async {
        let mut buf = [0u8; 1024];
        let (n, _) = rx_socket2.recv_from(&mut buf).await.expect("Failed to read from UDP socket 2");
        assert_eq!(&buf[..n], data.as_ref());
    };

    let tx = async {
        // Give receivers a moment to start listening
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        tx_socket.send_to(&data, addr).await.expect("Failed to write to stream");
    };

    tokio::join!(rx1, rx2, tx);
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

    // Calculate pacing for a specific bitrate    
    let interval_millis = 20 as u64;
    let packets_per_sec = tokio::time::Duration::from_secs(1).as_millis() as f64 / interval_millis as f64;    
    let num_packets = (packets_per_sec * 15.0) as u64; // Test for exactly 15 seconds
    
    let start_time = std::time::Instant::now();

    let target_addr = format!("127.0.0.1:{}", src_port).parse().expect("Invalid target address");
    let rtp_tx = rtp_tester::RtpTx::bind().await.expect("RTP Tx bound to port");
    let rtp_rx = rtp_tester::RtpRx::bind(sink_port).await.expect("RTP Rx bound to port");

    let rx_task = async { 
        let (received_packets, received_bytes) = rtp_rx.receive_packets(num_packets).await;
        (received_packets, received_bytes)
    };
    let tx_task = async { 
        rtp_tx.send_packets_at_rate(target_addr, interval_millis, num_packets).await;
    };

    let ((received_packets, received_bytes), _) = tokio::join!(rx_task, tx_task);

    let elapsed = start_time.elapsed();
    let mbps = (received_bytes as f64 / 1_024_000.0) / elapsed.as_secs_f64();
    
    println!("--- Throughput Test Results ---");
    println!("Packets/sec: {}", packets_per_sec);
    println!("Received bytes: {}", received_bytes);
    println!("Received: {}/{} packets = {}%", received_packets, num_packets, received_packets as f64 / num_packets as f64 * 100.0);
    println!("Elapsed time: {:.2?}", elapsed);
    println!("Throughput: {:.2} MB/s", mbps);
    println!("-------------------------------");

    assert!(received_packets > 0, "Did not receive any packets");
}

#[tokio::test]
async fn test_stream_deprovisioning_closes_sockets() {
    common::start_server().await;
    let mut client = common::connect_grpc_client().await;

    let src_port: u16 = 34000;
    let sink_port: u16 = 34001;
    
    let provision_reply = common::api_provision_stream(&mut client, src_port, sink_port).await;
    let stream_id = provision_reply.stream.unwrap().id;

    // The DataPlane should now be bound to the source port. 
    // Trying to bind to it locally should yield an Address in Use error.
    let bind_attempt = std::net::UdpSocket::bind(format!("0.0.0.0:{}", src_port));
    assert!(bind_attempt.is_err(), "Expected port to be in use by Streamer, but bind succeeded");

    common::api_deprovision_stream(&mut client, stream_id.as_str()).await;

    // Give the DataPlane a brief moment to abort the task and release the OS port
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // We should now be able to successfully bind to the source port because the Streamer released it
    let bind_attempt = std::net::UdpSocket::bind(format!("0.0.0.0:{}", src_port));
    assert!(bind_attempt.is_ok(), "Expected port to be released, but bind failed");
}