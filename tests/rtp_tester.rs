use std::net::SocketAddr;
use std::sync::Arc;

use rtp::packet::Packet;
use rtp::header::Header;
use webrtc_util::marshal::Marshal;

pub struct RtpTx {
    socket: Arc<tokio::net::UdpSocket>,
}
pub struct RtpRx {
    socket: Arc<tokio::net::UdpSocket>,
}

impl RtpTx {
    pub async fn bind() -> std::io::Result<Self> {
        let socket = tokio::net::UdpSocket::bind("0.0.0.0:0").await?;
        Ok(Self { 
            socket: Arc::new(socket)
        })
    }

    pub async fn send_packets_at_rate(&self, target_addr: SocketAddr, interval_millis: u64, num_packets: u64) -> () {    
        let socket = std::sync::Arc::clone(&self.socket);
        
        tokio::spawn(async move {        
            let mut ticker = tokio::time::interval(tokio::time::Duration::from_millis(interval_millis));
            
            for n in 1..=num_packets {
                ticker.tick().await; // Await the next exact tick
                 let rtp_packet = Packet {
                    header: Header {
                        version: 2,
                        sequence_number: n as u16,
                        ..Default::default()
                    },
                    payload: bytes::Bytes::from(vec![0u8; 160]), // 160 bytes of payload
                };

                let data = rtp_packet
                    .marshal()
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
                    .expect("RTP packet marshalled");

                let _ = socket.send_to(&data, target_addr).await;
            }
        }).await.expect("Packets sent")
    }
}

impl RtpRx {
    pub async fn bind(port: u16) -> std::io::Result<Self> {
        let socket = tokio::net::UdpSocket::bind(format!("0.0.0.0:{}", port)).await?;
        Ok(Self { 
            socket: Arc::new(socket)
        })
    }

    pub async fn receive_packets(&self, num_packets: u64) -> (u64, usize) {
        let socket = std::sync::Arc::clone(&self.socket);

        tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            let mut received_bytes: usize = 0;
            let mut received_packets: u64 = 0;

            while received_packets < num_packets {
                match tokio::time::timeout(tokio::time::Duration::from_millis(500), socket.recv_from(&mut buf)).await {
                    Ok(Ok((n, _))) => {
                        received_bytes += n;
                        received_packets += 1;
                    }
                    _ => break, // Timeout means the pipeline drained or dropped packets
                }
            }
            (received_packets, received_bytes)
        }).await.expect("Packets received")
    }
}