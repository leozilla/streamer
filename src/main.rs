use tonic::transport::Server;
use serde::Deserialize;
use std::{fs, fmt};

use control_plane::api::streamer_server::StreamerServer;
use control_plane::StreamerImpl;

mod control_plane;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    total_supported_streams: u32,
    source_port_range: PortRangeConfig,
    sink_port_range: PortRangeConfig,
    server: ServerConfig,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PortRangeConfig {
    from: u32,
    to: u32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ServerConfig {
    port: u32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let filename = "config.yml";
    let contents = fs::read_to_string(filename)
        .expect(&format!("Config file not found: {}", filename));

    let config: Config = serde_yaml::from_str(&contents)
        .expect(&format!("Failed to parse YAML config file: {}", filename));

    print_config(&config);

    let addr = format!("[::1]:{}", config.server.port)
        .parse()
        .expect("Failed to parse server bind address");
    let streamer = StreamerImpl::new(config);

    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(
            // This is the file we generated in build.rs
            include_bytes!(concat!(env!("OUT_DIR"), "/api_descriptor.bin")),
        )
        .build_v1()?;

    println!("Server listening on {}", addr);

    Server::builder()
        .add_service(reflection_service)
        .add_service(StreamerServer::new(streamer))
        .serve(addr)
        .await?;

    Ok(())
}

impl fmt::Display for PortRangeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.from, self.to)
    }
}

fn print_config(config: &Config) {
    println!("Total supported streams: {}", config.total_supported_streams);
    println!("Source port range: {}", config.source_port_range);
    println!("Sink port range: {}", config.sink_port_range);
}