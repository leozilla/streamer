use tonic::transport::Server;

use control_plane::api::streamer_server::StreamerServer;
use control_plane::StreamerImpl;

mod control_plane;
mod config; 

use config::Config;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config: Config = Config::parse()?;
    config.print();

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