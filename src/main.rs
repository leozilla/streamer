use std::sync::Arc;

use tracing::info;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

mod config;
mod metrics;
use config::{Config, InMemoryConfigStore};
use control_plane::ControlPlane;
use data_plane::DataPlane;
use crate::metrics::MetricsExporter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();
    info!("Starting streamer service");
 
    let config: Config = Config::parse().expect("Config parsed");
    config.log();
    
    let (ctrl_event_tx, _) = tokio::sync::broadcast::channel(100);
    let (data_event_tx, _) = tokio::sync::broadcast::channel(100);

    let config_store = Arc::new(InMemoryConfigStore::new(&config));
    let data_plane = Arc::new(DataPlane::new(data_event_tx));
    let control_plane = Arc::new(ControlPlane::new(
        Arc::clone(&config_store), 
        Arc::clone(&data_plane),
        ctrl_event_tx));
 
    MetricsExporter::new().start();
    data_plane.start().expect("Data plane started");
    control_plane.start(config.server.grpc_bind_addr, config.server.ws_bind_addr).await?;
     
    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| {
            // If no RUST_LOG env var is set, use this default:
            // "streamer=trace" -> allows TRACE in your code
            // "info" -> limits everything else to INFO
            EnvFilter::new("info")
        });

    // 2. Build the subscriber
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(filter)
        .init();
}