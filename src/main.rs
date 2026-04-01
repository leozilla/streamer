use std::sync::Arc;

mod config;

use config::{Config, InMemoryConfigStore};
use control_plane::ControlPlane;
use data_plane::DataPlane;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting streamer service");
 
    let config: Config = Config::parse()?;
    config.print();
    
    let config_store = Arc::new(InMemoryConfigStore::new(&config));
    let data_plane = Arc::new(DataPlane::new());
    let control_plane = ControlPlane::new(
        Arc::clone(&config_store), 
        Arc::clone(&data_plane));
 
    data_plane.start().expect("Data plane started");
    control_plane.start(config.server.bind_address).await?;
 
    Ok(())
}