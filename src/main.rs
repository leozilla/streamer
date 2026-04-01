use std::sync::Arc;

use control_plane::{config::Config, ControlPlane};
use data_plane::DataPlane;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting streamer service");
 
    let config: Config = Config::parse()?;
    config.print();
         
    let data_plane = Arc::new(DataPlane::new());
    let control_plane = ControlPlane::new(&config, Arc::clone(&data_plane));
 
    data_plane.start().expect("Data plane started");
    control_plane.start().await?;
 
    Ok(())
}