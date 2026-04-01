use control_plane;
// use data_plane;

use control_plane::{config::Config, ControlPlane};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting streamer service");
 
    let config: Config = Config::parse()?;
    config.print();
 
    let control_plane = ControlPlane::new(&config);
    control_plane.start().await?;
 
    Ok(())
}