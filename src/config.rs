use serde::{Deserialize, Deserializer};
use std::{fs, fmt};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use control_plane::config_store::{ConfigStore, ConfigStoreError};
use std::sync::RwLock;

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    pub total_supported_streams: u32,
    pub source_port_range: PortRangeConfig,
    pub sink_port_range: PortRangeConfig,
    pub server: ServerConfig,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PortRangeConfig {
    pub from: u32,
    pub to: u32,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerConfig {
    #[serde(rename = "port", deserialize_with = "deserialize_port_to_socket_addr")]
    pub bind_address: SocketAddr,
}

fn deserialize_port_to_socket_addr<'de, D>(deserializer: D) -> Result<SocketAddr, D::Error>
where
    D: Deserializer<'de>,
{
    let port: u16 = Deserialize::deserialize(deserializer)?;
    Ok(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port))
}

const CONFIG_FILE_NAME: &str = "config.yml";

impl Config {
    pub fn parse() -> Result<Config, Box<dyn std::error::Error>> {
        let contents = fs::read_to_string(CONFIG_FILE_NAME)
            .expect(&format!("Config file not found: {}", CONFIG_FILE_NAME));

        let config: Config = serde_yaml::from_str(&contents)
            .expect(&format!("Failed to parse YAML config file: {}", CONFIG_FILE_NAME));

        Ok(config)
    }

    pub fn print(&self) {
        println!("Total supported streams: {}", self.total_supported_streams);
        println!("Source port range: {}", self.source_port_range);
        println!("Sink port range: {}", self.sink_port_range);
        println!("Server address: {}", self.server.bind_address);
    }
}

impl fmt::Display for PortRangeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.from, self.to)
    }
}

pub struct InMemoryConfigStore {
    config: RwLock<Config>
}

impl InMemoryConfigStore {
    pub fn new(config: &Config) -> Self {
        Self {
            config: RwLock::new(config.clone())
        }
    }
}

impl ConfigStore for InMemoryConfigStore {
    fn total_supported_streams(&self) -> u32 {
        self.config.read().unwrap().total_supported_streams
    }

    fn set_new_config(&self, total_supported_streams: u32) -> Result<(), ConfigStoreError> {
        if (total_supported_streams < 1) || (total_supported_streams > 1024) {
            return Err(ConfigStoreError::InvalidArg);
        }
        
        let mut config = self.config.write().map_err(|_| ConfigStoreError::Unknown)?;        
        config.total_supported_streams = total_supported_streams;

        Ok(())
    }
}