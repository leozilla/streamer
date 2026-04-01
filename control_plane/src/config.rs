use serde::{Deserialize, Deserializer};
use std::{fs, fmt};
use std::net::{IpAddr, Ipv6Addr, SocketAddr};

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
    Ok(SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), port))
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