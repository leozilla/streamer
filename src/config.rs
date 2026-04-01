use serde::Deserialize;
use std::{fs, fmt};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    pub total_supported_streams: u32,
    pub source_port_range: PortRangeConfig,
    pub sink_port_range: PortRangeConfig,
    pub server: ServerConfig,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PortRangeConfig {
    pub from: u32,
    pub to: u32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerConfig {
    pub port: u32,
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
    }
}

impl fmt::Display for PortRangeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.from, self.to)
    }
}