use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConfigStoreError {
    #[error("invalid supported streams (expected to be in range 1-1024)")]
    InvalidArg,
    #[error("unknown config store error")]
    Unknown,
}

pub trait ConfigStore: Send + Sync {
    fn total_supported_streams(&self) -> u32;

    fn set_new_config(&self, total_supported_streams: u32) -> Result<(), ConfigStoreError>;

}
