pub trait ConfigStore: Send + Sync {
    fn total_supported_streams(&self) -> u32;

    fn set_new_config(&self, total_supported_streams: u32);

}
