pub struct DataPlane {
}

impl DataPlane {
    pub fn new() -> Self {
        Self { }
    }
    
    pub fn start(&self) -> Result<(), Box<dyn std::error::Error>> { 
        println!("Starting data plane.");
  
        Ok(())
    }
}