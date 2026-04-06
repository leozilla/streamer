use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/grpc_api.proto");
    println!("cargo:rerun-if-changed=proto/ws_api.proto");

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    
    tonic_build::configure()
        .file_descriptor_set_path(out_dir.join("api_descriptor.bin"))
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .compile_protos(&["proto/grpc_api.proto", "proto/ws_api.proto"], &["proto"])?;

    Ok(())
}