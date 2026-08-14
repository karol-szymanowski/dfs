fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protos = &[
        "proto/common.proto",
        "proto/master_chunkserver.proto",
        "proto/client_master.proto",
        "proto/chunk_data.proto",
        "proto/p2p_clone.proto",
    ];

    println!("cargo:rerun-if-changed=proto");

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(protos, &["proto"])?;

    Ok(())
}
