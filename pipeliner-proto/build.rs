fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &[
                "proto/pipeliner/v1/common.proto",
                "proto/pipeliner/v1/records.proto",
                "proto/pipeliner/v1/plugin.proto",
            ],
            &["proto"],
        )?;
    Ok(())
}
