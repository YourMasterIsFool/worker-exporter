fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Fungsi ini otomatis ada di semua versi tonic-build
    tonic_build::compile_protos("src/infrastructure/proto/facilitator/s3.proto")?;
    tonic_build::compile_protos("src/infrastructure/proto/ats/decryptor.proto")?;
    Ok(())
}
