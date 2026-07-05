use std::sync::Arc;

use crate::{
    application::grpcs::{ats::AtsDecryptorGrpc, facilitator::S3FacilitatorGrpc},
    core::config::config::Config,
};

pub struct GrpcService {
    pub s3_facilitator_grpc: Arc<S3FacilitatorGrpc>,
    pub ats_decryptor_grpc: Arc<AtsDecryptorGrpc>,
}

impl GrpcService {
    pub fn new(config: &Config) -> Self {
        Self {
            s3_facilitator_grpc: Arc::new(S3FacilitatorGrpc::new(&config.facilitator_grpc_url)),
            ats_decryptor_grpc: Arc::new(AtsDecryptorGrpc::new(&config.ats_grpc_url)),
        }
    }
}
