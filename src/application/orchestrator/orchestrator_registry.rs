use std::sync::Arc;

use crate::{
    application::{
        dataset::DatasetRegistry, grpcs::facilitator::S3FacilitatorGrpc, load::LoadRegistry,
        orchestrator::export_orchestrator::ExportOrchestrator,
        pipelines::pipeline_registry::PipelineRegistry, writter::WritterRegistry,
    },
    core::traits::{orchestrator::Orchestrator, strategy::registry::Registry},
    domain::logging::repo::LoggingRepo,
};

pub type OrchestratorRegistry = Registry<dyn Orchestrator>;
pub fn build_orchestrator_registry(
    writter: Arc<WritterRegistry>,
    pipeline: Arc<PipelineRegistry>,
    loader: Arc<LoadRegistry>,
    logging: Arc<dyn LoggingRepo>,
    dataset: Arc<DatasetRegistry>,
    s3_grpc: Arc<S3FacilitatorGrpc>,
) -> OrchestratorRegistry {
    let mut registry = OrchestratorRegistry::new();
    registry.register(
        "export",
        Arc::new(ExportOrchestrator {
            pipeline,
            writter,
            load: loader,
            logging,
            dataset,
            s3_grpc,
        }),
    );
    registry
}
