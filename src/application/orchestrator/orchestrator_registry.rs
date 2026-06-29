use std::sync::Arc;

use crate::{
    application::{
        dataset::DatasetRegistry, grpcs::gprc::GrpcService, load::LoadRegistry,
        orchestrator::export_orchestrator::ExportOrchestrator,
        pipelines::pipeline_registry::PipelineRegistry, writter::WritterRegistry,
    },
    core::traits::{
        orchestrator::Orchestrator, publisher::Publisher, strategy::registry::Registry,
    },
    domain::logging::repo::LoggingRepo,
};

pub type OrchestratorRegistry = Registry<dyn Orchestrator>;
pub fn build_orchestrator_registry(
    writter: Arc<WritterRegistry>,
    pipeline: Arc<PipelineRegistry>,
    loader: Arc<LoadRegistry>,
    logging: Arc<dyn LoggingRepo>,
    dataset: Arc<DatasetRegistry>,
    grpc_service: Arc<GrpcService>,
    publisher: Arc<dyn Publisher>,
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
            grpc_service,
            publisher,
        }),
    );
    registry
}
