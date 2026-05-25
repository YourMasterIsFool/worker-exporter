use std::sync::Arc;

use crate::{
    application::{
        dataset::DatasetRegistry, load::LoadRegistry,
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
) -> OrchestratorRegistry {
    let mut registry = OrchestratorRegistry::new();
    registry.register(
        "export",
        Arc::new(ExportOrchestrator {
            pipeline: pipeline,
            writter: writter,
            load: loader,
            logging: logging,
            dataset,
        }),
    );
    registry
}
