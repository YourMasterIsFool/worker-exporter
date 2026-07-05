use std::sync::Arc;

use crate::{
    application::{
        dataset::{DatasetRegistry, ImporterDatasetRegistry, ImporterDatasetRegistryV1},
        grpcs::gprc::GrpcService,
        import::import_registry::ImportLoaderRegistry,
        loader_registry::LoaderRegistry,
        orchestrator::{
            export_orchestrator::ExportOrchestrator, import_orchestrator::ImportOrchestrator,
        },
        pipelines::{
            pipeline_import_registry::PipelineImportRegistry, pipeline_registry::PipelineRegistry,
        },
        writter::WritterRegistry,
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
    loader: Arc<LoaderRegistry>,
    logging: Arc<dyn LoggingRepo>,
    dataset: Arc<DatasetRegistry>,
    grpc_service: Arc<GrpcService>,
    imporer_dataset: Arc<ImporterDatasetRegistry>,
    import_pipeline: Arc<PipelineImportRegistry>,
    publisher: Arc<dyn Publisher>,
    import_registry: Arc<ImportLoaderRegistry>,
    import_datast_registry_v1: Arc<ImporterDatasetRegistryV1>,
) -> OrchestratorRegistry {
    let mut registry = OrchestratorRegistry::new();

    registry.register(
        "import",
        Arc::new(ImportOrchestrator {
            grpc_service: grpc_service.clone(),
            imporer_dataset,
            import_pipeline,
            logging: logging.clone(),
            import_load_registry: import_registry.clone(),
            import_datasetV1: import_datast_registry_v1.clone(),
        }),
    );
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
