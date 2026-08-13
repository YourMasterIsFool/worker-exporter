use std::{sync::Arc, time::Duration};

use moka::future::Cache;

use crate::{
    application::{
        dataset::{DatasetRegistry, ImporterDatasetRegistry, ImporterDatasetRegistryV1},
        fetch_source_regitry::DataFetcherJsonRegistry,
        grpcs::gprc::GrpcService,
        import::import_registry::ImportLoaderRegistry,
        loader_registry::LoaderRegistry,
        orchestrator::{
            dynamic_export_orchestrator::DynamicExportOrchestrator,
            export_orchestrator::ExportOrchestrator, import_orchestrator::ImportOrchestrator,
            ingestion_orchestrator::IngestionOrchestrator, sync_orchestrator::SyncOrchestrator,
        },
        pipelines::{
            pipeline_import_registry::PipelineImportRegistry, pipeline_registry::PipelineRegistry,
        },
        sync_aggregator_registry::SyncAggregatorRegistry,
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
    import_dataset_registry_v1: Arc<ImporterDatasetRegistryV1>,
    fetcher_registry: Arc<DataFetcherJsonRegistry>,
    sync_aggregator: Arc<SyncAggregatorRegistry>,
) -> OrchestratorRegistry {
    let mut registry = OrchestratorRegistry::new();
    // for caching data
    let moka_config = Cache::builder()
        .time_to_live(Duration::from_mins(30))
        .max_capacity(5000)
        .build();
    registry
        .register(
            "IMPORT",
            Arc::new(ImportOrchestrator {
                grpc_service: grpc_service.clone(),
                imporer_dataset,
                import_pipeline,
                logging: logging.clone(),
                import_load_registry: import_registry.clone(),
                import_datasetV1: import_dataset_registry_v1.clone(),
                local_cache: moka_config,
                fetcher: fetcher_registry.clone(),
                publisher: publisher.clone(),
            }),
        )
        .ok();
    let export_orchestrator = Arc::new(ExportOrchestrator {
        pipeline: pipeline.clone(),
        writter,
        load: loader.clone(),
        logging: logging.clone(),
        dataset,
        grpc_service,
        publisher,
    });
    registry
        .register("EXPORT", export_orchestrator.clone())
        .ok();
    registry
        .register(
            "DYNAMIC_EXPORT",
            Arc::new(DynamicExportOrchestrator {
                export: export_orchestrator,
            }),
        )
        .ok();
    registry
        .register(
            "SYNC",
            Arc::new(SyncOrchestrator {
                sync_aggregator,
                logging,
            }),
        )
        .ok();
    registry
        .register(
            "ingestion",
            Arc::new(IngestionOrchestrator { pipeline, loader }),
        )
        .ok();
    registry
}
