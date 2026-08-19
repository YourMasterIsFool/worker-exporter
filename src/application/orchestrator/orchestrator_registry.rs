use std::{sync::Arc, time::Duration};

use moka::future::Cache;

use crate::{
    application::{
        aggregate_pipeline_registry::AggregatePipelineRegistry,
        dataset::{DatasetRegistry, ImporterDatasetRegistry, ImporterDatasetRegistryV1},
        fetch_source_regitry::DataFetcherJsonRegistry,
        grpcs::gprc::GrpcService,
        loader_registry::LoaderRegistry,
        orchestrator::{
            aggregate_orchestrator::AggregateOrchestrator,
            dynamic_export_orchestrator::DynamicExportOrchestrator,
            dynamic_import_orchestrator::DynamicImportOrchestrator,
            export_orchestrator::ExportOrchestrator, import_orchestrator::ImportOrchestrator,
            ingestion_orchestrator::IngestionOrchestrator,
            rollback_orchestrator::RollbackOrchestrator, sync_orchestrator::SyncOrchestrator,
        },
        pipelines::{
            pipeline_import_registry::PipelineImportRegistry, pipeline_registry::PipelineRegistry,
            rollback_pipeline_registry::RollbackPipelineRegistry,
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
    import_dataset_registry_v1: Arc<ImporterDatasetRegistryV1>,
    fetcher_registry: Arc<DataFetcherJsonRegistry>,
    sync_aggregator: Arc<SyncAggregatorRegistry>,
    rollback_pipeline: Arc<RollbackPipelineRegistry>,
    aggregate_pipelines: Arc<AggregatePipelineRegistry>,
) -> OrchestratorRegistry {
    let mut registry = OrchestratorRegistry::new();
    // for caching data
    let moka_config = Cache::builder()
        .time_to_live(Duration::from_mins(30))
        .max_capacity(5000)
        .build();
    let import_orchestrator = Arc::new(ImportOrchestrator {
        grpc_service: grpc_service.clone(),
        imporer_dataset,
        import_pipeline,
        logging: logging.clone(),
        import_datasetV1: import_dataset_registry_v1.clone(),
        local_cache: moka_config,
        fetcher: fetcher_registry.clone(),
        publisher: publisher.clone(),
    });
    registry
        .register("IMPORT", import_orchestrator.clone())
        .ok();
    registry
        .register(
            "DYNAMIC_IMPORT",
            Arc::new(DynamicImportOrchestrator {
                import: import_orchestrator,
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
        publisher: publisher.clone(),
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
                logging: logging.clone(),
            }),
        )
        .ok();
    registry
        .register(
            "ROLLBACK",
            Arc::new(RollbackOrchestrator {
                pipeline: rollback_pipeline,
                logging: logging.clone(),
            }),
        )
        .ok();
    registry
        .register(
            "AGGREGATE",
            Arc::new(AggregateOrchestrator {
                pipelines: aggregate_pipelines,
                logging: logging.clone(),
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
