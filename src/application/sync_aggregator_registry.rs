use std::sync::Arc;

use crate::{
    application::{
        bulk_loader_registry::BulkLoaderRegistry, dataset::SyncDatasetRegistry,
        ingestion::source_registry::SourceRegistry,
        sync::ats::vacancy::AtsVacancyAtsSyncAggregator,
    },
    core::traits::{load::SyncAggregator, strategy::registry::Registry},
    domain::logging::repo::LoggingRepo,
    infrastructure::{
        db::ats_client::AtsClient,
        loader::warehouse::warehouse_logging_import::WarehouseLoggingImport,
    },
};

pub type SyncAggregatorRegistry = Registry<dyn SyncAggregator + Send + Sync>;

pub fn build_sync_aggregator_registry(
    source_registry: Arc<SourceRegistry>,
    sync_dataset: Arc<SyncDatasetRegistry>,
    bulk_loader: Arc<BulkLoaderRegistry>,
    ats_client: Arc<AtsClient>,
    logging: Arc<dyn LoggingRepo>,
    import_logging: Arc<WarehouseLoggingImport>,
) -> SyncAggregatorRegistry {
    let mut registry = SyncAggregatorRegistry::new();

    registry
        .register(
            "LSI_VACANCY_SYNC",
            Arc::new(AtsVacancyAtsSyncAggregator {
                source: source_registry,
                dataset: sync_dataset,
                bulk_loader,
                ats_client,
                logging,
                import_logging,
            }),
        )
        .expect("error register lsi vacancy sync aggregator");

    registry
}
