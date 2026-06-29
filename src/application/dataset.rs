use std::sync::Arc;

use crate::application::fetch_source_regitry::DataFetcherJsonRegistry;
use crate::application::grpcs::gprc::GrpcService;
use crate::application::ingestion::source_registry::SourceRegistry;
use crate::infrastructure::datasets::export::ats::lsi_candidate_dataset::LsiCandidateDataset;
use crate::infrastructure::db::ats_client::AtsClient;
use crate::{
    core::{datasets::dataset_trait::Dataset, traits::strategy::registry::Registry},
    infrastructure::warehouse::warehouse_client::WarehouseClient,
};

pub type DatasetRegistry = Registry<dyn Dataset + Send + Sync>;

pub fn build_dataset_registry(
    _client: Arc<WarehouseClient>,
    ats_db: Arc<AtsClient>,
    data_fetcher_registry: Arc<DataFetcherJsonRegistry>,
    source_registry: Arc<SourceRegistry>,
    grpc_service: Arc<GrpcService>,
) -> DatasetRegistry {
    let mut registry = DatasetRegistry::new();

    registry.register(
        "lsi_ats_candidate_dataset",
        Arc::new(LsiCandidateDataset {
            client: ats_db.clone(),
            data_fetcher: data_fetcher_registry.clone(),
            source: source_registry.clone(),
            grpc_service: grpc_service.clone(),
        }),
    );

    registry
}
