use std::sync::Arc;

use crate::application::fetch_source_regitry::DataFetcherJsonRegistry;
use crate::application::grpcs::gprc::GrpcService;
use crate::application::ingestion::source_registry::SourceRegistry;
use crate::core::traits::dataset_trait::{
    Dataset, ImporterDataset, ImporterDatasetV1, SyncDataset,
};
use crate::core::traits::strategy::registry::Registry;
use crate::infrastructure::datasets::export::ats::lsi_candidate_dataset::LsiCandidateDataset;
use crate::infrastructure::datasets::import::ats::ats_import_lsi_vacancy_dataset::AtsImportLsiVacancyDataset;
use crate::infrastructure::datasets::import::ats::lsi_import_vacancy_v1::LsiImportVacancyV1;
use crate::infrastructure::datasets::import::dynamci_dataset_v1::DynamicDatasetV1;
use crate::infrastructure::datasets::import::dynamic_dataset::DynamicDataset;
use crate::infrastructure::datasets::import::lsi_vacancy_sync_dataset::LsiVacancySyncDataset;
use crate::infrastructure::db::ats_client::AtsClient;
use crate::infrastructure::db::warehouse_client::WarehouseClient;

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

pub type ImporterDatasetRegistry = Registry<dyn ImporterDataset + Send + Sync>;

pub fn build_importer_dataset_registry(
    fetcher_registry: Arc<DataFetcherJsonRegistry>,
) -> ImporterDatasetRegistry {
    let mut registry = ImporterDatasetRegistry::new();

    registry
        .register(
            "dynamic_dataset",
            Arc::new(DynamicDataset {
                fetcher: fetcher_registry.clone(),
            }),
        )
        .unwrap();
    registry
        .register(
            "ats_import_lsi_vacancy_dataset",
            Arc::new(AtsImportLsiVacancyDataset {}),
        )
        .expect("error registrer ats import lsi vacancy dataset");

    registry
}

pub type ImporterDatasetRegistryV1 = Registry<dyn ImporterDatasetV1 + Send + Sync>;

pub fn build_importer_dataset_registry_v1(
    fetcher_registry: Arc<DataFetcherJsonRegistry>,
) -> ImporterDatasetRegistryV1 {
    let mut registry = ImporterDatasetRegistryV1::new();

    registry
        .register(
            "DYNAMIC_DATASET_V1",
            Arc::new(DynamicDatasetV1::new(fetcher_registry.clone())),
        )
        .expect("error registrer ats import lsi vacancy dataset");
    registry
        .register(
            "LSI_IMPORT_VACANCY_V1",
            Arc::new(LsiImportVacancyV1::new(fetcher_registry.clone())),
        )
        .expect("error register lsi import vacancy v1 dataset");
    registry
}

pub type SyncDatasetRegistry = Registry<dyn SyncDataset + Send + Sync>;

pub fn build_sync_dataset_registry(
    fetcher_registry: Arc<DataFetcherJsonRegistry>,
    source_registry: Arc<SourceRegistry>,
) -> SyncDatasetRegistry {
    let mut registry = SyncDatasetRegistry::new();

    registry
        .register(
            "LSI_VACANCY_SYNC_DATASET",
            Arc::new(LsiVacancySyncDataset {
                source: source_registry.clone(),
                fetcher: fetcher_registry.clone(),
            }),
        )
        .expect("error register lsi vacancy sync dataset");

    registry
}
