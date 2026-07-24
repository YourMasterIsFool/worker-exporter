use std::sync::Arc;

use moka::future::Cache;
use serde_json::Value;
use tokio::sync::RwLock;

use crate::application::datasets::export::ats::applicant_dataset::ApplicantDataset;
use crate::application::datasets::export::ats::lsi_candidate_dataset::LsiCandidateDataset;
use crate::application::datasets::import::ats::ats_import_lsi_vacancy_dataset::AtsImportLsiVacancyDataset;
use crate::application::datasets::import::ats::lsi_import_vacancy_v1::LsiImportVacancyV1;
use crate::application::datasets::import::dynamci_dataset_v1::DynamicDatasetV1;
use crate::application::datasets::import::dynamic_dataset::DynamicDataset;
use crate::application::datasets::import::lsi_vacancy_sync_dataset::LsiVacancySyncDataset;
use crate::application::fetch_source_regitry::DataFetcherJsonRegistry;
use crate::application::grpcs::gprc::GrpcService;
use crate::application::ingestion::source_registry::SourceRegistry;
use crate::application::validations::{
    required_validator::RequiredValidator, unique_validator::UniqueValidator, ValidationRegistry,
};
use crate::core::traits::dataset_trait::{
    Dataset, ImporterDataset, ImporterDatasetV1, SyncDataset,
};
use crate::core::traits::strategy::registry::Registry;
use crate::infrastructure::db::ats_client::AtsClient;
use crate::infrastructure::db::warehouse_client::WarehouseClient;
use crate::infrastructure::loader::warehouse::warehouse_logging_import::WarehouseLoggingImport;

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

    let applicant_dataset = Arc::new(ApplicantDataset {
        source: source_registry,
        fetcher: data_fetcher_registry,
        export_cache: Arc::new(RwLock::new(None::<Cache<String, Value>>)),
        cached_fetchers: Arc::new(RwLock::new(Default::default())),
    });
    registry.register("lsi_applicant_dataset", applicant_dataset.clone());
    registry.register("APPLICANT_DATASET", applicant_dataset);

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
    let mut validation_registry = ValidationRegistry::new();
    validation_registry.register("REQUIRED", RequiredValidator);
    validation_registry.register("UNIQUE", UniqueValidator::new(fetcher_registry.clone()));
    let validation_registry = Arc::new(validation_registry);

    registry
        .register(
            "DYNAMIC_DATASET_V1",
            Arc::new(DynamicDatasetV1::new(fetcher_registry.clone())),
        )
        .expect("error registrer ats import lsi vacancy dataset");
    registry
        .register(
            "LSI_IMPORT_VACANCY_V1",
            Arc::new(LsiImportVacancyV1::new(
                fetcher_registry.clone(),
                validation_registry,
            )),
        )
        .expect("error register lsi import vacancy v1 dataset");
    registry
}

pub type SyncDatasetRegistry = Registry<dyn SyncDataset + Send + Sync>;

pub fn build_sync_dataset_registry(
    fetcher_registry: Arc<DataFetcherJsonRegistry>,
    source_registry: Arc<SourceRegistry>,
    import_logging: Arc<WarehouseLoggingImport>,
) -> SyncDatasetRegistry {
    let mut registry = SyncDatasetRegistry::new();

    registry
        .register(
            "LSI_VACANCY_SYNC_DATASET",
            Arc::new(LsiVacancySyncDataset {
                source: source_registry.clone(),
                fetcher: fetcher_registry.clone(),
                import_logging,
            }),
        )
        .expect("error register lsi vacancy sync dataset");

    registry
}
