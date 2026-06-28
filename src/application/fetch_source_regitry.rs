use std::sync::Arc;

use crate::{
    core::traits::{
        etl::DataFetcherJson, extractor::incremental_source::FetchSource,
        strategy::registry::Registry,
    },
    infrastructure::{
        db::{ats_client::AtsClient, master_client::MasterClient},
        ingestion::{
            ats::{
                applicant::applicant_bucket::AtsApplicantBucketSource,
                candidate::{
                    candidate::AtsCandidateSource,
                    candidate_client_field::AtsCandidateClientFieldSource,
                    education::AtsEducationSource, experient::AtsExperinceSource,
                },
                vacancy::vacancy::AtsVacancySource,
            },
            master::{geo::MasterGeoSource, master_item::MasterMasterItemsSource},
            organization::{
                organization_postgres_client::OrganizatinPostgresClient,
                sql::tenant::source::TenantSource,
            },
        },
    },
};
pub type DataFetcherJsonRegistry = Registry<dyn DataFetcherJson + Send + Sync>;

pub struct DataFetcherJsonRegistryStruct {
    pub(crate) organization_db: Arc<OrganizatinPostgresClient>,
    pub(crate) ats_db: Arc<OrganizatinPostgresClient>,
}

pub fn build_data_fetcher_json_registry(
    organization_db: Arc<OrganizatinPostgresClient>,
    ats_db: Arc<AtsClient>,
    master_db: Arc<MasterClient>,
) -> DataFetcherJsonRegistry {
    let mut registry = DataFetcherJsonRegistry::new();
    registry.register(
        "ats_candidate_custom_field_fetcher",
        Arc::new(AtsCandidateClientFieldSource {
            pool: ats_db.clone(),
        }),
    );

    registry.register(
        "ats_candidate_fetcher",
        Arc::new(AtsCandidateSource {
            pool: ats_db.clone(),
        }),
    );
    registry.register(
        "master_geo_fetcher",
        Arc::new(MasterGeoSource {
            pool: master_db.clone(),
        }),
    );
    registry.register(
        "master_master_items_fetcher",
        Arc::new(MasterMasterItemsSource {
            pool: master_db.clone(),
        }),
    );

    registry.register(
        "ats_education_fetcher",
        Arc::new(AtsEducationSource {
            pool: ats_db.clone(),
        }),
    );

    registry.register(
        "ats_vacancy_fetcher",
        Arc::new(AtsVacancySource {
            pool: ats_db.clone(),
        }),
    );

    registry.register(
        "ats_experience_fetcher",
        Arc::new(AtsExperinceSource {
            pool: ats_db.clone(),
        }),
    );

    registry.register(
        "ats_candidate_field_fetcher",
        Arc::new(AtsCandidateClientFieldSource {
            pool: ats_db.clone(),
        }),
    );

    registry
}
