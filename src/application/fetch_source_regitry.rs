use std::sync::Arc;

use crate::{
    core::{
        config::database_manager,
        traits::{etl::DataFetcherJson, strategy::registry::Registry},
    },
    infrastructure::{
        db::{
            ats_client::AtsClient, db_manager::DatabaseInfraManager, master_client::MasterClient,
        },
        ingestion::{
            ats::{
                candidate::{
                    candidate::AtsCandidateSource,
                    candidate_client_field::AtsCandidateClientFieldSource,
                    education::AtsEducationSource, experient::AtsExperinceSource,
                },
                vacancy::vacancy::AtsVacancySource,
            },
            hris::attedances::attedance_locations::{
                IngestionHrisAttedanceLocations, IngestionHrisAttedanceLocationsSource,
            },
            master::{geo::MasterGeoSource, master_item::MasterMasterItemsSource},
            organization::organization_postgres_client::OrganizatinPostgresClient,
        },
    },
};
pub type DataFetcherJsonRegistry = Registry<dyn DataFetcherJson + Send + Sync>;

pub struct DataFetcherJsonRegistryStruct {
    pub(crate) organization_db: Arc<OrganizatinPostgresClient>,
    pub(crate) ats_db: Arc<OrganizatinPostgresClient>,
}

pub fn build_data_fetcher_json_registry(
    database_infra_manager: Arc<DatabaseInfraManager>,
) -> DataFetcherJsonRegistry {
    let mut registry = DataFetcherJsonRegistry::new();
    let manager = database_infra_manager.clone();
    registry.register(
        "ats_candidate_custom_field_fetcher",
        Arc::new(AtsCandidateClientFieldSource {
            pool: manager.ats.clone(),
        }),
    );

    registry.register(
        "ats_candidate_fetcher",
        Arc::new(AtsCandidateSource {
            pool: manager.ats.clone(),
        }),
    );
    registry.register(
        "master_geo_fetcher",
        Arc::new(MasterGeoSource {
            pool: manager.master.clone(),
        }),
    );
    registry.register(
        "master_master_items_fetcher",
        Arc::new(MasterMasterItemsSource {
            pool: manager.master.clone(),
        }),
    );

    registry.register(
        "ats_education_fetcher",
        Arc::new(AtsEducationSource {
            pool: manager.ats.clone(),
        }),
    );

    registry.register(
        "ats_vacancy_fetcher",
        Arc::new(AtsVacancySource {
            pool: manager.ats.clone(),
        }),
    );

    registry.register(
        "ats_experience_fetcher",
        Arc::new(AtsExperinceSource {
            pool: manager.ats.clone(),
        }),
    );

    registry.register(
        "ats_candidate_field_fetcher",
        Arc::new(AtsCandidateClientFieldSource {
            pool: manager.ats.clone(),
        }),
    );

    registry.register(
        "hris_attendance_locations_fetcher",
        Arc::new(IngestionHrisAttedanceLocationsSource {
            pool: manager.hris.clone(),
        }),
    );

    registry
}
