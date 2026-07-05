use std::sync::Arc;

use crate::{
    core::traits::{etl::DataFetcherJson, strategy::registry::Registry},
    infrastructure::{
        db::db_manager::DatabaseInfraManager,
        ingestion::{
            ats::{
                candidate::{
                    candidate::AtsCandidateSource,
                    candidate_client_field::AtsCandidateClientFieldSource,
                    education::AtsEducationSource, experient::AtsExperinceSource,
                },
                vacancy::vacancy::AtsVacancySource,
            },
            hris::{
                attedances::attedance_locations::IngestionHrisAttedanceLocationsSource,
                employee::{
                    employee::IngestionEmployeeSource,
                    emplyoee_statuses::IngestionEmployeeStatusesSource,
                },
            },
            master::{geo::MasterGeoSource, master_item::MasterMasterItemsSource},
            organization::{
                core::{
                    ingestion_job_grade::IngestionJobGradeSource,
                    ingestion_job_level::IngestionJobLevelSource,
                    ingestion_job_position::IngestionOrganizationJobPositionSource,
                    ingestion_organization_unit::IngestionOrganizationUnitSource,
                },
                organization_postgres_client::OrganizatinPostgresClient,
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
        "MASTER_GEO",
        Arc::new(MasterGeoSource {
            pool: manager.master.clone(),
        }),
    );
    registry.register(
        "MASTER_ITEMS",
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
        "ATTEDANCE_LOCATION",
        Arc::new(IngestionHrisAttedanceLocationsSource {
            pool: manager.hris.clone(),
        }),
    );

    registry.register(
        "JOB_POSITION",
        Arc::new(IngestionOrganizationJobPositionSource {
            pool: manager.organization.clone(),
        }),
    );

    registry.register(
        "ORGANIZATION",
        Arc::new(IngestionOrganizationUnitSource {
            pool: manager.organization.clone(),
        }),
    );

    registry.register(
        "JOB_GRADE",
        Arc::new(IngestionJobGradeSource {
            pool: manager.organization.clone(),
        }),
    );

    registry.register(
        "JOB_LEVEL",
        Arc::new(IngestionJobLevelSource {
            pool: manager.organization.clone(),
        }),
    );
    registry.register(
        "EMPLOYMENT_STATUS",
        Arc::new(IngestionEmployeeStatusesSource {
            pool: manager.hris.clone(),
        }),
    );

    registry.register(
        "EMPLOYEE",
        Arc::new(IngestionEmployeeSource {
            pool: manager.hris.clone(),
        }),
    );
    registry
}
