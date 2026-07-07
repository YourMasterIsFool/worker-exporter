use std::sync::Arc;

use crate::{
    core::traits::{etl::DataFetcherJson, strategy::registry::Registry},
    infrastructure::{
        db::db_manager::DatabaseInfraManager,
        ingestion::{
            ats::{
                applicant::{
                    requirement_stage_template::IngestionRequirementStageTemplateSource,
                    requirement_stage_template_detail::IngestionRequirementStageTemplateDetailSource,
                },
                candidate::{
                    candidate::AtsCandidateSource,
                    candidate_client_field::AtsCandidateClientFieldSource,
                    education::AtsEducationSource, experient::AtsExperinceSource,
                },
                form::{
                    field::IngestionFormFieldSource, template::IngestionFormTemplateSource,
                    template_detail::IngestionFormTemplateDetailSource,
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
        source::warehouse::temporary::ats::{
            ats_vacancy_custom_field_temporary::AtsVacancyCustomFieldTemporarySource,
            ats_vacancy_pic_temporary::AtsVacancyPicTemporarySource,
            ats_vacancy_requirement_temporary::AtsVacancyRequirementTemporarySource,
            ats_vacancy_stage_temporary::AtsVacancyStageTemporarySource,
            ats_vacancy_temporary::AtsVacancyTemporarySource,
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
        "WAREHOUSE_ATS_VACANCY_STAGE",
        Arc::new(AtsVacancyStageTemporarySource {
            pool: manager.warehouse.clone(),
        }),
    );

    registry.register(
        "WAREHOUSE_ATS_VACANCY_REQUIREMENT",
        Arc::new(AtsVacancyRequirementTemporarySource {
            pool: manager.warehouse.clone(),
        }),
    );

    registry.register(
        "WAREHOUSE_ATS_VACANCY_CUSTOM_FIELD",
        Arc::new(AtsVacancyCustomFieldTemporarySource {
            pool: manager.warehouse.clone(),
        }),
    );

    registry.register(
        "WAREHOUSE_ATS_VACANCY_TEMPORARY",
        Arc::new(AtsVacancyTemporarySource {
            pool: manager.warehouse.clone(),
        }),
    );

    registry.register(
        "WAREHOUSE_ATS_VACANCY_PIC",
        Arc::new(AtsVacancyPicTemporarySource {
            pool: manager.warehouse.clone(),
        }),
    );

    registry.register(
        "STAGE_TEMPLATE",
        Arc::new(IngestionRequirementStageTemplateSource {
            pool: manager.ats.clone(),
        }),
    );

    registry.register(
        "REQUIREMENT_STAGE_TEMPLATE",
        Arc::new(IngestionRequirementStageTemplateSource {
            pool: manager.ats.clone(),
        }),
    );

    registry.register(
        "STAGE_TEMPLATE_DETAIL",
        Arc::new(IngestionRequirementStageTemplateDetailSource {
            pool: manager.ats.clone(),
        }),
    );

    registry.register(
        "REQUIREMENT_STAGE_TEMPLATE_DETAIL",
        Arc::new(IngestionRequirementStageTemplateDetailSource {
            pool: manager.ats.clone(),
        }),
    );

    registry.register(
        "FORM_TEMPLATE",
        Arc::new(IngestionFormTemplateSource {
            pool: manager.ats.clone(),
        }),
    );

    registry.register(
        "REQUIREMENT_TEMPLATE",
        Arc::new(IngestionFormTemplateSource {
            pool: manager.ats.clone(),
        }),
    );

    registry.register(
        "FORM_FIELD",
        Arc::new(IngestionFormFieldSource {
            pool: manager.ats.clone(),
        }),
    );

    registry.register(
        "FORM_TEMPLATE_DETAIL",
        Arc::new(IngestionFormTemplateDetailSource {
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
