use std::sync::Arc;

use crate::{
    core::traits::{
        extractor::source_incremental_extract::SourceIncrementalExtract,
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
            master::master_item::MasterMasterItemsSource,
            organization::{
                organization_postgres_client::OrganizatinPostgresClient,
                sql::tenant::source::TenantSource,
            },
        },
    },
};
pub type SourceRegistry = Registry<dyn SourceIncrementalExtract + Send + Sync>;

pub struct SourceRegistryStruct {
    pub(crate) organization_db: Arc<OrganizatinPostgresClient>,
    pub(crate) ats_db: Arc<OrganizatinPostgresClient>,
}

pub fn build_source_registry(
    organization_db: Arc<OrganizatinPostgresClient>,
    ats_db: Arc<AtsClient>,
    master_db: Arc<MasterClient>,
) -> SourceRegistry {
    let mut registry = SourceRegistry::new();
    registry.register(
        "oltp_tenants",
        Arc::new(TenantSource {
            pool: organization_db.clone(),
        }),
    );
    registry.register(
        "oltp_ats_candidate",
        Arc::new(AtsCandidateSource {
            pool: ats_db.clone(),
        }),
    );

    registry.register(
        "oltp_ats_vacancy",
        Arc::new(AtsVacancySource {
            pool: ats_db.clone(),
        }),
    );

    registry.register(
        "oltp_ats_candidate_client_field",
        Arc::new(AtsCandidateClientFieldSource {
            pool: ats_db.clone(),
        }),
    );

    registry.register(
        "oltp_ats_applicant_bucket",
        Arc::new(AtsApplicantBucketSource {
            pool: ats_db.clone(),
        }),
    );

    registry.register(
        "oltp_ats_experience_source",
        Arc::new(AtsExperinceSource {
            pool: ats_db.clone(),
        }),
    );

    registry.register(
        "oltp_ats_education_source",
        Arc::new(AtsEducationSource {
            pool: ats_db.clone(),
        }),
    );
    // registry.register(
    //     "oltp_master_master_items",
    //     MasterMasterItemsSource {
    //         pool: master_db.clone(),
    //     },
    // );

    registry
}
