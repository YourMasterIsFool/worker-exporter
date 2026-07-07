use std::sync::Arc;

use crate::{
    core::traits::{load::BulkLoaderTrait, strategy::registry::Registry},
    infrastructure::{
        db::ats_client::AtsClient,
        loader::oltp::ats::bulk_sync::{
            bulk_custom_field::AtsVacancyCustomFieldBulkLoader, bulk_pic::AtsVacancyPicBulkLoader,
            bulk_requirement::AtsVacancyRequirementBulkLoader,
            bulk_stage::AtsVacancyStageBulkLoader, bulk_vacancy::AtsVacancyBulkLoader,
        },
    },
};

pub type BulkLoaderRegistry = Registry<dyn BulkLoaderTrait + Send + Sync>;

pub fn build_bulk_loader_registry(ats_client: Arc<AtsClient>) -> BulkLoaderRegistry {
    let mut registry = BulkLoaderRegistry::new();

    registry
        .register(
            "ATS_VACANCY",
            Arc::new(AtsVacancyBulkLoader {
                client: ats_client.clone(),
            }),
        )
        .expect("error register ats vacancy bulk loader");
    registry
        .register(
            "ATS_VACANCY_CUSTOM_FIELD",
            Arc::new(AtsVacancyCustomFieldBulkLoader {
                client: ats_client.clone(),
            }),
        )
        .expect("error register ats vacancy custom field bulk loader");
    registry
        .register(
            "ATS_VACANCY_PIC",
            Arc::new(AtsVacancyPicBulkLoader {
                client: ats_client.clone(),
            }),
        )
        .expect("error register ats vacancy pic bulk loader");
    registry
        .register(
            "ATS_VACANCY_REQUIREMENT",
            Arc::new(AtsVacancyRequirementBulkLoader {
                client: ats_client.clone(),
            }),
        )
        .expect("error register ats vacancy requirement bulk loader");
    registry
        .register(
            "ATS_VACANCY_STAGE",
            Arc::new(AtsVacancyStageBulkLoader { client: ats_client }),
        )
        .expect("error register ats vacancy stage bulk loader");

    registry
}
