use std::sync::Arc;

use crate::{
    core::traits::{load::RollbackLoader, strategy::registry::Registry},
    infrastructure::loader::oltp::ats::delete_by_logging_id::DeleteByLoggingId,
};

pub type RollbackLoaderRegistry = Registry<dyn RollbackLoader + Send + Sync>;

pub fn build_rollback_loader_registry() -> RollbackLoaderRegistry {
    let mut registry = RollbackLoaderRegistry::new();
    for (key, table) in [
        (
            "ATS_VACANCY_REQUIREMENT_ROLLBACK",
            "vacancy.vacancy_requirement",
        ),
        ("ATS_VACANCY_STAGE_ROLLBACK", "vacancy.vacancy_stage"),
        (
            "ATS_VACANCY_PUBLISH_ON_ROLLBACK",
            "vacancy.vacancy_publish_on",
        ),
        ("ATS_VACANCY_PIC_ROLLBACK", "vacancy.vacancy_pic"),
        (
            "ATS_VACANCY_CUSTOM_FIELD_ROLLBACK",
            "vacancy.vacancy_custom_field",
        ),
        ("ATS_VACANCY_ROLLBACK", "vacancy.vacancy"),
    ] {
        registry
            .register(key, Arc::new(DeleteByLoggingId { table }))
            .expect("rollback loader key must be unique");
    }
    registry
}
