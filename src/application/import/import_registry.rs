use std::sync::Arc;

use crate::{
    core::traits::{load::ImportLoadPersitence, strategy::registry::Registry},
    infrastructure::{
        db::db_manager::DatabaseInfraManager,
        loader::warehouse::temporary::ats::warehouse_ats_vacancy_temporary::WarehouseAtsVacancyTemporaryLoader,
    },
};

pub type ImportLoaderRegistry = Registry<dyn ImportLoadPersitence + Send + Sync>;

pub fn build_import_loader_registry(manager: Arc<DatabaseInfraManager>) -> ImportLoaderRegistry {
    let mut registry = ImportLoaderRegistry::new();

    registry.register(
        "warehouse_ats_vacancy_temporary",
        Arc::new(WarehouseAtsVacancyTemporaryLoader {
            client: manager.warehouse.clone(),
        }),
    );

    registry
}
