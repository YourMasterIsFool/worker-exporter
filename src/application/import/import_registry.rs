use std::sync::Arc;

use crate::{
    core::traits::{load::ImportLoadPersitence, strategy::registry::Registry},
    infrastructure::{
        db::db_manager::DatabaseInfraManager,
        loader::warehouse::temporary::{
            ats::warehouse_ats_vacancy_temporary::WarehouseAtsVacancyTemporaryLoader,
            warehouse_dynamic_temporary_loader::WarehouseDynamicTemporaryLoader,
        },
    },
};

pub type ImportLoaderRegistry = Registry<dyn ImportLoadPersitence + Send + Sync>;

pub fn build_import_loader_registry(manager: Arc<DatabaseInfraManager>) -> ImportLoaderRegistry {
    let mut registry = ImportLoaderRegistry::new();

    registry
        .register(
            "warehouse_ats_vacancy_temporary",
            Arc::new(WarehouseAtsVacancyTemporaryLoader {
                client: manager.warehouse.clone(),
            }),
        )
        .ok();

    registry
        .register(
            "WAREHOUSE_DYNAMIC_TEMPORARY_LOADER",
            Arc::new(WarehouseDynamicTemporaryLoader {
                client: manager.warehouse.clone(),
            }),
        )
        .ok();
    registry
}
