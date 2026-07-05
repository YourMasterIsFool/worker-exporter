use std::sync::Arc;

use crate::core::traits::{load::IncrementalLoadPersitence, strategy::registry::Registry};
use crate::infrastructure::db::db_manager::DatabaseInfraManager;
use crate::infrastructure::db::warehouse_client::WarehouseClient;
use crate::infrastructure::loader::warehouse::snapshots::organization::tenants::upsert::TenantWarehouseSnapshot;
use crate::infrastructure::loader::warehouse::temporary::ats::warehouse_ats_vacancy_temporary::WarehouseAtsVacancyTemporaryLoader;

pub type LoaderRegistry = Registry<dyn IncrementalLoadPersitence + Send + Sync>;

pub fn build_loader_registry(manager: Arc<DatabaseInfraManager>) -> LoaderRegistry {
    let mut registry = LoaderRegistry::new();
    let db = manager.clone();

    registry.register(
        "tenants",
        Arc::new(TenantWarehouseSnapshot {
            client: db.warehouse.clone(),
        }),
    );

    registry
}
