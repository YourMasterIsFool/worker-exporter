use std::sync::Arc;

use crate::core::traits::{load::IncrementalLoadPersitence, strategy::registry::Registry};
use crate::infrastructure::db::warehouse_client::WarehouseClient;
use crate::infrastructure::loader::warehouse::snapshots::organization::tenants::upsert::TenantWarehouseSnapshot;

pub type LoadRegistry = Registry<dyn IncrementalLoadPersitence + Send + Sync>;

pub fn build_load_registry(client: Arc<WarehouseClient>) -> LoadRegistry {
    let mut registry = LoadRegistry::new();

    registry.register("tenants", Arc::new(TenantWarehouseSnapshot { client }));

    registry
}
