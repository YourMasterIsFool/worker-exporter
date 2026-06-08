use std::sync::Arc;

use crate::{
    core::traits::{load::IncrementalLoad, strategy::registry::Registry},
    infrastructure::warehouse::{
        snapshots::organization::tenants::upsert::TenantWarehouseSnapshot,
        warehouse_client::WarehouseClient,
    },
};

pub type LoadRegistry = Registry<dyn IncrementalLoad + Send + Sync>;

pub fn build_load_registry(client: Arc<WarehouseClient>) -> LoadRegistry {
    let mut registry = LoadRegistry::new();

    registry.register("tenants", Arc::new(TenantWarehouseSnapshot { client }));

    registry
}
