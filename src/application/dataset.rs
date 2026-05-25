use std::sync::Arc;

use crate::{
    core::{datasets::dataset_trait::Dataset, traits::strategy::registry::Registry},
    domain::datasets::organization::tenant_dataset::TenantDataset,
    infrastructure::warehouse::warehouse_client::WarehouseClient,
};

pub type DatasetRegistry = Registry<dyn Dataset + Send + Sync>;

pub fn build_dataset_registry(client: Arc<WarehouseClient>) -> DatasetRegistry {
    let mut registry = DatasetRegistry::new();

    registry.register(
        "tenants",
        Arc::new(TenantDataset {
            client: client.clone(),
        }),
    );

    registry
}
