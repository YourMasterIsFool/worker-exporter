use std::sync::Arc;

use crate::core::traits::{load::IncrementalLoadPersitence, strategy::registry::Registry};
use crate::infrastructure::db::db_manager::DatabaseInfraManager;
use crate::infrastructure::loader::olap::master::{
    geo_dimension::GeoDimensionLoader, master_item_dimension::MasterItemDimensionLoader,
};
use crate::infrastructure::loader::warehouse::snapshots::organization::tenants::upsert::TenantWarehouseSnapshot;
use crate::infrastructure::loader::{
    olap::ats::{
        entity_dimension::AtsEntityDimensionLoader, vacancy_dimension::AtsVacancyDimensionLoader,
    },
    oltp::ats::entity_mapping::VACANCY_ENTITIES,
};

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
        .register(
            "OLAP_MASTER_ITEM_DIMENSION",
            Arc::new(MasterItemDimensionLoader {
                client: db.warehouse.clone(),
            }),
        )
        .expect("OLAP master-item loader key must be unique");
    registry
        .register(
            "OLAP_GEO_DIMENSION",
            Arc::new(GeoDimensionLoader {
                client: db.warehouse.clone(),
            }),
        )
        .expect("OLAP geo loader key must be unique");
    registry
        .register(
            "OLAP_ATS_VACANCY_DIMENSION",
            Arc::new(AtsVacancyDimensionLoader {
                client: db.warehouse.clone(),
            }),
        )
        .expect("OLAP ATS vacancy loader key must be unique");
    for mapping in VACANCY_ENTITIES
        .iter()
        .copied()
        .filter(|mapping| mapping.entity != "vacancy")
    {
        let key = format!("OLAP_ATS_{}_DIMENSION", mapping.entity.to_ascii_uppercase());
        registry
            .register(
                &key,
                Arc::new(AtsEntityDimensionLoader {
                    client: db.warehouse.clone(),
                    mapping,
                }),
            )
            .expect("OLAP ATS entity loader key must be unique");
    }

    registry
}
