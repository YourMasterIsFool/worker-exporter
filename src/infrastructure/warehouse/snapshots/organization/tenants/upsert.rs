use std::sync::Arc;

use anyhow::{Ok, Result};
use tracing::info;

use crate::{
    core::traits::load::{IncrementalLoad, LoadEnum, LoaderWarehouse},
    infrastructure::warehouse::{
        snapshots::organization::tenants::model::TenantSnapshotModel,
        warehouse_client::WarehouseClient,
    },
    utils::build_query,
};

pub struct TenantWarehouseSnapshot {
    pub client: Arc<WarehouseClient>,
}

impl LoaderWarehouse for TenantWarehouseSnapshot {
    fn table(&self) -> &'static str {
        "tenant_snapshots"
    }

    fn columns(&self) -> &'static [&'static str] {
        &["tenant_name", "tenant_code", "tenant_id"]
    }

    fn conflicted_columns(&self) -> &'static [&'static str] {
        &["tenant_id"]
    }
}

// buat loader

#[async_trait::async_trait]
impl IncrementalLoad for TenantWarehouseSnapshot {
    async fn load(&self, rows: &[serde_json::Value]) -> Result<i64> {
        if rows.is_empty() {
            return Ok(0);
        }
        // mapping into snapshotmodel
        let snapshots: Vec<TenantSnapshotModel> = rows
            .iter()
            .map(|row| TenantSnapshotModel::try_from(row))
            .collect::<Result<Vec<_>>>()?;

        let upsert_load = LoadEnum::Upsert;
        let query = build_query(self, &upsert_load);

        info!("snapshot {:?}", snapshots);
        info!("snapshot query {}", query);
        let mut tx = self.client.pool.begin().await?;

        for snapshot in &snapshots {
            sqlx::query(&query)
                .bind(&snapshot.tenant_name)
                .bind(&snapshot.tenant_code)
                .bind(&snapshot.tenant_id)
                .execute(&mut *tx)
                .await?;
        }

        tx.commit().await?;

        Ok(snapshots.len() as i64)
    }
}
