use std::sync::Arc;

use anyhow::{Ok, Result};

use crate::{
    core::traits::load::{IncrementalLoad, LoadEnum, UpsertLoader},
    infrastructure::warehouse::{
        snapshots::tenants::model::TenantSnapshotModel, warehouse_client::WarehouseClient,
    },
    utils::build_query,
};

pub struct TenantWarehouseSnapshot {
    pub client: Arc<WarehouseClient>,
}

impl UpsertLoader for TenantWarehouseSnapshot {
    fn table(&self) -> &'static str {
        "warehouse.tenants"
    }

    fn columns(&self) -> &'static [&'static str] {
        &["tenant_name", "tenant_code", "tenant_id"]
    }

    fn conflicted_columns(&self) -> &'static [&'static str] {
        &["tenant_id"]
    }
}
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

        let query = build_query(self, LoadEnum::Upsert);

        let mut tx = self.client.pool.begin().await?;

        for snapshot in &snapshots {
            sqlx::query(&query)
                .bind(&snapshot.tenant_name)
                .bind(&snapshot.tenant_name)
                .bind(&snapshot.tenant_code)
                .execute(&mut *tx)
                .await?;
        }

        tx.commit().await?;

        Ok(snapshots.len() as i64)
    }
}
