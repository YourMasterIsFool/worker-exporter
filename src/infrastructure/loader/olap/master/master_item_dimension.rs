use std::sync::Arc;

use anyhow::Result;
use chrono::NaiveDateTime;
use serde::Deserialize;
use uuid::Uuid;

use crate::{
    core::traits::load::IncrementalLoadPersitence,
    infrastructure::db::warehouse_client::WarehouseClient,
};

#[derive(Deserialize)]
struct MasterItemSource {
    id: Uuid,
    tenant_id: Option<Uuid>,
    category_id: Uuid,
    code: String,
    name: String,
    description: Option<String>,
    is_active: bool,
    sort_order: Option<i32>,
    metadata: Option<serde_json::Value>,
    created_at: NaiveDateTime,
    updated_at: NaiveDateTime,
    parent_id: Option<Uuid>,
    multilang: Option<serde_json::Value>,
}

struct MasterItemDimension {
    source_id: Uuid,
    source_tenant_id: Option<Uuid>,
    source_category_id: Uuid,
    source_parent_id: Option<Uuid>,
    code: String,
    name: String,
    description: Option<String>,
    is_active: bool,
    sort_order: Option<i32>,
    metadata: Option<serde_json::Value>,
    multilang: Option<serde_json::Value>,
    source_created_at: NaiveDateTime,
    source_updated_at: NaiveDateTime,
}

impl TryFrom<&serde_json::Value> for MasterItemDimension {
    type Error = serde_json::Error;

    fn try_from(value: &serde_json::Value) -> Result<Self, Self::Error> {
        let source: MasterItemSource = serde_json::from_value(value.clone())?;

        Ok(Self {
            source_id: source.id,
            source_tenant_id: source.tenant_id,
            source_category_id: source.category_id,
            source_parent_id: source.parent_id,
            code: source.code,
            name: source.name,
            description: source.description,
            is_active: source.is_active,
            sort_order: source.sort_order,
            metadata: source.metadata,
            multilang: source.multilang,
            source_created_at: source.created_at,
            source_updated_at: source.updated_at,
        })
    }
}

pub struct MasterItemDimensionLoader {
    pub client: Arc<WarehouseClient>,
}

#[async_trait::async_trait]
impl IncrementalLoadPersitence for MasterItemDimensionLoader {
    async fn fetch_max_source_updated_at(&self) -> Result<Option<NaiveDateTime>> {
        Ok(
            sqlx::query_scalar("SELECT MAX(source_updated_at) FROM olap.master_item_dimensions")
                .fetch_one(&self.client.pool)
                .await?,
        )
    }

    async fn load(&self, rows: &[serde_json::Value]) -> Result<i64> {
        if rows.is_empty() {
            return Ok(0);
        }

        let dimensions = rows
            .iter()
            .map(MasterItemDimension::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        let mut tx = self.client.pool.begin().await?;
        let mut loaded = 0;

        for row in dimensions {
            let current: Option<NaiveDateTime> = sqlx::query_scalar(
                "SELECT source_updated_at FROM olap.master_item_dimensions \
                 WHERE source_id = $1 AND is_current FOR UPDATE",
            )
            .bind(row.source_id)
            .fetch_optional(&mut *tx)
            .await?;

            if current.is_some_and(|updated_at| updated_at >= row.source_updated_at) {
                continue;
            }

            sqlx::query(
                "UPDATE olap.master_item_dimensions \
                 SET is_current = false, end_from = $2 \
                 WHERE source_id = $1 AND is_current",
            )
            .bind(row.source_id)
            .bind(row.source_updated_at)
            .execute(&mut *tx)
            .await?;

            sqlx::query(
                "INSERT INTO olap.master_item_dimensions (\
                    id, source_id, source_tenant_id, source_category_id, source_parent_id,\
                    code, name, description, is_active, sort_order, metadata, multilang,\
                    source_created_at, source_updated_at, start_from, is_current\
                 ) VALUES (\
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, true\
                 )",
            )
            .bind(Uuid::now_v7())
            .bind(row.source_id)
            .bind(row.source_tenant_id)
            .bind(row.source_category_id)
            .bind(row.source_parent_id)
            .bind(row.code)
            .bind(row.name)
            .bind(row.description)
            .bind(row.is_active)
            .bind(row.sort_order)
            .bind(row.metadata.map(sqlx::types::Json))
            .bind(row.multilang.map(sqlx::types::Json))
            .bind(row.source_created_at)
            .bind(row.source_updated_at)
            .bind(row.source_updated_at)
            .execute(&mut *tx)
            .await?;

            loaded += 1;
        }

        tx.commit().await?;
        Ok(loaded)
    }
}

#[cfg(test)]
mod tests {
    use super::MasterItemDimension;

    #[test]
    fn transforms_master_item_for_scd2() {
        let value = serde_json::json!({
            "id": "018f55c3-4a5d-7d8e-8f9a-b0c1d2e3f401",
            "tenant_id": null,
            "category_id": "018f55c3-4a5d-7d8e-8f9a-b0c1d2e3f402",
            "code": "EMPLOYMENT_TYPE",
            "name": "Permanent",
            "description": null,
            "is_active": true,
            "sort_order": 1,
            "metadata": {"key": "value"},
            "created_at": "2026-08-12T00:00:00",
            "updated_at": "2026-08-12T01:00:00",
            "parent_id": null,
            "multilang": null
        });

        let dimension = MasterItemDimension::try_from(&value).unwrap();
        assert_eq!(dimension.code, "EMPLOYMENT_TYPE");
        assert_eq!(
            dimension.source_updated_at.to_string(),
            "2026-08-12 01:00:00"
        );
    }
}
