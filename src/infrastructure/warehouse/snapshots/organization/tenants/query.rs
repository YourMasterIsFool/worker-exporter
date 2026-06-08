use std::sync::Arc;

use anyhow::Result;
use sqlx::{Execute, QueryBuilder, Row};
use tracing::info;

use crate::{
    core::traits::{
        extractor::incremental_source::{IncrementalSource, WhereField},
        load::{LoadEnum, LoaderWarehouse},
    },
    domain::snapshots::organization::tenant_snapshot::{
        TenantSnapshot, TenantSnapshotQueryRepository,
    },
    infrastructure::warehouse::{
        snapshots::organization::tenants::model::TenantSnapshotModel,
        warehouse_client::WarehouseClient,
    },
    utils::build_query,
};

pub struct TenantPoolQuerySnapshot {
    pub pool: Arc<WarehouseClient>,
}

impl LoaderWarehouse for TenantPoolQuerySnapshot {
    fn table(&self) -> &'static str {
        "tenant_snapshots"
    }

    fn columns(&self) -> &'static [&'static str] {
        &["id", "tenant_name", "tenant_code", "tenant_id"]
    }

    fn conflicted_columns(&self) -> &'static [&'static str] {
        &[]
    }
}

impl TenantPoolQuerySnapshot {
    async fn find_min_cursor(&self, filters: Option<Vec<WhereField>>) -> anyhow::Result<String> {
        let sql = format!("SELECT id from {}", &self.table());
        let mut qb = QueryBuilder::new(&sql);

        if let Some(where_fields) = filters {
            qb.push(" WHERE 1=1 ");

            for field in where_fields {
                qb.push(format!(" AND {} ", field.field))
                    .push_bind(field.value);
            }
        }

        qb.push(" LIMIT 1 ORDER BY id ASC ");

        let result = qb
            .build_query_scalar::<String>()
            .fetch_one(&self.pool.pool)
            .await?;

        Ok(result)
    }
}
#[async_trait::async_trait]
impl IncrementalSource for TenantPoolQuerySnapshot {
    type Cursor = String;
    type Output = serde_json::Value;
    async fn fetch(
        &self,
        stategy_enum: &LoadEnum,
        limit: i64,
        cursor: Option<Self::Cursor>,
    ) -> Result<Vec<Self::Output>> {
        info!("run query ");
        let qb = build_query(self, stategy_enum);
        let jsonable_query_builder = format!(" SELECT to_jsonb(t) from ({}", &qb);
        let mut query = QueryBuilder::new(&jsonable_query_builder);
        println!("this query {}", qb.clone());
        if let Some(cursor_data) = cursor {
            query.push(" AND id > ").push_bind(cursor_data);
        }

        query.push(" ORDER BY id ASC ");
        query.push(" LIMIT ").push_bind(limit);
        query.push(") t");

        let result = query.build().fetch_all(&self.pool.pool).await?;
        info!("result query {:?}", result);
        let formatted_result = result
            .into_iter()
            .map(|f| {
                let val = f.try_get(0).map_err(|err| anyhow::anyhow!(err))?;
                Ok(val)
            })
            .collect::<Result<Vec<serde_json::Value>, anyhow::Error>>()?;
        Ok(formatted_result)
    }
}

#[async_trait::async_trait]
impl TenantSnapshotQueryRepository for TenantPoolQuerySnapshot {
    async fn find_by_ids(&self, tenant_ids: &[String]) -> anyhow::Result<Vec<TenantSnapshot>> {
        let build_query = format!(
            "SELECT * FROM {} WHERE tenant_id IN ({})",
            self.table(),
            tenant_ids.join(", ")
        );
        let mut query = QueryBuilder::new(&build_query);
        let result = query
            .build_query_as::<TenantSnapshotModel>()
            .fetch_all(&self.pool.pool)
            .await?;

        Ok(result.iter().map(|f| f.into()).collect())
    }

    async fn find_last_cursor(&self) -> anyhow::Result<String> {
        let build_query = format!("SELECT MAX(id) FROM {}", self.table());
        let mut query = QueryBuilder::new(&build_query);
        let result = query
            .build_query_scalar::<String>()
            .fetch_one(&self.pool.pool)
            .await?;
        Ok(result)
    }

    async fn find_first_cursor(&self) -> anyhow::Result<String> {
        let build_query = format!("SELECT MIN(id) FROM {}", self.table());
        let mut query = QueryBuilder::new(&build_query);
        let result = query
            .build_query_scalar::<String>()
            .fetch_one(&self.pool.pool)
            .await?;
        Ok(result)
    }
}
