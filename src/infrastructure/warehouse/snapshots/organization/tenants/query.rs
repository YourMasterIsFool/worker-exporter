use std::sync::Arc;

use anyhow::Result;
use sqlx::QueryBuilder;
use tracing::info;

use crate::{
    core::traits::{
        extractor::{
            build_query_trait::BuildTableTrait,
            incremental_source::{IncrementalSource, WhereField},
        },
        load::LoaderWarehouse,
    },
    domain::snapshots::organization::tenant_snapshot::{
        TenantSnapshot, TenantSnapshotQueryRepository,
    },
    infrastructure::warehouse::{
        snapshots::organization::tenants::model::TenantSnapshotModel,
        warehouse_client::WarehouseClient,
    },
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

#[async_trait::async_trait]
impl IncrementalSource for TenantPoolQuerySnapshot {
    type Cursor = String;
    type Output = TenantSnapshot;
    async fn fetch(
        &self,
        limit: i64,
        cursor: Option<Self::Cursor>,
        _filters: Option<Vec<WhereField>>,
    ) -> Result<(Vec<Self::Output>)> {
        info!("run query ");
        let mut query = QueryBuilder::new(
            "SELECT id, tenant_name, tenant_code, tenant_id FROM tenant_snapshots WHERE 1=1",
        );

        if let Some(cursor_data) = cursor {
            query.push(" AND id > ").push_bind(cursor_data);
        }

        query.push(" ORDER BY id ASC ");
        query.push(" LIMIT ").push_bind(limit);

        let result = query
            .build_query_as::<TenantSnapshotModel>()
            .fetch_all(&self.pool.pool)
            .await?;
        Ok(result.iter().map(|f| f.into()).collect())
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
