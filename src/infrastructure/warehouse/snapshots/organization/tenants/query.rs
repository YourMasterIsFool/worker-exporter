use std::{result, sync::Arc};

use anyhow::{Ok, Result};
use sqlx::{postgres::PgPoolOptions, PgPool, QueryBuilder};
use tracing_subscriber::filter;

use crate::{
    core::traits::{
        extractor::{
            build_query_trait::BuildTableTrait,
            incremental_source::{IncrementalSource, WhereField},
        },
        load::{LoadEnum, LoaderWarehouse},
    },
    domain::snapshots::organization::tenant_snapshot::{
        TenantSnapshot, TenantSnapshotQueryRepository,
    },
    infrastructure::{
        ingestion::organization::enums::OrganizationTableEnum,
        warehouse::{
            snapshots::organization::tenants::model::TenantSnapshotModel,
            warehouse_client::WarehouseClient,
        },
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
        &["tenant_name", "tenant_code", "tenant_id"]
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
        filters: Option<Vec<WhereField>>,
    ) -> Result<(Vec<Self::Output>)> {
        let query = build_query(self, LoadEnum::Select(filters));
        let mut query = QueryBuilder::new(&query);
        if let Some(c) = cursor {
            query.push(" WHERE id > ").push_bind(c);
        }
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
}
