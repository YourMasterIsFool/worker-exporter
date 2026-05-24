use std::sync::Arc;

use anyhow::{Ok, Result};
use sqlx::{Postgres, QueryBuilder, Value};
use tracing::info;
use uuid::Uuid;

use crate::{
    core::traits::extractor::{
        build_query_trait::BuildTableTrait, source_incremental_extract::SourceIncrementalExtract,
    },
    domain::ingestion::organization::tenant::{TenantEntity, TenantIngestionRepo},
    infrastructure::ingestion::organization::{
        enums::OrganizationTableEnum, organization_postgres_client::OrganizatinPostgresClient,
        sql::tenant::model::TenantModel,
    },
};

pub struct TenantSource {
    pub pool: Arc<OrganizatinPostgresClient>,
}

impl BuildTableTrait<OrganizationTableEnum> for TenantSource {
    fn table(&self) -> OrganizationTableEnum {
        OrganizationTableEnum::Tenant
    }
    fn columns(&self) -> &'static [&'static str] {
        &[
            "id",
            "tenant_code",
            "tenant_name",
            "tenant_vision",
            "tenant_description",
            "tenant_level_id",
            "status",
            "created_at",
            "deleted_at",
            "update_at",
        ]
    }
}

#[async_trait::async_trait]
impl TenantIngestionRepo for TenantSource {
    async fn findByIds(&self, tenant_ids: Vec<String>) -> Vec<TenantEntity> {
        let sql_query = format!(
            "SELECT {} FROM {} WHERE id IN ({})",
            self.columns().join(","),
            self.table().postgres(),
            tenant_ids.join(",")
        );

        let mut qb = QueryBuilder::new(&sql_query);

        let result = qb
            .build_query_as::<TenantModel>()
            .fetch_all(&self.pool.pool)
            .await
            .unwrap_or_default();
        result.into_iter().map(|r| r.into()).collect()
    }
}

#[async_trait::async_trait]
impl SourceIncrementalExtract for TenantSource {
    async fn extract(
        &self,
        cursor: Option<Uuid>,
        limit: i64,
        _params: Option<serde_json::Value>,
    ) -> Result<(Vec<serde_json::Value>, Option<Uuid>)> {
        let sql_query = format!(
            "SELECT {} FROM {}",
            self.columns().join(","),
            self.table().postgres()
        );

        let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(sql_query);

        if let Some(c) = cursor {
            qb.push(" WHERE id > ").push_bind(c);
        }
        qb.push(" LIMIT ").push_bind(limit);
        let result = qb
            .build_query_as::<TenantModel>()
            .fetch_all(&self.pool.pool)
            .await?;

        let last_cursor: Option<Uuid> = match result.last() {
            None => None,
            Some(last) => Some(last.id),
        };

        let parse_result = result
            .iter()
            .map(|f| serde_json::to_value(f))
            .collect::<Result<Vec<serde_json::Value>, _>>()?;

        info!(" tenant source {:?}", parse_result.clone());
        Ok((parse_result, last_cursor))
    }
}
