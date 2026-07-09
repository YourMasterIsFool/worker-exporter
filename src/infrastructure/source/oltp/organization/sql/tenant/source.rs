use std::sync::Arc;

use anyhow::{Ok, Result};
use sqlx::{Postgres, QueryBuilder};
use tracing::debug;
use uuid::Uuid;

use crate::{
    core::traits::{
        extractor::{
            build_query_trait::{BuildTableTrait, TableEnum},
            source_incremental_extract::SourceIncrementalExtract,
        },
        load::LoadEnum,
    },
    domain::oltp::organization::tenant::{TenantIngestionEntity, TenantIngestionRepo},
    infrastructure::source::oltp::organization::{
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
    async fn find_by_ids(&self, tenant_ids: Vec<String>) -> Vec<TenantIngestionEntity> {
        let sql_query = format!(
            "SELECT {} FROM {} WHERE id IN ({})",
            self.columns().join(","),
            self.table().table_name(),
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
        _strategy_enum: &LoadEnum,
    ) -> Result<(Vec<serde_json::Value>, Option<Uuid>)> {
        let sql_query = format!(
            "SELECT {} FROM {}",
            self.columns().join(","),
            self.table().table_name()
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

        let last_cursor: Option<Uuid> = result.last().map(|last| last.id);

        let parse_result = result
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<serde_json::Value>, _>>()?;

        debug!(" tenant source {:?}", parse_result.clone());
        Ok((parse_result, last_cursor))
    }
}
