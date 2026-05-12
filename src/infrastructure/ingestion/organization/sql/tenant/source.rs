use std::sync::Arc;

use anyhow::{Ok, Result};
use sqlx::{Postgres, QueryBuilder, Value};
use uuid::Uuid;

use crate::{
    core::traits::extractor::{
        build_query_trait::BuildQueryTrait, incremental_extract::IncrementalExtract,
    },
    infrastructure::ingestion::organization::{
        enums::OrganizationTableEnum, organization_postgres_client::OrganizatinPostgresClient,
        sql::tenant::model::TenantModel,
    },
};

pub struct TenantSource {
    pub pool: Arc<OrganizatinPostgresClient>,
}

impl BuildQueryTrait<OrganizationTableEnum> for TenantSource {
    fn query(&self) -> &'static str {
        "SELECT id, tenant_code, tenant_name, tenant_vision, tenant_description, tenant_level_id, status, created_at, deleted_at"
    }
    fn table(&self) -> OrganizationTableEnum {
        OrganizationTableEnum::Tenant
    }
}

#[async_trait::async_trait]
impl IncrementalExtract for TenantSource {
    async fn extract(
        &self,
        cursor: Option<Uuid>,
        limit: i64,
        params: Option<serde_json::Value>,
    ) -> Result<(Vec<serde_json::Value>, Option<Uuid>)> {
        let sql_query = format!("{} from {} where 1=1 ", &self.query(), "core.tenants");

        let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(sql_query);

        if let Some(c) = cursor {
            qb.push(" and id > ").push_bind(c);
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
        Ok((parse_result, last_cursor))
    }
}
