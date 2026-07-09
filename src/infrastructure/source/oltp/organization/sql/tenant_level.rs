use std::convert::TryFrom;

use serde::Serialize;
use sqlx::PgPool;

use crate::{
    core::traits::etl::TypedExtractor,
    domain::oltp::organization::tenant_level::TenantLevelIngestionEntity,
};

#[derive(Debug, Clone, sqlx::FromRow, Serialize)]
pub struct TenantLevelIngestionModel {
    pub id: uuid::Uuid,
    pub tenant_id: uuid::Uuid,
    pub code: String,
    pub name: String,
    pub description: Option<String>,
    pub is_active: bool,
    pub sort_order: Option<i32>,
    pub metadata: Option<serde_json::Value>,
    pub created_at: chrono::NaiveDateTime,
    pub updated_at: chrono::NaiveDateTime,
    pub parent_id: Option<uuid::Uuid>,
    pub multilang: Option<serde_json::Value>,
}

impl TryFrom<TenantLevelIngestionModel> for TenantLevelIngestionEntity {
    type Error = anyhow::Error;

    fn try_from(value: TenantLevelIngestionModel) -> anyhow::Result<Self> {
        Ok(TenantLevelIngestionEntity {
            id: value.id,
            tenant_id: value.tenant_id,
            oltp_tenant_id: value.tenant_id,
            code: value.code,
            name: value.name,
            description: value.description,
            is_active: value.is_active,
            sort_order: value.sort_order,
            metadata: value.metadata,
            created_at: value.created_at,
            updated_at: value.updated_at,
            parent_id: value.parent_id,
            multilang: value.multilang,
        })
    }
}

pub struct TenantLevelIngestion;

#[async_trait::async_trait]
impl TypedExtractor for TenantLevelIngestion {
    type Output = TenantLevelIngestionEntity;
    type Cursor = String;

    async fn fetch(
        &self,
        pool: &PgPool,
        cursor: Option<Self::Cursor>,
        limit: i64,
    ) -> anyhow::Result<Vec<Self::Output>> {
        let query = sqlx::query_as::<_, TenantLevelIngestionModel>(
            "SELECT * FROM tenant_level WHERE id > $1 ORDER BY id LIMIT $2",
        )
        .bind(cursor.unwrap_or_default())
        .bind(limit);

        let result = query.fetch_all(pool).await?;

        result
            .into_iter()
            .map(TenantLevelIngestionEntity::try_from)
            .collect::<anyhow::Result<Vec<_>>>()
    }
}
