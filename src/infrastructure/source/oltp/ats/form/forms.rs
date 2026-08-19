use std::sync::Arc;

use serde_json::Value;
use sqlx::{Postgres, QueryBuilder};
use uuid::Uuid;

use crate::{
    core::traits::{
        etl::{fetch_data_json, DataFetcherJson},
        extractor::{
            build_query_trait::BuildTableTrait,
            source_incremental_extract::SourceIncrementalExtract,
        },
        load::LoadEnum,
    },
    core::types::WhereField,
    domain::oltp::organization::forms::FormIngestionEntity,
    infrastructure::{db::ats_client::AtsClient, source::oltp::ats::types::AtsIngestionTable},
};

pub struct FormSource {
    pub pool: Arc<AtsClient>,
}

impl BuildTableTrait<AtsIngestionTable> for FormSource {
    fn table(&self) -> AtsIngestionTable {
        AtsIngestionTable::Form
    }

    fn columns(&self) -> &'static [&'static str] {
        &[
            "id",
            "updated_at",
            "created_at",
            "deleted_at",
            "tenant_id",
            "title",
            "description",
            "scoring_enabled",
            "minimum_score",
            "timer_enabled",
            "duration_minutes",
            "permission_keys",
            "status::text AS status",
            "group_id",
        ]
    }
}

#[async_trait::async_trait]
impl DataFetcherJson for FormSource {
    async fn fetch(&self, filters: Option<Vec<WhereField>>) -> anyhow::Result<Vec<Value>> {
        let mut query = QueryBuilder::<Postgres>::new(format!(
            "SELECT {} FROM {}",
            self.columns().join(", "),
            self.table().table_name()
        ));
        fetch_data_json::<FormIngestionEntity>(
            &mut query,
            filters,
            &self.pool.pool,
            Some("title ASC"),
        )
        .await
    }
}

#[async_trait::async_trait]
impl SourceIncrementalExtract for FormSource {
    async fn extract(
        &self,
        cursor: Option<Uuid>,
        limit: i64,
        _strategy: &LoadEnum,
    ) -> anyhow::Result<(Vec<Value>, Option<Uuid>)> {
        let mut query = QueryBuilder::<Postgres>::new(format!(
            "SELECT {} FROM {}",
            self.columns().join(", "),
            self.table().table_name()
        ));
        if let Some(cursor) = cursor {
            query.push(" WHERE id > ").push_bind(cursor);
        }
        query.push(" ORDER BY id ASC LIMIT ").push_bind(limit);
        let rows = query
            .build_query_as::<FormIngestionEntity>()
            .fetch_all(&self.pool.pool)
            .await?;
        let next_cursor = rows.last().map(|row| row.id);
        Ok((
            rows.iter()
                .map(serde_json::to_value)
                .collect::<Result<Vec<_>, _>>()?,
            next_cursor,
        ))
    }
}
