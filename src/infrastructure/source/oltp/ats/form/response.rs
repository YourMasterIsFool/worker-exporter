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
    domain::oltp::organization::forms::FormResponseIngestionEntity,
    infrastructure::{db::ats_client::AtsClient, source::oltp::ats::types::AtsIngestionTable},
};

pub struct FormResponseSource {
    pub pool: Arc<AtsClient>,
}

impl BuildTableTrait<AtsIngestionTable> for FormResponseSource {
    fn table(&self) -> AtsIngestionTable {
        AtsIngestionTable::FormResponse
    }

    fn columns(&self) -> &'static [&'static str] {
        &[
            "id",
            "form_id",
            "respondent_id",
            "status::text AS status",
            "started_at",
            "submitted_at",
            "total_score",
            "total_point",
            "notify_after_submit",
            "respondent_name",
            "respondent_image",
            "respondent_email",
            "identifier_id",
            "extension",
        ]
    }
}

#[async_trait::async_trait]
impl DataFetcherJson for FormResponseSource {
    async fn fetch(&self, filters: Option<Vec<WhereField>>) -> anyhow::Result<Vec<Value>> {
        let mut query = QueryBuilder::<Postgres>::new(format!(
            "SELECT {} FROM {}",
            self.columns().join(", "),
            self.table().table_name()
        ));
        fetch_data_json::<FormResponseIngestionEntity>(
            &mut query,
            filters,
            &self.pool.pool,
            Some("started_at DESC"),
        )
        .await
    }
}

#[async_trait::async_trait]
impl SourceIncrementalExtract for FormResponseSource {
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
            .build_query_as::<FormResponseIngestionEntity>()
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
