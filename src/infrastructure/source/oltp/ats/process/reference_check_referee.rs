use std::sync::Arc;

use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{FromRow, Postgres, QueryBuilder};
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
    infrastructure::{db::ats_client::AtsClient, source::oltp::ats::types::AtsIngestionTable},
};

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct ReferenceCheckReferee {
    pub id: Uuid,
    pub updated_at: Option<NaiveDateTime>,
    pub created_at: NaiveDateTime,
    pub deleted_at: Option<NaiveDateTime>,
    pub tenant_id: Uuid,
    pub group_id: Uuid,
    pub reference_check_id: Uuid,
    pub name: Option<String>,
    pub email: String,
    pub submission_link: Option<String>,
    pub submission_status: String,
    pub submitted_at: Option<DateTime<Utc>>,
    pub identifier_id: Option<String>,
}

pub struct ReferenceCheckRefereeSource {
    pub pool: Arc<AtsClient>,
}

impl BuildTableTrait<AtsIngestionTable> for ReferenceCheckRefereeSource {
    fn table(&self) -> AtsIngestionTable {
        AtsIngestionTable::ReferenceCheckReferee
    }

    fn columns(&self) -> &'static [&'static str] {
        &[
            "id",
            "updated_at",
            "created_at",
            "deleted_at",
            "tenant_id",
            "group_id",
            "reference_check_id",
            "name",
            "email",
            "submission_link",
            "submission_status",
            "submitted_at",
            "identifier_id",
        ]
    }
}

#[async_trait::async_trait]
impl DataFetcherJson for ReferenceCheckRefereeSource {
    async fn fetch(&self, filters: Option<Vec<WhereField>>) -> anyhow::Result<Vec<Value>> {
        let sql = format!(
            "SELECT {} FROM {}",
            self.columns().join(", "),
            self.table().table_name()
        );
        let mut query = QueryBuilder::<Postgres>::new(sql);
        fetch_data_json::<ReferenceCheckReferee>(&mut query, filters, &self.pool.pool, None).await
    }
}

#[async_trait::async_trait]
impl SourceIncrementalExtract for ReferenceCheckRefereeSource {
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
            .build_query_as::<ReferenceCheckReferee>()
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
