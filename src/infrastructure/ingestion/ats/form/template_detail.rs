use std::sync::Arc;

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{FromRow, Postgres, QueryBuilder};
use tracing::instrument;
use uuid::Uuid;

use crate::{
    core::{
        traits::{
            etl::{fetch_data_json, DataFetcherJson},
            extractor::{
                build_query_trait::BuildTableTrait,
                source_incremental_extract::SourceIncrementalExtract,
            },
            load::LoadEnum,
        },
        types::WhereField,
    },
    infrastructure::{db::ats_client::AtsClient, ingestion::ats::types::AtsIngestionTable},
};

#[derive(Debug, Default, FromRow, Serialize, Deserialize)]
#[sqlx(default)]
pub struct IngestionFormTemplateDetailModel {
    pub id: Uuid,
    pub name: String,
    pub updated_at: Option<NaiveDateTime>,
    pub created_at: Option<NaiveDateTime>,
    pub deleted_at: Option<NaiveDateTime>,
    pub form_field_id: Option<Uuid>,
    pub template_id: Option<Uuid>,
    pub sequence: Option<i32>,
    pub is_mandatory: Option<bool>,
    pub status: Option<String>,
    pub criteria: Option<Value>,
    pub score_setting: Option<Value>,
    pub duration_in_minute: Option<i32>,
    pub field_snapshot: Option<Value>,
}

pub struct IngestionFormTemplateDetailSource {
    pub pool: Arc<AtsClient>,
}

impl BuildTableTrait<AtsIngestionTable> for IngestionFormTemplateDetailSource {
    fn table(&self) -> AtsIngestionTable {
        AtsIngestionTable::FormTemplateDetail
    }

    fn columns(&self) -> &'static [&'static str] {
        &[
            "id",
            "COALESCE(field_snapshot->>'name', id::text) AS name",
            "updated_at",
            "created_at",
            "deleted_at",
            "form_field_id",
            "template_id",
            "sequence",
            "is_mandatory",
            "status::text AS status",
            "criteria",
            "score_setting",
            "duration_in_minute",
            "field_snapshot",
        ]
    }
}

#[async_trait::async_trait]
impl DataFetcherJson for IngestionFormTemplateDetailSource {
    #[instrument(skip(self), fields(types = "form template detail data fetcher"))]
    async fn fetch(
        &self,
        filters: Option<Vec<WhereField>>,
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        let sql = format!(
            " SELECT {} from {} ",
            self.columns().join(", "),
            self.table().table_name()
        );

        let mut qb = QueryBuilder::new(&sql);

        let parse_result = fetch_data_json::<IngestionFormTemplateDetailModel>(
            &mut qb,
            filters,
            &self.pool.pool,
            Some("sequence"),
        )
        .await;

        match parse_result {
            Ok(result) => {
                tracing::debug!("form template detail source {:?}", result.clone());
                Ok(result)
            }
            Err(e) => {
                tracing::error!("{}", e);
                Err(e)
            }
        }
    }
}

#[async_trait::async_trait]
impl SourceIncrementalExtract for IngestionFormTemplateDetailSource {
    async fn extract(
        &self,
        cursor: Option<Uuid>,
        limit: i64,
        _strategy_enum: &LoadEnum,
    ) -> anyhow::Result<(Vec<serde_json::Value>, Option<Uuid>)> {
        let sql_query = format!(
            "SELECT {} FROM {}",
            self.columns().join(","),
            self.table().table_name()
        );

        let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(sql_query);

        if let Some(c) = cursor {
            qb.push(" WHERE id > ").push_bind(c);
        }
        qb.push(" ORDER BY sequence ASC, id ASC ");
        qb.push(" LIMIT ").push_bind(limit);

        let result = qb
            .build_query_as::<IngestionFormTemplateDetailModel>()
            .fetch_all(&self.pool.pool)
            .await?;

        let last_cursor: Option<Uuid> = result.last().map(|last| last.id);

        let parse_result = result
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<serde_json::Value>, _>>()?;

        tracing::debug!("form template detail source {:?}", parse_result.clone());
        Ok((parse_result, last_cursor))
    }
}
