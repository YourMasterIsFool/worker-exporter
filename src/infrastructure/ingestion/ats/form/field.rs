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
pub struct IngestionFormFieldModel {
    pub id: Uuid,
    pub name: String,
    pub updated_at: Option<NaiveDateTime>,
    pub created_at: Option<NaiveDateTime>,
    pub deleted_at: Option<NaiveDateTime>,
    pub tenant_id: Option<Uuid>,
    pub group_id: Option<Uuid>,
    pub actor_id: Option<Uuid>,
    pub key_field: Option<String>,
    pub field_type: Option<String>,
    pub field_purpose: Option<String>,
    pub tooltip: Option<String>,
    pub multiple_choice_option: Option<Value>,
    pub allowed_file_type: Option<Vec<String>>,
    pub maximum_file: Option<i32>,
    pub maximum_file_size_in_kb: Option<i32>,
    pub is_default_question: Option<bool>,
    pub status: Option<String>,
    pub is_range: Option<bool>,
    pub compare_operator: Option<String>,
    pub pivot: Option<i32>,
    pub min_pivot: Option<i32>,
    pub max_pivot: Option<i32>,
    pub data_mapping: Option<Value>,
    pub master_data: Option<Value>,
}

pub struct IngestionFormFieldSource {
    pub pool: Arc<AtsClient>,
}

impl BuildTableTrait<AtsIngestionTable> for IngestionFormFieldSource {
    fn table(&self) -> AtsIngestionTable {
        AtsIngestionTable::FormField
    }

    fn columns(&self) -> &'static [&'static str] {
        &[
            "id",
            "key_field AS name",
            "updated_at",
            "created_at",
            "deleted_at",
            "tenant_id",
            "group_id",
            "actor_id",
            "key_field",
            "field_type::text AS field_type",
            "field_purpose",
            "tooltip",
            "multiple_choice_option",
            "allowed_file_type::text[] AS allowed_file_type",
            "maximum_file",
            "maximum_file_size_in_kb",
            "is_default_question",
            "status::text AS status",
            "is_range",
            "compare_operator::text AS compare_operator",
            "pivot",
            "min_pivot",
            "max_pivot",
            "data_mapping",
            "master_data",
        ]
    }
}

#[async_trait::async_trait]
impl DataFetcherJson for IngestionFormFieldSource {
    #[instrument(skip(self), fields(types = "form field data fetcher"))]
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

        let parse_result =
            fetch_data_json::<IngestionFormFieldModel>(&mut qb, filters, &self.pool.pool, None)
                .await;

        match parse_result {
            Ok(result) => {
                tracing::debug!("form field source {:?}", result.clone());
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
impl SourceIncrementalExtract for IngestionFormFieldSource {
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
        qb.push(" LIMIT ").push_bind(limit);

        let result = qb
            .build_query_as::<IngestionFormFieldModel>()
            .fetch_all(&self.pool.pool)
            .await?;

        let last_cursor: Option<Uuid> = result.last().map(|last| last.id);

        let parse_result = result
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<serde_json::Value>, _>>()?;

        tracing::debug!("form field source {:?}", parse_result.clone());
        Ok((parse_result, last_cursor))
    }
}
