use std::sync::Arc;

use bigdecimal::BigDecimal;
use chrono::{NaiveDate, NaiveDateTime};
use serde::{Deserialize, Serialize};
use sqlx::{types::JsonValue, FromRow, Postgres, QueryBuilder};

// Untuk jsonb
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
// Untuk numeric

#[derive(Debug, Serialize, Deserialize, Clone, sqlx::Type)]
#[sqlx(type_name = "candidate.candidate_client_field_field_type_enum")]
pub enum CandidateClientFieldType {
    #[sqlx(rename = "TEXT")]
    Text,
    #[sqlx(rename = "NUMBER")]
    Number,
    #[sqlx(rename = "DATE")]
    Date,
    #[sqlx(rename = "UUID")]
    Uuid,
    #[sqlx(rename = "BOOLEAN")]
    Boolean,
}
#[derive(Debug, FromRow, Serialize, Deserialize)]
pub struct AtsCandidateClientFieldModel {
    pub id: Uuid,
    pub updated_at: NaiveDateTime,
    pub created_at: NaiveDateTime,
    pub deleted_at: Option<NaiveDateTime>,
    pub tenant_id: Uuid,
    pub group_id: Uuid,
    pub parent_id: Option<Uuid>,
    pub field_key: String,
    pub field_type: CandidateClientFieldType, // Bisa diganti dengan enum kustom jika Anda membuat type-nya
    pub value_text: Option<String>,
    pub value_number: Option<BigDecimal>,
    pub value_id: Option<Uuid>,
    pub meta: Option<JsonValue>, // Menggunakan serde_json::Value via sqlx
    pub reference_module: Option<String>,
    pub value_bool: Option<bool>,
    pub value_date: Option<NaiveDate>,
}
pub struct AtsCandidateClientFieldSource {
    pub pool: Arc<AtsClient>,
}
impl BuildTableTrait<AtsIngestionTable> for AtsCandidateClientFieldSource {
    fn table(&self) -> AtsIngestionTable {
        AtsIngestionTable::CandidateClientField
    }

    fn columns(&self) -> &'static [&'static str] {
        &[
            "id",
            "updated_at",
            "created_at",
            "deleted_at",
            "tenant_id",
            "group_id",
            "parent_id",
            "field_key",
            "field_type",
            "value_text",
            "value_number",
            "value_id",
            "meta",
            "reference_module",
            "value_bool",
            "value_date",
        ]
    }
}

// implement data fetcher
#[async_trait::async_trait]
impl DataFetcherJson for AtsCandidateClientFieldSource {
    async fn fetch(
        &self,
        filters: Option<Vec<WhereField>>,
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        let sql = format!(" SELECT * from {} ", self.table().table_name());

        let mut qb = QueryBuilder::new(&sql);

        let parse_result = fetch_data_json::<AtsCandidateClientFieldModel>(
            &mut qb,
            filters,
            &self.pool.pool,
            None,
        )
        .await?;

        tracing::debug!(" candidate source {:?}", parse_result.clone());
        Ok(parse_result)
    }
}
#[async_trait::async_trait]
impl SourceIncrementalExtract for AtsCandidateClientFieldSource {
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
            .build_query_as::<AtsCandidateClientFieldModel>()
            .fetch_all(&self.pool.pool)
            .await?;

        let last_cursor: Option<Uuid> = result.last().map(|last| last.id);

        let parse_result = result
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<serde_json::Value>, _>>()?;

        tracing::debug!(" candidate source {:?}", parse_result.clone());
        Ok((parse_result, last_cursor))
    }
}
