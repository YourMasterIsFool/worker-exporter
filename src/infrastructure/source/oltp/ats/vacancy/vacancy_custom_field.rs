use std::sync::Arc;

use bigdecimal::BigDecimal;
use chrono::{NaiveDate, NaiveDateTime};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{FromRow, Postgres, QueryBuilder};
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
    infrastructure::{db::ats_client::AtsClient, source::oltp::ats::types::AtsIngestionTable},
};

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct OltpVacancyCustomField {
    pub id: Uuid,
    pub updated_at: Option<NaiveDateTime>,
    pub created_at: NaiveDateTime,
    pub deleted_at: Option<NaiveDateTime>,
    pub tenant_id: Uuid,
    pub group_id: Uuid,
    pub parent_id: Option<Uuid>,
    pub field_key: String,
    pub field_type: Option<String>,
    pub value_text: Option<String>,
    pub value_number: Option<BigDecimal>,
    pub value_id: Option<Uuid>,
    pub meta: Option<Value>,
    pub reference_module: Option<String>,
    pub value_bool: Option<bool>,
    pub value_date: Option<NaiveDate>,
}

pub struct OltpVacancyCustomFieldSource {
    pub pool: Arc<AtsClient>,
}

impl BuildTableTrait<AtsIngestionTable> for OltpVacancyCustomFieldSource {
    fn table(&self) -> AtsIngestionTable {
        AtsIngestionTable::VacancyCustomField
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
            "field_type::text AS field_type",
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

#[async_trait::async_trait]
impl DataFetcherJson for OltpVacancyCustomFieldSource {
    async fn fetch(&self, filters: Option<Vec<WhereField>>) -> anyhow::Result<Vec<Value>> {
        let sql = format!(
            "SELECT {} FROM {}",
            self.columns().join(", "),
            self.table().table_name()
        );
        let mut query = QueryBuilder::<Postgres>::new(sql);

        fetch_data_json::<OltpVacancyCustomField>(&mut query, filters, &self.pool.pool, None).await
    }
}

#[async_trait::async_trait]
impl SourceIncrementalExtract for OltpVacancyCustomFieldSource {
    async fn extract(
        &self,
        cursor: Option<Uuid>,
        limit: i64,
        _strategy_enum: &LoadEnum,
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
            .build_query_as::<OltpVacancyCustomField>()
            .fetch_all(&self.pool.pool)
            .await?;
        let next_cursor = rows.last().map(|row| row.id);
        let rows = rows
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<_>, _>>()?;

        Ok((rows, next_cursor))
    }
}
