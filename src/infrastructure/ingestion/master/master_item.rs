use std::sync::Arc;

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use sqlx::{types::JsonValue, FromRow, Postgres, QueryBuilder}; // Untuk jsonb
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
    infrastructure::{
        db::master_client::MasterClient, ingestion::master::types::MasterIngestionTable,
    },
};
// Untuk numeric

#[derive(Debug, FromRow, Serialize, Deserialize)]
pub struct MasterMasterItemModel {
    pub id: Uuid,
    pub tenant_id: Option<Uuid>,
    pub category_id: Uuid,
    pub code: String,
    pub name: String,
    pub description: Option<String>,
    pub is_active: bool,
    pub sort_order: Option<i32>,
    pub metadata: Option<JsonValue>,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
    pub parent_id: Option<Uuid>,
    pub multilang: Option<JsonValue>,
}
pub struct MasterMasterItemsSource {
    pub pool: Arc<MasterClient>,
}

impl BuildTableTrait<MasterIngestionTable> for MasterMasterItemsSource {
    fn table(&self) -> MasterIngestionTable {
        MasterIngestionTable::MasterItem
    }

    fn columns(&self) -> &'static [&'static str] {
        &[
            "id",
            "tenant_id",
            "category_id",
            "code",
            "name",
            "description",
            "is_active",
            "sort_order",
            "metadata",
            "created_at",
            "updated_at",
            "parent_id",
            "multilang",
        ]
    }
}

#[async_trait::async_trait]
impl DataFetcherJson for MasterMasterItemsSource {
    async fn fetch(
        &self,
        filters: Option<Vec<WhereField>>,
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        let sql = format!(" SELECT  * from {} ", self.table().table_name());

        let mut qb = QueryBuilder::new(&sql);

        let parse_result =
            fetch_data_json::<MasterMasterItemModel>(&mut qb, filters, &self.pool.pool, None).await;

        match parse_result {
            Ok(result) => {
                tracing::debug!(" Master Item Model {:?}", result.clone());
                Ok(result)
            }
            Err(e) => {
                tracing::error!("Failed to fetch Master Item Model: {:?}", e);
                Err(e)
            }
        }
    }
}
#[async_trait::async_trait]
impl SourceIncrementalExtract for MasterMasterItemsSource {
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
            .build_query_as::<MasterMasterItemModel>()
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
