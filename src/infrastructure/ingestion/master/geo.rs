use std::{f32, sync::Arc};

use chrono::{DateTime, NaiveDate, Utc};
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
        db::{ats_client::AtsClient, master_client::MasterClient},
        ingestion::{ats::types::AtsIngestionTable, master::types::MasterIngestionTable},
    },
};
// Untuk numeric

#[derive(Debug, FromRow, Serialize, Deserialize)]
pub struct MasterGeoModel {
    pub id: Uuid,
    pub level: String,
    pub parent_id: Option<Uuid>,
    pub code: String,
    pub name: String,
    pub description: Option<String>,
    pub is_active: bool,
    pub sort_order: Option<i32>,
    pub metadata: Option<JsonValue>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
pub struct MasterGeoSource {
    pub pool: Arc<MasterClient>,
}

impl BuildTableTrait<MasterIngestionTable> for MasterGeoSource {
    fn table(&self) -> MasterIngestionTable {
        MasterIngestionTable::Geo
    }

    fn columns(&self) -> &'static [&'static str] {
        &[
            "id",
            "level",
            "parent_id",
            "code",
            "name",
            "description",
            "is_active",
            "sort_order",
            "metadata",
            "created_at",
            "updated_at",
        ]
    }
}

#[async_trait::async_trait]
impl DataFetcherJson for MasterGeoSource {
    async fn fetch(
        &self,
        filters: Option<Vec<WhereField>>,
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        let sql_query = format!(
            "SELECT {} FROM {}",
            self.columns().join(","),
            self.table().table_name()
        );

        let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(sql_query);

        let parse_result =
            fetch_data_json::<MasterGeoModel>(&mut qb, filters, &self.pool.pool).await?;
        tracing::info!(" master geo source {:?}", parse_result.clone());
        Ok(parse_result)
    }
}
#[async_trait::async_trait]
impl SourceIncrementalExtract for MasterGeoSource {
    async fn extract(
        &self,
        cursor: Option<Uuid>,
        limit: i64,
        strategy_enum: &LoadEnum,
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
            .build_query_as::<MasterGeoModel>()
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

        tracing::info!(" geo source {:?}", parse_result.clone());
        Ok((parse_result, last_cursor))
    }
}
