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
        db::master_client::MasterClient,
        ingestion::master::types::MasterIngestionTable,
    },
};

#[derive(Debug, Serialize, Deserialize, Clone, sqlx::Type)]
#[sqlx(type_name = "geo_nodes_level_enum")]
pub enum GeoNodesLevel {
    #[sqlx(rename = "country")]
    Country,
    #[sqlx(rename = "adm1")]
    Adm1,
    #[sqlx(rename = "adm2")]
    Adm2,
    #[sqlx(rename = "adm3")]
    Adm3,
    #[sqlx(rename = "adm4")]
    Adm4,
}
// Untuk numeric

#[derive(Debug, FromRow, Serialize, Deserialize)]
pub struct MasterGeoModel {
    pub id: Uuid,
    pub level: GeoNodesLevel,
    pub parent_id: Option<Uuid>,
    pub code: String,
    pub name: String,
    pub description: Option<String>,
    pub is_active: bool,
    pub sort_order: Option<i32>,
    pub metadata: Option<JsonValue>,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
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
            fetch_data_json::<MasterGeoModel>(&mut qb, filters, &self.pool.pool, None).await?;
        tracing::debug!(" master geo source {:?}", parse_result.clone());
        Ok(parse_result)
    }
}
#[async_trait::async_trait]
impl SourceIncrementalExtract for MasterGeoSource {
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
            .build_query_as::<MasterGeoModel>()
            .fetch_all(&self.pool.pool)
            .await?;

        let last_cursor: Option<Uuid> = result.last().map(|last| last.id);

        let parse_result = result
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<serde_json::Value>, _>>()?;

        tracing::debug!(" geo source {:?}", parse_result.clone());
        Ok((parse_result, last_cursor))
    }
}
