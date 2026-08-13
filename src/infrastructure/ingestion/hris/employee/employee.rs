use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Postgres, QueryBuilder};
use tracing::instrument;
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
    infrastructure::{
        db::{ats_client::AtsClient, hris_client::HrisClient},
        ingestion::hris::{normalize_hris_filters, types::HrisIngestionTable},
    },
};

#[derive(Debug, FromRow, Serialize, Deserialize)]
pub struct IngestionEmployee {
    pub id: String,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub full_name: Option<String>,
    pub email: Option<String>, // Kolom lain di-comment sesuai aslinya
}

pub struct IngestionEmployeeSource {
    pub pool: Arc<HrisClient>,
}

impl BuildTableTrait<HrisIngestionTable> for IngestionEmployeeSource {
    fn table(&self) -> HrisIngestionTable {
        HrisIngestionTable::Employee
    }

    fn columns(&self) -> &'static [&'static str] {
        &["id", "first_name", "last_name", "full_name", "email"]
    }
}

#[async_trait::async_trait]
impl DataFetcherJson for IngestionEmployeeSource {
    #[instrument(skip(self), fields(types = "employee data fetcher"))]
    async fn fetch(
        &self,
        filters: Option<Vec<WhereField>>,
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        let filters = normalize_hris_filters(filters);

        let sql = format!(
            " SELECT {} from {} ",
            self.columns().join(", "),
            self.table().table_name()
        );

        let mut qb = QueryBuilder::new(&sql);

        let parse_result =
            fetch_data_json::<IngestionEmployee>(&mut qb, filters, &self.pool.pool, None).await;

        match parse_result {
            Ok(result) => {
                tracing::debug!("employee source {:?}", result.clone());
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
impl SourceIncrementalExtract for IngestionEmployeeSource {
    async fn extract(
        &self,
        cursor: Option<Uuid>,
        limit: i64,
        _strategy_enum: &LoadEnum,
    ) -> anyhow::Result<(Vec<serde_json::Value>, Option<Uuid>)> {
        let sql_query = format!(
            "SELECT {} FROM {}",
            self.columns().join(", "),
            self.table().table_name()
        );

        let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(sql_query);

        if let Some(c) = cursor {
            qb.push(" WHERE id > ").push_bind(c);
        }
        qb.push(" LIMIT ").push_bind(limit);

        let result = qb
            .build_query_as::<IngestionEmployee>()
            .fetch_all(&self.pool.pool)
            .await?;

        let last_cursor_string = result.last().map(|last| last.id.clone());
        let last_cursor: Option<Uuid> = last_cursor_string.and_then(|id| id.parse().ok());

        let parse_result = result
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<serde_json::Value>, _>>()?;

        tracing::debug!("candidate source {:?}", parse_result.clone());
        Ok((parse_result, last_cursor))
    }
}
