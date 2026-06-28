use std::sync::Arc;

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
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

#[derive(Debug, FromRow, Serialize, Deserialize)]
struct AtsExperienceModel {
    pub id: Uuid,
    pub updated_at: NaiveDateTime,
    pub created_at: NaiveDateTime,
    pub deleted_at: Option<NaiveDateTime>, // NULL di DB menjadi Option di Rust
    pub candidate_id: Uuid,
    pub job_title: String,
    pub company_name: String,
    pub employment_type: Option<String>,
    pub industry: Option<String>,
    pub job_description: Option<String>,
    pub start_date: NaiveDate, // 'date' di PG biasanya map ke NaiveDate
    pub end_date: Option<NaiveDate>,
    pub is_still_here: bool,
    pub job_type_id: Option<String>,
}

pub struct AtsExperinceSource {
    pub pool: Arc<AtsClient>,
}

impl BuildTableTrait<AtsIngestionTable> for AtsExperinceSource {
    fn table(&self) -> AtsIngestionTable {
        AtsIngestionTable::Experience
    }

    fn columns(&self) -> &'static [&'static str] {
        &[
            "id",
            "updated_at",
            "created_at",
            "deleted_at",
            "candidate_id",
            "job_title",
            "company_name",
            "employment_type",
            "industry",
            "job_description",
            "start_date",
            "end_date",
            "is_still_here",
            "job_type_id",
        ]
    }
}

#[async_trait::async_trait]
impl DataFetcherJson for AtsExperinceSource {
    #[instrument(skip(self), fields(types = "candidate data fetcher"))]
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
            fetch_data_json::<AtsExperienceModel>(&mut qb, filters, &self.pool.pool).await;

        match parse_result {
            Ok(result) => {
                tracing::info!(" candidate source {:?}", result.clone());
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
impl SourceIncrementalExtract for AtsExperinceSource {
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
            .build_query_as::<AtsExperienceModel>()
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

        tracing::info!(" candidate source {:?}", parse_result.clone());
        Ok((parse_result, last_cursor))
    }
}
