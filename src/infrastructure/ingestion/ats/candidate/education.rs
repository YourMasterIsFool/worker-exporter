use std::sync::Arc;

use bigdecimal::BigDecimal;
use chrono::{format::Numeric, DateTime, NaiveDate, NaiveDateTime, Utc};
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
struct AtsEducationModel {
    pub id: Uuid,
    pub updated_at: NaiveDateTime,
    pub created_at: NaiveDateTime,
    pub deleted_at: Option<NaiveDateTime>,
    pub candidate_id: Uuid,
    pub institute_name: String,
    pub level: Option<String>,
    pub major: Option<String>,
    pub start_date: NaiveDate,
    pub end_date: Option<NaiveDate>,
    pub is_still_here: bool,
    pub academic_score: Option<BigDecimal>,
}

pub struct AtsEducationSource {
    pub pool: Arc<AtsClient>,
}

impl BuildTableTrait<AtsIngestionTable> for AtsEducationSource {
    fn table(&self) -> AtsIngestionTable {
        AtsIngestionTable::Education
    }

    fn columns(&self) -> &'static [&'static str] {
        &[
            "id",
            "updated_at",
            "created_at",
            "deleted_at",
            "candidate_id",
            "institute_name",
            "level",
            "major",
            "start_date",
            "end_date",
            "is_still_here",
            "academic_score",
        ]
    }
}
#[async_trait::async_trait]
impl DataFetcherJson for AtsEducationSource {
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
            fetch_data_json::<AtsEducationModel>(&mut qb, filters, &self.pool.pool).await;

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
impl SourceIncrementalExtract for AtsEducationSource {
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
            .build_query_as::<AtsEducationModel>()
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
