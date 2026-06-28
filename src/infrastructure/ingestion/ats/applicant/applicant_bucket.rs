use std::{f32, sync::Arc};

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{types::JsonValue, FromRow, Postgres, QueryBuilder};
use tracing::info;
// Untuk jsonb
use uuid::Uuid;

use crate::{
    core::traits::{
        extractor::{
            build_query_trait::BuildTableTrait,
            source_incremental_extract::SourceIncrementalExtract,
        },
        load::LoadEnum,
    },
    infrastructure::{db::ats_client::AtsClient, ingestion::ats::types::AtsIngestionTable},
};
// Untuk numeric

#[derive(Debug, FromRow, Serialize, Deserialize)]
pub struct AtsApplicantBucketModel {
    pub id: Uuid,
    pub updated_at: NaiveDateTime,
    pub created_at: NaiveDateTime,
    pub deleted_at: Option<NaiveDateTime>,
    pub tenant_id: Uuid,
    pub group_id: Uuid,
    pub candidate_id: Uuid,
    pub vacancy_id: Uuid,
    pub source: Option<String>,
    pub is_blacklisted_candidate: bool,
    pub permission_keys: Option<Vec<String>>,
    pub match_percentage: Option<f64>,
    pub candidate_meta: Option<JsonValue>,
    pub current_process: Option<JsonValue>,
    pub filterable_attribute: Option<JsonValue>,
    pub keywords: Option<Vec<String>>,
}

pub struct AtsApplicantBucketSource {
    pub pool: Arc<AtsClient>,
}

impl BuildTableTrait<AtsIngestionTable> for AtsApplicantBucketSource {
    fn table(&self) -> AtsIngestionTable {
        AtsIngestionTable::ApplicantBucket
    }

    fn columns(&self) -> &'static [&'static str] {
        &[
            "id",
            "updated_at",
            "created_at",
            "deleted_at",
            "tenant_id",
            "group_id",
            "candidate_id",
            "vacancy_id",
            "source",
            "is_blacklisted_candidate",
            "permission_keys",
            "match_percentage",
            "candidate_meta",
            "current_process",
            "filterable_attribute",
            "keywords",
        ]
    }
}
#[async_trait::async_trait]
impl SourceIncrementalExtract for AtsApplicantBucketSource {
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
            .build_query_as::<AtsApplicantBucketModel>()
            .fetch_all(&self.pool.pool)
            .await;

        match result {
            Ok(result) => {
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
            Err(e) => {
                info!("errror applicant bucket {}", e);
                Err(anyhow::anyhow!(e))
            }
        }
    }
}
