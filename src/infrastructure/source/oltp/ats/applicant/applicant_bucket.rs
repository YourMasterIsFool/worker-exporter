use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{types::JsonValue, FromRow, Postgres, QueryBuilder};
use tracing::debug;
// Untuk jsonb
use crate::core::types::Identifierable;
use crate::core::types::WhereField;
use crate::utils::incremental_cursor_resolver::IncrementalCursorResolver;
use crate::{
    core::traits::{
        etl::{fetch_data_json, DataFetcherJson},
        extractor::{
            build_query_trait::BuildTableTrait,
            source_incremental_extract::SourceIncrementalExtract,
        },
        load::LoadEnum,
    },
    infrastructure::{db::ats_client::AtsClient, source::oltp::ats::types::AtsIngestionTable},
};
use uuid::Uuid;
// Untuk numeric

#[derive(Debug, FromRow, Serialize, Deserialize)]
pub struct AtsApplicantBucketModel {
    pub id: Uuid,
    pub updated_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
    pub tenant_id: Uuid,
    pub group_id: Uuid,
    pub candidate_id: Uuid,
    pub vacancy_id: Uuid,
    pub source: Option<String>,
    pub is_blacklisted_candidate: bool,
    pub permission_keys: Option<Vec<String>>,
    pub match_percentage: Option<f64>,
    pub current_process: Option<JsonValue>,
    pub filterable_attribute: Option<JsonValue>,
    pub keywords: Option<Vec<String>>,
}

impl Identifierable for AtsApplicantBucketModel {
    fn get_id(&self) -> Uuid {
        self.id
    }
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
            "current_process",
            "filterable_attribute",
            "keywords",
        ]
    }
}

#[async_trait::async_trait]
impl DataFetcherJson for AtsApplicantBucketSource {
    async fn fetch(
        &self,
        filters: Option<Vec<WhereField>>,
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        let sql = format!(
            " SELECT DISTINCT ON (candidate_id) {} FROM {} ",
            self.columns().join(", "),
            self.table().table_name()
        );

        let mut qb = QueryBuilder::new(&sql);

        let parse_result = fetch_data_json::<AtsApplicantBucketModel>(
            &mut qb,
            filters,
            &self.pool.pool,
            Some("candidate_id, created_at DESC, id DESC"),
        )
        .await?;

        tracing::debug!(" applicant bucket source {:?}", parse_result.clone());
        Ok(parse_result)
    }
}

#[async_trait::async_trait]
impl SourceIncrementalExtract for AtsApplicantBucketSource {
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

        let mut filters: Option<Vec<WhereField>> = None;

        let mut incremental_resolver = IncrementalCursorResolver::<AtsApplicantBucketModel>::new(
            qb,
            &self.pool.pool,
            limit,
            "Applicant bucket source",
        );

        if let LoadEnum::Select(filter, _) = _strategy_enum {
            filters = filter.clone();
        }

        incremental_resolver
            .with_filters(&filters)
            .with_cursor(cursor)
            .resolve()
            .await
    }
}
