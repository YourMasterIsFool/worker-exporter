use std::sync::Arc;

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{types::JsonValue, FromRow, QueryBuilder};
use tracing::instrument;
use uuid::Uuid;

use crate::core::traits::etl::{fetch_data_json, DataFetcherJson};
use crate::core::traits::extractor::build_query_trait::BuildTableTrait;
use crate::core::types::WhereField;
use crate::infrastructure::db::ats_client::AtsClient;
use crate::infrastructure::source::oltp::ats::types::AtsIngestionTable;

#[derive(Debug, FromRow, Serialize, Deserialize)]
pub struct AtsApplicantSnapshotModel {
    pub id: Uuid,
    pub updated_at: NaiveDateTime,
    pub created_at: NaiveDateTime,
    pub deleted_at: Option<NaiveDateTime>,
    pub tenant_id: Uuid,
    pub group_id: Uuid,
    pub applicant_bucket_id: Uuid,
    pub candidate_data: JsonValue,
    pub is_active: bool,
    pub actor_id: Uuid,
}

pub struct AtsApplicantSnapshotSource {
    pub pool: Arc<AtsClient>,
}

impl BuildTableTrait<AtsIngestionTable> for AtsApplicantSnapshotSource {
    fn table(&self) -> AtsIngestionTable {
        AtsIngestionTable::ApplicantSnapshot
    }

    fn columns(&self) -> &'static [&'static str] {
        &[
            "id",
            "updated_at",
            "created_at",
            "deleted_at",
            "tenant_id",
            "group_id",
            "applicant_bucket_id",
            "candidate_data",
            "is_active",
            "actor_id",
        ]
    }
}

#[async_trait::async_trait]
impl DataFetcherJson for AtsApplicantSnapshotSource {
    #[instrument(skip(self), fields(types = "applicant snapshot fetcher"))]
    async fn fetch(&self, filters: Option<Vec<WhereField>>) -> anyhow::Result<Vec<Value>> {
        let columns = self.columns().join(", ");
        let table = "applicant.applicant_snapshots";

        // Keep only the latest active snapshot per applicant bucket. The
        // subquery lets fetch_data_json append dynamic filters safely.
        let sql = format!(
            "SELECT DISTINCT ON (applicant_bucket_id) {columns} \
             FROM (SELECT {columns} FROM {table} WHERE is_active = TRUE) AS snapshot"
        );
        let mut query = QueryBuilder::new(&sql);

        fetch_data_json::<AtsApplicantSnapshotModel>(
            &mut query,
            filters,
            &self.pool.pool,
            Some("applicant_bucket_id, created_at DESC, id DESC"),
        )
        .await
    }
}
