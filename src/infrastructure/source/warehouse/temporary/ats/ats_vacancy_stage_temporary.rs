use std::sync::Arc;

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::FromRow;
use tracing::instrument;
use uuid::Uuid;

use crate::{
    core::{
        traits::{
            etl::DataFetcherJson,
            extractor::{
                build_query_trait::BuildTableTrait,
                source_incremental_extract::SourceIncrementalExtract,
            },
            load::LoadEnum,
        },
        types::WhereField,
    },
    infrastructure::{
        db::warehouse_client::WarehouseClient, loader::warehouse::types::WarehouseTemporaryTable,
    },
    utils::{extract_temporary_rows, fetch_temporary_rows},
};

#[derive(Debug, FromRow, Serialize, Deserialize)]
pub struct AtsVacancyStageTemporaryModel {
    pub id: Uuid,
    pub updated_at: Option<NaiveDateTime>,
    pub created_at: NaiveDateTime,
    pub deleted_at: Option<NaiveDateTime>,
    pub vacancy_id: Uuid,
    pub stage_name: String,
    pub stage_type: String,
    pub is_default: bool,
    pub stage_email_template_id: Option<Uuid>,
    pub stage_email_template_reject: Option<Uuid>,
    pub sequence: i32,
    pub is_locked: bool,
    pub form_template_id: Option<Uuid>,
    pub share_access: Value,
    pub logging_id: Option<String>,
}

pub struct AtsVacancyStageTemporarySource {
    pub pool: Arc<WarehouseClient>,
}

impl BuildTableTrait<WarehouseTemporaryTable> for AtsVacancyStageTemporarySource {
    fn table(&self) -> WarehouseTemporaryTable {
        WarehouseTemporaryTable::AtsVacancyStage
    }

    fn columns(&self) -> &'static [&'static str] {
        &[
            "id",
            "updated_at",
            "created_at",
            "deleted_at",
            "vacancy_id",
            "stage_name",
            "stage_type",
            "is_default",
            "stage_email_template_id",
            "stage_email_template_reject",
            "sequence",
            "is_locked",
            "form_template_id",
            "share_access",
            "logging_id",
        ]
    }
}

#[async_trait::async_trait]
impl DataFetcherJson for AtsVacancyStageTemporarySource {
    #[instrument(skip(self), fields(types = "ats vacancy stage temporary data fetcher"))]
    async fn fetch(
        &self,
        filters: Option<Vec<WhereField>>,
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        fetch_temporary_rows::<AtsVacancyStageTemporaryModel>(
            &self.pool.pool,
            self.table().table_name(),
            self.columns(),
            filters,
        )
        .await
    }
}

#[async_trait::async_trait]
impl SourceIncrementalExtract for AtsVacancyStageTemporarySource {
    async fn extract(
        &self,
        cursor: Option<Uuid>,
        limit: i64,
        _strategy_enum: &LoadEnum,
    ) -> anyhow::Result<(Vec<serde_json::Value>, Option<Uuid>)> {
        extract_temporary_rows::<AtsVacancyStageTemporaryModel, _>(
            &self.pool.pool,
            self.table().table_name(),
            self.columns(),
            cursor,
            limit,
            |row| row.id,
        )
        .await
    }
}
