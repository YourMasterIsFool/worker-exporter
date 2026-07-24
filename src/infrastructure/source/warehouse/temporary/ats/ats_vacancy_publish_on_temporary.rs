use std::sync::Arc;

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
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
pub struct AtsVacancyPublishOnTemporaryModel {
    pub id: Uuid,
    pub updated_at: Option<NaiveDateTime>,
    pub created_at: NaiveDateTime,
    pub deleted_at: Option<NaiveDateTime>,
    pub vacancy_id: Uuid,
    pub platform: String,
    pub status: String,
    pub careersite_tenant_id: Option<Uuid>,
}

pub struct AtsVacancyPublishOnTemporarySource {
    pub pool: Arc<WarehouseClient>,
}

impl BuildTableTrait<WarehouseTemporaryTable> for AtsVacancyPublishOnTemporarySource {
    fn table(&self) -> WarehouseTemporaryTable {
        WarehouseTemporaryTable::AtsVacancyPublishOn
    }

    fn columns(&self) -> &'static [&'static str] {
        &[
            "id",
            "updated_at",
            "created_at",
            "deleted_at",
            "vacancy_id",
            "platform",
            "status",
            "careersite_tenant_id",
        ]
    }
}

#[async_trait::async_trait]
impl DataFetcherJson for AtsVacancyPublishOnTemporarySource {
    #[instrument(
        skip(self),
        fields(types = "ats vacancy publish-on temporary data fetcher")
    )]
    async fn fetch(
        &self,
        filters: Option<Vec<WhereField>>,
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        fetch_temporary_rows::<AtsVacancyPublishOnTemporaryModel>(
            &self.pool.pool,
            self.table().table_name(),
            self.columns(),
            filters,
        )
        .await
    }
}

#[async_trait::async_trait]
impl SourceIncrementalExtract for AtsVacancyPublishOnTemporarySource {
    async fn extract(
        &self,
        cursor: Option<Uuid>,
        limit: i64,
        _strategy_enum: &LoadEnum,
    ) -> anyhow::Result<(Vec<serde_json::Value>, Option<Uuid>)> {
        extract_temporary_rows::<AtsVacancyPublishOnTemporaryModel, _>(
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
