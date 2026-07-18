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
pub struct AtsVacancyPicTemporaryModel {
    pub id: Uuid,
    pub vacancy_id: Uuid,
    pub employee_id: Uuid,
    pub employee_meta: Option<Value>,
    pub is_verified: bool,
    pub created_at: NaiveDateTime,
    pub logging_id: Option<String>,
}

pub struct AtsVacancyPicTemporarySource {
    pub pool: Arc<WarehouseClient>,
}

impl BuildTableTrait<WarehouseTemporaryTable> for AtsVacancyPicTemporarySource {
    fn table(&self) -> WarehouseTemporaryTable {
        WarehouseTemporaryTable::AtsVacancyPic
    }

    fn columns(&self) -> &'static [&'static str] {
        &[
            "id",
            "vacancy_id",
            "employee_id",
            "employee_meta",
            "is_verified",
            "created_at",
            "logging_id",
        ]
    }
}

#[async_trait::async_trait]
impl DataFetcherJson for AtsVacancyPicTemporarySource {
    #[instrument(skip(self), fields(types = "ats vacancy pic temporary data fetcher"))]
    async fn fetch(
        &self,
        filters: Option<Vec<WhereField>>,
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        fetch_temporary_rows::<AtsVacancyPicTemporaryModel>(
            &self.pool.pool,
            self.table().table_name(),
            self.columns(),
            filters,
        )
        .await
    }
}

#[async_trait::async_trait]
impl SourceIncrementalExtract for AtsVacancyPicTemporarySource {
    async fn extract(
        &self,
        cursor: Option<Uuid>,
        limit: i64,
        _strategy_enum: &LoadEnum,
    ) -> anyhow::Result<(Vec<serde_json::Value>, Option<Uuid>)> {
        extract_temporary_rows::<AtsVacancyPicTemporaryModel, _>(
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
