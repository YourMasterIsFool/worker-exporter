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
pub struct AtsVacancyRequirementTemporaryModel {
    pub id: Uuid,
    pub updated_at: Option<NaiveDateTime>,
    pub created_at: NaiveDateTime,
    pub deleted_at: Option<NaiveDateTime>,
    pub vacancy_id: Uuid,
    pub key_field: String,
    pub field_type: String,
    pub field_purpose: String,
    pub tooltip: Option<String>,
    pub multiple_choice_option: Option<Value>,
    pub allowed_file_type: Option<Value>,
    pub maximum_file: Option<i32>,
    pub maximum_file_size_in_kb: Option<i32>,
    pub is_default_question: bool,
    pub is_range: bool,
    pub compare_operator: Option<String>,
    pub pivot: Option<i32>,
    pub min_pivot: Option<i32>,
    pub max_pivot: Option<i32>,
    pub criteria: Option<Value>,
    pub is_mandatory: bool,
    pub master_data: Option<Value>,
    pub data_mapping: Option<Value>,
    pub logging_id: Option<String>,
}

pub struct AtsVacancyRequirementTemporarySource {
    pub pool: Arc<WarehouseClient>,
}

impl BuildTableTrait<WarehouseTemporaryTable> for AtsVacancyRequirementTemporarySource {
    fn table(&self) -> WarehouseTemporaryTable {
        WarehouseTemporaryTable::AtsVacancyRequirement
    }

    fn columns(&self) -> &'static [&'static str] {
        &[
            "id",
            "updated_at",
            "created_at",
            "deleted_at",
            "vacancy_id",
            "key_field",
            "field_type",
            "field_purpose",
            "tooltip",
            "multiple_choice_option",
            "allowed_file_type",
            "maximum_file",
            "maximum_file_size_in_kb",
            "is_default_question",
            "is_range",
            "compare_operator",
            "pivot",
            "min_pivot",
            "max_pivot",
            "criteria",
            "is_mandatory",
            "master_data",
            "data_mapping",
            "logging_id",
        ]
    }
}

#[async_trait::async_trait]
impl DataFetcherJson for AtsVacancyRequirementTemporarySource {
    #[instrument(
        skip(self),
        fields(types = "ats vacancy requirement temporary data fetcher")
    )]
    async fn fetch(
        &self,
        filters: Option<Vec<WhereField>>,
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        fetch_temporary_rows::<AtsVacancyRequirementTemporaryModel>(
            &self.pool.pool,
            self.table().table_name(),
            self.columns(),
            filters,
        )
        .await
    }
}

#[async_trait::async_trait]
impl SourceIncrementalExtract for AtsVacancyRequirementTemporarySource {
    async fn extract(
        &self,
        cursor: Option<Uuid>,
        limit: i64,
        _strategy_enum: &LoadEnum,
    ) -> anyhow::Result<(Vec<serde_json::Value>, Option<Uuid>)> {
        extract_temporary_rows::<AtsVacancyRequirementTemporaryModel, _>(
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
