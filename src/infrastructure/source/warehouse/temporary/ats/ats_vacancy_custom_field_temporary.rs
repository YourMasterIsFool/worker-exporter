use std::sync::Arc;

use bigdecimal::BigDecimal;
use chrono::{NaiveDate, NaiveDateTime};
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
pub struct AtsVacancyCustomFieldTemporaryModel {
    pub id: Uuid,
    pub updated_at: Option<NaiveDateTime>,
    pub created_at: NaiveDateTime,
    pub deleted_at: Option<NaiveDateTime>,
    pub tenant_id: Uuid,
    pub group_id: Uuid,
    pub parent_id: Option<Uuid>,
    pub field_key: String,
    pub field_type: Option<String>,
    pub value_text: Option<String>,
    pub value_number: Option<BigDecimal>,
    pub value_id: Option<Uuid>,
    pub meta: Option<Value>,
    pub reference_module: Option<String>,
    pub value_bool: Option<bool>,
    pub value_date: Option<NaiveDate>,
    pub logging_id: Option<Uuid>,
}

pub struct AtsVacancyCustomFieldTemporarySource {
    pub pool: Arc<WarehouseClient>,
}

impl BuildTableTrait<WarehouseTemporaryTable> for AtsVacancyCustomFieldTemporarySource {
    fn table(&self) -> WarehouseTemporaryTable {
        WarehouseTemporaryTable::AtsVacancyCustomField
    }

    fn columns(&self) -> &'static [&'static str] {
        &[
            "id",
            "updated_at",
            "created_at",
            "deleted_at",
            "tenant_id",
            "group_id",
            "parent_id",
            "field_key",
            "field_type",
            "value_text",
            "value_number",
            "value_id",
            "meta",
            "reference_module",
            "value_bool",
            "value_date",
            "logging_id",
        ]
    }
}

#[async_trait::async_trait]
impl DataFetcherJson for AtsVacancyCustomFieldTemporarySource {
    #[instrument(
        skip(self),
        fields(types = "ats vacancy custom field temporary data fetcher")
    )]
    async fn fetch(
        &self,
        filters: Option<Vec<WhereField>>,
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        fetch_temporary_rows::<AtsVacancyCustomFieldTemporaryModel>(
            &self.pool.pool,
            self.table().table_name(),
            self.columns(),
            filters,
        )
        .await
    }
}

#[async_trait::async_trait]
impl SourceIncrementalExtract for AtsVacancyCustomFieldTemporarySource {
    async fn extract(
        &self,
        cursor: Option<Uuid>,
        limit: i64,
        _strategy_enum: &LoadEnum,
    ) -> anyhow::Result<(Vec<serde_json::Value>, Option<Uuid>)> {
        extract_temporary_rows::<AtsVacancyCustomFieldTemporaryModel, _>(
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
