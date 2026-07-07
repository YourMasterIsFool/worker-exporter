use std::sync::Arc;

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use serde_json::Value;
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
    infrastructure::{
        db::warehouse_client::WarehouseClient, loader::warehouse::types::WarehouseTemporaryTable,
    },
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
        let sql = format!(
            " SELECT {} from {} ",
            self.columns().join(", "),
            self.table().table_name()
        );

        let mut qb = QueryBuilder::new(&sql);

        fetch_data_json::<AtsVacancyStageTemporaryModel>(&mut qb, filters, &self.pool.pool, None)
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
            .build_query_as::<AtsVacancyStageTemporaryModel>()
            .fetch_all(&self.pool.pool)
            .await?;
        let last_cursor = result.last().map(|last| last.id);
        let rows = result
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<serde_json::Value>, _>>()?;

        Ok((rows, last_cursor))
    }
}
