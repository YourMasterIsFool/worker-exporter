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
        let sql = format!(
            " SELECT {} from {} ",
            self.columns().join(", "),
            self.table().table_name()
        );

        let mut qb = QueryBuilder::new(&sql);

        fetch_data_json::<AtsVacancyPicTemporaryModel>(&mut qb, filters, &self.pool.pool, None)
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
            .build_query_as::<AtsVacancyPicTemporaryModel>()
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
