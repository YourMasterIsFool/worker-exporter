use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Postgres, QueryBuilder};
use tracing::{debug, instrument};
use uuid::Uuid;

use crate::{
    core::{
        traits::{
            etl::{fetch_data_json, DataFetcherJson},
            extractor::{
                build_query_trait::{BuildTableTrait, TableEnum},
                source_incremental_extract::SourceIncrementalExtract,
            },
            load::LoadEnum,
        },
        types::WhereField,
    },
    infrastructure::{
        db::organization_client::OrganizationClient,
        source::oltp::organization::enums::OrganizationTableEnum,
    },
};

#[derive(Debug, FromRow, Serialize, Deserialize)]
pub struct IngestionJobLevelModel {
    pub id: Uuid,
    pub name: String,
}
pub struct IngestionJobLevelSource {
    pub pool: Arc<OrganizationClient>,
}

impl BuildTableTrait<OrganizationTableEnum> for IngestionJobLevelSource {
    fn table(&self) -> OrganizationTableEnum {
        // Pastikan AtsIngestionTable memiliki varian untuk Joblevels
        OrganizationTableEnum::JobLevel
    }

    fn columns(&self) -> &'static [&'static str] {
        &[
            "id",
            "name",
            // "description",
            // "tenant_id",
            // "group_id",
            // "cost_center_id",
            // "parent_id",
            // "status",
            // "created_at",
            // "updated_at",
            // "deleted_at",
            // "permission_keys",
            // "filterable_attribute",
            // "keywords",
        ]
    }
}

#[async_trait::async_trait]
impl DataFetcherJson for IngestionJobLevelSource {
    #[instrument(skip(self), fields(types = "ingestion job data fetcher"))]
    async fn fetch(
        &self,
        filters: Option<Vec<WhereField>>,
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        let sql = format!(" SELECT id,name from {} ", self.table().table_name());

        let mut qb = QueryBuilder::new(&sql);

        let parse_result =
            fetch_data_json::<IngestionJobLevelModel>(&mut qb, filters, &self.pool.pool, None)
                .await;

        match parse_result {
            Ok(result) => {
                tracing::debug!(" ingestion job level source {:?}", result.clone());
                Ok(result)
            }
            Err(e) => {
                tracing::error!("{}", e);
                Err(e)
            }
        }
    }
}

#[async_trait::async_trait]
impl SourceIncrementalExtract for IngestionJobLevelSource {
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
            .build_query_as::<IngestionJobLevelModel>()
            .fetch_all(&self.pool.pool)
            .await;

        match result {
            Ok(result) => {
                let last_cursor: Option<Uuid> = result.last().map(|last| last.id);

                let parse_result = result
                    .iter()
                    .map(serde_json::to_value)
                    .collect::<Result<Vec<serde_json::Value>, _>>()?;

                tracing::debug!("ingestion job level {:?}", parse_result);
                Ok((parse_result, last_cursor))
            }
            Err(e) => {
                debug!("ingestion job level {}", e);
                Err(anyhow::anyhow!(e))
            }
        }
    }
}
