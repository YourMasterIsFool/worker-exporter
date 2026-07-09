use std::sync::Arc;

use serde::{Deserialize, Serialize};
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
    infrastructure::{db::ats_client::AtsClient, source::oltp::ats::types::AtsIngestionTable},
};

#[derive(Debug, FromRow, Serialize, Deserialize)]
pub struct IngestionRequirementStageTemplateModel {
    pub id: Uuid,
    pub template_name: String,
}

pub struct IngestionRequirementStageTemplateSource {
    pub pool: Arc<AtsClient>,
}

impl BuildTableTrait<AtsIngestionTable> for IngestionRequirementStageTemplateSource {
    fn table(&self) -> AtsIngestionTable {
        AtsIngestionTable::RequirementStageTemplate
    }

    fn columns(&self) -> &'static [&'static str] {
        &["id", "template_name"]
    }
}

#[async_trait::async_trait]
impl DataFetcherJson for IngestionRequirementStageTemplateSource {
    #[instrument(skip(self), fields(types = "requirement stage template data fetcher"))]
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

        let parse_result = fetch_data_json::<IngestionRequirementStageTemplateModel>(
            &mut qb,
            filters,
            &self.pool.pool,
            None,
        )
        .await;

        match parse_result {
            Ok(result) => {
                tracing::debug!("requirement stage template source {:?}", result.clone());
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
impl SourceIncrementalExtract for IngestionRequirementStageTemplateSource {
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
            .build_query_as::<IngestionRequirementStageTemplateModel>()
            .fetch_all(&self.pool.pool)
            .await?;

        let last_cursor: Option<Uuid> = result.last().map(|last| last.id);

        let parse_result = result
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<serde_json::Value>, _>>()?;

        tracing::debug!(
            "requirement stage template source {:?}",
            parse_result.clone()
        );
        Ok((parse_result, last_cursor))
    }
}
