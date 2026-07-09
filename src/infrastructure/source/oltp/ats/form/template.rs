use std::sync::Arc;

use chrono::NaiveDateTime;
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

#[derive(Debug, Default, FromRow, Serialize, Deserialize)]
#[sqlx(default)]
pub struct IngestionFormTemplateModel {
    pub id: Uuid,
    pub updated_at: Option<NaiveDateTime>,
    pub created_at: Option<NaiveDateTime>,
    pub deleted_at: Option<NaiveDateTime>,
    pub tenant_id: Option<Uuid>,
    pub group_id: Option<Uuid>,
    pub name: String,
    pub description: Option<String>,
    pub template_type: Option<String>,
    pub status: Option<String>,
}

pub struct IngestionFormTemplateSource {
    pub pool: Arc<AtsClient>,
}

impl BuildTableTrait<AtsIngestionTable> for IngestionFormTemplateSource {
    fn table(&self) -> AtsIngestionTable {
        AtsIngestionTable::FormTemplate
    }

    fn columns(&self) -> &'static [&'static str] {
        &[
            "id",
            "updated_at",
            "created_at",
            "deleted_at",
            "tenant_id",
            "group_id",
            "name",
            "description",
            "type::text AS template_type",
            "status::text AS status",
        ]
    }
}

#[async_trait::async_trait]
impl DataFetcherJson for IngestionFormTemplateSource {
    #[instrument(skip(self), fields(types = "form template data fetcher"))]
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

        let parse_result = fetch_data_json::<IngestionFormTemplateModel>(
            &mut qb,
            filters,
            &self.pool.pool,
            Some("name"),
        )
        .await;

        match parse_result {
            Ok(result) => {
                tracing::debug!("form template source {:?}", result.clone());
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
impl SourceIncrementalExtract for IngestionFormTemplateSource {
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
        qb.push(" ORDER BY name ASC, id ASC ");
        qb.push(" LIMIT ").push_bind(limit);

        let result = qb
            .build_query_as::<IngestionFormTemplateModel>()
            .fetch_all(&self.pool.pool)
            .await?;

        let last_cursor: Option<Uuid> = result.last().map(|last| last.id);

        let parse_result = result
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<serde_json::Value>, _>>()?;

        tracing::debug!("form template source {:?}", parse_result.clone());
        Ok((parse_result, last_cursor))
    }
}
