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
    infrastructure::{db::ats_client::AtsClient, source::oltp::ats::types::AtsIngestionTable},
};

#[derive(Debug, Default, FromRow, Serialize, Deserialize)]
#[sqlx(default)]
pub struct IngestionRequirementStageTemplateDetailModel {
    pub id: Uuid,
    pub updated_at: Option<NaiveDateTime>,
    pub created_at: Option<NaiveDateTime>,
    pub deleted_at: Option<NaiveDateTime>,
    pub recruitment_stage_id: Option<Uuid>,
    pub recruitment_stage_template_id: Option<Uuid>,
    pub order: Option<i32>,
    pub status: Option<String>,
    pub stage_snapshot: Option<Value>,
    pub recruitment_stage_meta: Option<Value>,
}

pub struct IngestionRequirementStageTemplateDetailSource {
    pub pool: Arc<AtsClient>,
}

impl BuildTableTrait<AtsIngestionTable> for IngestionRequirementStageTemplateDetailSource {
    fn table(&self) -> AtsIngestionTable {
        AtsIngestionTable::RequirementStageTemplateDetail
    }

    fn columns(&self) -> &'static [&'static str] {
        &[
            "detail.id AS id",
            "detail.updated_at",
            "detail.created_at",
            "detail.deleted_at",
            "detail.recruitment_stage_id",
            "detail.recruitment_stage_template_id",
            "detail.\"order\" AS \"order\"",
            "detail.status::text AS status",
            "detail.stage_snapshot",
            "to_jsonb(stage) AS recruitment_stage_meta",
        ]
    }
}

#[async_trait::async_trait]
impl DataFetcherJson for IngestionRequirementStageTemplateDetailSource {
    #[instrument(
        skip(self),
        fields(types = "requirement stage template detail data fetcher")
    )]
    async fn fetch(
        &self,
        filters: Option<Vec<WhereField>>,
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        let qualified_filters = filters.map(|filters| {
            filters
                .into_iter()
                .map(|mut field| {
                    if field.field == "id" {
                        field.field = "detail.id".into();
                    }
                    field
                })
                .collect::<Vec<_>>()
        });

        let sql = format!(
            " SELECT {} from {} detail LEFT JOIN applicant.recruitment_stage stage ON stage.id = detail.recruitment_stage_id ",
            self.columns().join(", "),
            self.table().table_name()
        );

        let mut qb = QueryBuilder::new(&sql);

        let parse_result = fetch_data_json::<IngestionRequirementStageTemplateDetailModel>(
            &mut qb,
            qualified_filters,
            &self.pool.pool,
            Some("detail.\"order\""),
        )
        .await;

        match parse_result {
            Ok(result) => {
                tracing::debug!(
                    "requirement stage template detail source {:?}",
                    result.clone()
                );
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
impl SourceIncrementalExtract for IngestionRequirementStageTemplateDetailSource {
    async fn extract(
        &self,
        cursor: Option<Uuid>,
        limit: i64,
        _strategy_enum: &LoadEnum,
    ) -> anyhow::Result<(Vec<serde_json::Value>, Option<Uuid>)> {
        let sql_query = format!(
            "SELECT {} FROM {} detail LEFT JOIN applicant.recruitment_stage stage ON stage.id = detail.recruitment_stage_id",
            self.columns().join(","),
            self.table().table_name()
        );

        let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(sql_query);

        if let Some(c) = cursor {
            qb.push(" WHERE detail.id > ").push_bind(c);
        }
        qb.push(" ORDER BY detail.\"order\" ASC, detail.id ASC ");
        qb.push(" LIMIT ").push_bind(limit);

        let result = qb
            .build_query_as::<IngestionRequirementStageTemplateDetailModel>()
            .fetch_all(&self.pool.pool)
            .await?;

        let last_cursor: Option<Uuid> = result.last().map(|last| last.id);

        let parse_result = result
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<serde_json::Value>, _>>()?;

        tracing::debug!(
            "requirement stage template detail source {:?}",
            parse_result.clone()
        );
        Ok((parse_result, last_cursor))
    }
}
