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
pub struct IngestionJobPositionModel {
    pub id: Uuid,
    pub position_name: String,
    pub custom_fields: serde_json::Value,
}
pub struct IngestionOrganizationJobPositionSource {
    pub pool: Arc<OrganizationClient>,
}

impl BuildTableTrait<OrganizationTableEnum> for IngestionOrganizationJobPositionSource {
    fn table(&self) -> OrganizationTableEnum {
        // Pastikan AtsIngestionTable memiliki varian untuk JobPositions
        OrganizationTableEnum::JobPosition
    }

    fn columns(&self) -> &'static [&'static str] {
        &[
            "id",
            "position_name",
            r#"COALESCE(
                (
                    SELECT jsonb_agg(to_jsonb(job_position_custom_field) ORDER BY job_position_custom_field.created_at)
                    FROM core.job_position_custom_fields AS job_position_custom_field
                    WHERE job_position_custom_field.parent_id = core.job_positions.id
                        AND job_position_custom_field.deleted_at IS NULL
                ),
                '[]'::jsonb
            ) AS custom_fields"#,
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
impl DataFetcherJson for IngestionOrganizationJobPositionSource {
    #[instrument(skip(self), fields(types = "ingestion job data fetcher"))]
    async fn fetch(
        &self,
        filters: Option<Vec<WhereField>>,
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        let sql = format!(
            "SELECT {} FROM {}",
            self.columns().join(", "),
            self.table().table_name(),
        );

        let mut qb = QueryBuilder::new(&sql);

        let parse_result =
            fetch_data_json::<IngestionJobPositionModel>(&mut qb, filters, &self.pool.pool, None)
                .await;

        match parse_result {
            Ok(result) => {
                tracing::debug!(
                    " ingestion job position locations source {:?}",
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
impl SourceIncrementalExtract for IngestionOrganizationJobPositionSource {
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
            .build_query_as::<IngestionJobPositionModel>()
            .fetch_all(&self.pool.pool)
            .await;

        match result {
            Ok(result) => {
                let last_cursor: Option<Uuid> = result.last().map(|last| last.id);

                let parse_result = result
                    .iter()
                    .map(serde_json::to_value)
                    .collect::<Result<Vec<serde_json::Value>, _>>()?;

                tracing::debug!("ingestion job position {:?}", parse_result);
                Ok((parse_result, last_cursor))
            }
            Err(e) => {
                debug!("ingestion job position {}", e);
                Err(anyhow::anyhow!(e))
            }
        }
    }
}
