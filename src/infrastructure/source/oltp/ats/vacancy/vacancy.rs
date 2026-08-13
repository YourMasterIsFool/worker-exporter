use std::sync::Arc;

use serde_json::Value;
use sqlx::{Postgres, QueryBuilder};
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
    domain::oltp::ats::vacancy::AtsVacancy,
    infrastructure::{db::ats_client::AtsClient, source::oltp::ats::types::AtsIngestionTable},
};

pub struct AtsVacancySource {
    pub pool: Arc<AtsClient>,
}

impl BuildTableTrait<AtsIngestionTable> for AtsVacancySource {
    fn table(&self) -> AtsIngestionTable {
        AtsIngestionTable::Vacancy
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
            "requirement_stage_template_id",
            "recruitment_form_template_id",
            "pre_onboarding_template_id",
            "pre_onboarding_template_meta",
            "snapshot",
            "active_publish_keys",
            "created_by",
            "permission_keys",
            "job_functions",
            "job_function_ids",
            "branch_id",
            "branch_meta",
            "organization_id",
            "organization_meta",
            "expired_date",
            "employment_status_id",
            "employment_status_meta",
            "work_placement_type_id",
            "work_placement_type_meta",
            "work_location_id",
            "work_location_meta",
            "province_id",
            "province_meta",
            "city_id",
            "city_meta",
            "job_position_id",
            "job_position_meta",
            "is_confidential",
            "headcount_quota",
            "salary_min",
            "salary_max",
            "is_hide_salary",
            "currency_code",
            "is_no_expired_date",
            "version",
            "verified_status",
            "status::text AS status",
            "completion_status",
            "\"currentHired\" AS current_hired",
            "is_locked",
            "total_hired",
            "publish_at",
            "sla",
            "logging_id",
            "keywords",
            "filterable_attribute",
        ]
    }
}

#[async_trait::async_trait]
impl DataFetcherJson for AtsVacancySource {
    async fn fetch(&self, filters: Option<Vec<WhereField>>) -> anyhow::Result<Vec<Value>> {
        let mut query = QueryBuilder::<Postgres>::new(format!(
            "SELECT {} FROM {}",
            self.columns().join(", "),
            self.table().table_name()
        ));
        fetch_data_json::<AtsVacancy>(&mut query, filters, &self.pool.pool, None).await
    }
}

#[async_trait::async_trait]
impl SourceIncrementalExtract for AtsVacancySource {
    async fn count_updated_after(
        &self,
        updated_after: Option<chrono::NaiveDateTime>,
    ) -> anyhow::Result<i64> {
        let mut query = sqlx::query_scalar("SELECT COUNT(*) FROM vacancy.vacancy");
        if let Some(updated_after) = updated_after {
            query =
                sqlx::query_scalar("SELECT COUNT(*) FROM vacancy.vacancy WHERE updated_at > $1")
                    .bind(updated_after.and_utc());
        }
        Ok(query.fetch_one(&self.pool.pool).await?)
    }

    async fn extract_updated_after(
        &self,
        updated_after: Option<chrono::NaiveDateTime>,
        limit: i64,
    ) -> anyhow::Result<(Vec<Value>, Option<Uuid>)> {
        let mut query = QueryBuilder::<Postgres>::new(format!(
            "SELECT {} FROM {}",
            self.columns().join(", "),
            self.table().table_name()
        ));
        if let Some(updated_after) = updated_after {
            query
                .push(" WHERE updated_at > ")
                .push_bind(updated_after.and_utc());
        }
        query
            .push(" ORDER BY updated_at, id LIMIT ")
            .push_bind(limit);

        let rows = query
            .build_query_as::<AtsVacancy>()
            .fetch_all(&self.pool.pool)
            .await?;
        let next_cursor = rows.last().map(|row| row.id);
        Ok((
            rows.iter()
                .map(serde_json::to_value)
                .collect::<Result<Vec<_>, _>>()?,
            next_cursor,
        ))
    }

    async fn extract(
        &self,
        cursor: Option<Uuid>,
        limit: i64,
        _strategy: &LoadEnum,
    ) -> anyhow::Result<(Vec<Value>, Option<Uuid>)> {
        let mut query = QueryBuilder::<Postgres>::new(format!(
            "SELECT {} FROM {}",
            self.columns().join(", "),
            self.table().table_name()
        ));
        if let Some(cursor) = cursor {
            query.push(" WHERE id > ").push_bind(cursor);
        }
        query.push(" ORDER BY id LIMIT ").push_bind(limit);

        let rows = query
            .build_query_as::<AtsVacancy>()
            .fetch_all(&self.pool.pool)
            .await?;
        let next_cursor = rows.last().map(|row| row.id);
        Ok((
            rows.iter()
                .map(serde_json::to_value)
                .collect::<Result<Vec<_>, _>>()?,
            next_cursor,
        ))
    }
}
