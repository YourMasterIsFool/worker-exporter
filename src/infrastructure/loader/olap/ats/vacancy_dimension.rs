use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use chrono::{DateTime, NaiveDateTime, Utc};
use serde_json::Value;
use sqlx::{FromRow, Postgres, QueryBuilder};
use uuid::Uuid;

use crate::{
    core::traits::load::IncrementalLoadPersitence, domain::oltp::ats::vacancy::AtsVacancy,
    infrastructure::db::warehouse_client::WarehouseClient,
};

struct VacancyDimension {
    source: AtsVacancy,
    source_updated_at: DateTime<Utc>,
    source_data: Value,
}

#[derive(FromRow)]
struct CurrentVacancyDimension {
    source_id: Uuid,
    source_updated_at: DateTime<Utc>,
}

const BATCH_SIZE: usize = 500;

impl TryFrom<&Value> for VacancyDimension {
    type Error = serde_json::Error;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        let source: AtsVacancy = serde_json::from_value(value.clone())?;
        let source_updated_at = source.updated_at.unwrap_or(source.created_at);

        Ok(Self {
            source,
            source_updated_at,
            source_data: value.clone(),
        })
    }
}

pub struct AtsVacancyDimensionLoader {
    pub client: Arc<WarehouseClient>,
}

#[async_trait::async_trait]
impl IncrementalLoadPersitence for AtsVacancyDimensionLoader {
    async fn fetch_max_source_updated_at(&self) -> Result<Option<NaiveDateTime>> {
        Ok(sqlx::query_scalar::<_, Option<DateTime<Utc>>>(
            "SELECT MAX(source_updated_at) FROM olap_ats.vacancy_dimensions",
        )
        .fetch_one(&self.client.pool)
        .await?
        .map(|updated_at| updated_at.naive_utc()))
    }

    async fn load(&self, rows: &[Value]) -> Result<i64> {
        if rows.is_empty() {
            return Ok(0);
        }

        let dimensions = rows
            .iter()
            .map(VacancyDimension::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        let mut tx = self.client.pool.begin().await?;
        let mut loaded = 0_i64;

        for chunk in dimensions.chunks(BATCH_SIZE) {
            let source_ids = chunk.iter().map(|row| row.source.id).collect::<Vec<_>>();
            let current = sqlx::query_as::<_, CurrentVacancyDimension>(
                "SELECT source_id, source_updated_at FROM olap_ats.vacancy_dimensions \
                 WHERE source_id = ANY($1) AND is_current FOR UPDATE",
            )
            .bind(&source_ids)
            .fetch_all(&mut *tx)
            .await?
            .into_iter()
            .map(|row| (row.source_id, row.source_updated_at))
            .collect::<HashMap<_, _>>();
            let pending = chunk
                .iter()
                .filter(|row| {
                    current
                        .get(&row.source.id)
                        .is_none_or(|updated_at| *updated_at < row.source_updated_at)
                })
                .collect::<Vec<_>>();
            let existing = pending
                .iter()
                .filter(|row| current.contains_key(&row.source.id))
                .collect::<Vec<_>>();

            if !existing.is_empty() {
                sqlx::query(
                    "UPDATE olap_ats.vacancy_dimensions AS dimension \
                     SET is_current = false, end_from = updates.source_updated_at \
                     FROM UNNEST($1::uuid[], $2::timestamptz[]) \
                        AS updates(source_id, source_updated_at) \
                     WHERE dimension.source_id = updates.source_id AND dimension.is_current",
                )
                .bind(existing.iter().map(|row| row.source.id).collect::<Vec<_>>())
                .bind(
                    existing
                        .iter()
                        .map(|row| row.source_updated_at)
                        .collect::<Vec<_>>(),
                )
                .execute(&mut *tx)
                .await?;
            }

            if pending.is_empty() {
                continue;
            }

            let mut insert = QueryBuilder::<Postgres>::new(
                "INSERT INTO olap_ats.vacancy_dimensions (\
                    id, source_id, source_tenant_id, source_group_id, name, status,\
                    completion_status, source_created_at, source_updated_at, source_deleted_at,\
                    source_data, start_from, is_current\
                 ) ",
            );
            insert.push_values(&pending, |mut builder, row| {
                builder
                    .push_bind(Uuid::now_v7())
                    .push_bind(row.source.id)
                    .push_bind(row.source.tenant_id)
                    .push_bind(row.source.group_id)
                    .push_bind(&row.source.name)
                    .push_bind(&row.source.status)
                    .push_bind(&row.source.completion_status)
                    .push_bind(row.source.created_at)
                    .push_bind(row.source_updated_at)
                    .push_bind(row.source.deleted_at)
                    .push_bind(sqlx::types::Json(row.source_data.clone()))
                    .push_bind(row.source_updated_at)
                    .push_bind(true);
            });
            insert.build().execute(&mut *tx).await?;
            loaded += pending.len() as i64;
        }

        tx.commit().await?;
        Ok(loaded)
    }
}

#[cfg(test)]
mod tests {
    use super::VacancyDimension;

    #[test]
    fn uses_created_at_when_vacancy_has_no_updated_at() {
        let value = serde_json::from_str(
            r#"{
                "id":"018f55c3-4a5d-7d8e-8f9a-b0c1d2e3f401","updated_at":null,
                "created_at":"2026-08-12T00:00:00Z","deleted_at":null,
                "tenant_id":"018f55c3-4a5d-7d8e-8f9a-b0c1d2e3f402","group_id":"018f55c3-4a5d-7d8e-8f9a-b0c1d2e3f403",
                "name":"Backend Engineer","description":null,"requirement_stage_template_id":null,
                "recruitment_form_template_id":null,"pre_onboarding_template_id":null,"pre_onboarding_template_meta":null,
                "snapshot":null,"active_publish_keys":null,"created_by":null,"permission_keys":null,"job_functions":null,
                "job_function_ids":null,"branch_id":null,"branch_meta":null,"organization_id":null,"organization_meta":null,
                "expired_date":null,"employment_status_id":null,"employment_status_meta":null,"work_placement_type_id":null,
                "work_placement_type_meta":null,"work_location_id":null,"work_location_meta":null,"province_id":null,
                "province_meta":null,"city_id":null,"city_meta":null,"job_position_id":null,"job_position_meta":null,
                "is_confidential":false,"headcount_quota":1,"salary_min":null,"salary_max":null,"is_hide_salary":false,
                "currency_code":null,"is_no_expired_date":false,"version":1,"verified_status":null,"status":"DRAFT",
                "completion_status":null,"current_hired":0,"is_locked":false,"total_hired":null,"publish_at":null,
                "sla":null,"logging_id":null,"keywords":null,"filterable_attribute":null
            }"#,
        )
        .unwrap();

        let dimension = VacancyDimension::try_from(&value).unwrap();
        assert_eq!(
            dimension.source_updated_at.to_string(),
            "2026-08-12 00:00:00 UTC"
        );
    }
}
