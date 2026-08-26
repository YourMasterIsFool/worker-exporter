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
pub struct AtsVacancyTemporaryModel {
    pub id: Uuid,
    pub updated_at: Option<NaiveDateTime>,
    pub created_at: Option<NaiveDateTime>,
    pub deleted_at: Option<NaiveDateTime>,
    pub tenant_id: Option<Uuid>,
    pub group_id: Option<Uuid>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub requirement_stage_template_id: Option<Uuid>,
    pub recruitment_form_template_id: Option<Uuid>,
    pub pre_onboarding_template_id: Option<Uuid>,
    pub snapshot: Option<Value>,
    pub active_publish_keys: Option<Vec<String>>,
    pub created_by: Option<Uuid>,
    pub permission_keys: Option<Vec<String>>,
    pub job_functions: Option<Value>,
    pub job_function_ids: Option<Vec<Uuid>>,
    pub expired_date: Option<NaiveDateTime>,
    pub is_confidential: Option<bool>,
    pub headcount_quota: Option<i32>,
    pub salary_min: Option<i64>,
    pub salary_max: Option<i64>,
    pub is_hide_salary: Option<bool>,
    pub verified_status: Option<Value>,
    pub status: Option<String>,
    pub is_locked: Option<bool>,
    pub publish_at: Option<NaiveDateTime>,
    pub job_position_id: Option<Uuid>,
    pub job_position_meta: Option<Value>,
    pub organization_id: Option<Uuid>,
    pub organization_meta: Option<Value>,
    pub employment_status_id: Option<Uuid>,
    pub employment_status_meta: Option<Value>,
    pub work_placement_type_id: Option<Uuid>,
    pub work_placement_type_meta: Option<Value>,
    pub work_location_id: Option<Uuid>,
    pub work_location_meta: Option<Value>,
    pub total_hired: Option<i32>,
    pub keywords: Option<Vec<String>>,
    pub filterable_attribute: Option<Value>,
    pub sla: Option<i32>,
    pub province_id: Option<Uuid>,
    pub province_meta: Option<Value>,
    pub city_id: Option<Uuid>,
    pub city_meta: Option<Value>,
    pub completion_status: Option<String>,
    pub current_hired: Option<i32>,
}

pub struct AtsVacancyTemporarySource {
    pub pool: Arc<WarehouseClient>,
}

impl BuildTableTrait<WarehouseTemporaryTable> for AtsVacancyTemporarySource {
    fn table(&self) -> WarehouseTemporaryTable {
        WarehouseTemporaryTable::AtsVacancyTemporary
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
            "snapshot",
            "active_publish_keys",
            "created_by",
            "permission_keys",
            "job_functions",
            "job_function_ids",
            "expired_date",
            "is_confidential",
            "headcount_quota",
            "salary_min",
            "salary_max",
            "is_hide_salary",
            "verified_status",
            "status",
            "is_locked",
            "publish_at",
            "job_position_id",
            "job_position_meta",
            "organization_id",
            "organization_meta",
            "employment_status_id",
            "employment_status_meta",
            "work_placement_type_id",
            "work_placement_type_meta",
            "work_location_id",
            "work_location_meta",
            "total_hired",
            "keywords",
            "filterable_attribute",
            "sla",
            "province_id",
            "province_meta",
            "city_id",
            "city_meta",
            "completion_status",
            "\"currentHired\" AS current_hired",
        ]
    }
}

#[async_trait::async_trait]
impl DataFetcherJson for AtsVacancyTemporarySource {
    #[instrument(skip(self), fields(types = "ats vacancy temporary data fetcher"))]
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

        fetch_data_json::<AtsVacancyTemporaryModel>(&mut qb, filters, &self.pool.pool, None).await
    }
}

#[async_trait::async_trait]
impl SourceIncrementalExtract for AtsVacancyTemporarySource {
    #[instrument(skip(self, filters), fields(types = "ats_vacancy_temporary_min_id"))]
    async fn get_min_id(&self, filters: Option<Vec<WhereField>>) -> anyhow::Result<Option<String>> {
        let sql = format!(" SELECT id from {} ", self.table().table_name());
        let mut qb = QueryBuilder::new(&sql);

        tracing::info!("filter min id filters {:?}", filters);
        if let Some(filters) = filters {
            qb.push(" WHERE 1=1 ");
            for field in filters {
                let bind_value = field.value.clone().or_else(|| {
                    field
                        .values
                        .as_ref()
                        .and_then(|values| values.first().cloned())
                });

                let Some(bind_value) = bind_value else {
                    tracing::warn!(
                        field = %field.field,
                        operator = %field.operator,
                        "skip min id filter because value is empty"
                    );
                    continue;
                };

                qb.push(format!(" AND {} {} ", field.field, field.operator))
                    .push_bind(bind_value);
            }
        }

        qb.push(" ORDER BY id ASC limit 1");

        let result = qb
            .build_query_scalar::<Uuid>()
            .fetch_optional(&self.pool.pool)
            .await
            .map_err(|err| {
                tracing::error!("error get min id ats vacancy temporary source {:?}", err);
                err
            })?;

        tracing::info!("result {:?}", result.clone());
        Ok(result.map(|id| id.to_string()))
    }

    #[instrument(
        skip(self, strategy_enum),
        fields(types = "ats_vacancy_temporary_extract", cursor = ?cursor, limit = %limit)
    )]
    async fn extract(
        &self,
        cursor: Option<Uuid>,
        limit: i64,
        strategy_enum: &LoadEnum,
    ) -> anyhow::Result<(Vec<serde_json::Value>, Option<Uuid>)> {
        let sql_query = format!(
            "SELECT {} FROM {}",
            self.columns().join(","),
            self.table().table_name()
        );

        let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(sql_query);
        let mut has_where = false;

        if let LoadEnum::Select(Some(filters), _) = strategy_enum {
            qb.push(" WHERE 1=1");
            has_where = true;

            for field in filters {
                let bind_value = field.value.clone().or_else(|| {
                    field
                        .values
                        .as_ref()
                        .and_then(|values| values.first().cloned())
                });

                let Some(bind_value) = bind_value else {
                    tracing::warn!(
                        field = %field.field,
                        operator = %field.operator,
                        "skip extract filter because value is empty"
                    );
                    continue;
                };

                qb.push(format!(" AND {} {} ", field.field, field.operator))
                    .push_bind(bind_value);
            }
        }

        if let Some(c) = cursor {
            if has_where {
                qb.push(" AND id > ").push_bind(c);
            } else {
                qb.push(" WHERE id > ").push_bind(c);
            }
        }
        qb.push(" ORDER BY id ASC");
        qb.push(" LIMIT ").push_bind(limit);

        let result = qb
            .build_query_as::<AtsVacancyTemporaryModel>()
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
