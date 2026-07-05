use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Postgres, QueryBuilder};
use tracing::instrument;
// Untuk jsonb
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
        db::{ats_client::AtsClient, hris_client::HrisClient},
        ingestion::hris::types::HrisIngestionTable,
    },
};
// Untuk numeric

#[derive(Debug, FromRow, Serialize, Deserialize)]
pub struct IngestionHrisAttedanceLocations {
    pub id: String,
    // pub updated_at: NaiveDateTime,
    // pub created_at: NaiveDateTime,
    // pub deleted_at: Option<NaiveDateTime>,
    // pub tenant_id: Uuid,
    // pub group_id: Uuid,
    pub name: String,
    // pub description: Option<String>,
    // pub requirement_stage_template_id: Option<Uuid>,
    // pub recruitment_form_template_id: Option<Uuid>,
    // pub pre_onboarding_template_id: Option<Uuid>,
    // pub snapshot: Option<JsonValue>,
    // pub active_publish_keys: Option<Vec<String>>,
    // pub created_by: Option<Uuid>,
    // pub permission_keys: Option<Vec<String>>,
    // pub job_functions: Option<JsonValue>,
    // pub expired_date: Option<NaiveDateTime>,
    // pub is_confidential: bool,
    // pub headcount_quota: i32,
    // pub salary_min: Option<i64>,
    // pub salary_max: Option<i64>,
    // pub is_hide_salary: bool,
    // pub verified_status: Option<JsonValue>,
    // pub status: String, // Bisa diganti dengan Enum Rust
    // pub is_locked: bool,
    // pub publish_at: Option<NaiveDateTime>,
    // pub job_position_id: Option<Uuid>,
    // pub job_position_meta: Option<JsonValue>,
    // pub organization_id: Option<Uuid>,
    // pub organization_meta: Option<JsonValue>,
    // pub employment_status_id: Option<Uuid>,
    // pub employment_status_meta: Option<JsonValue>,
    // pub work_placement_type_id: Option<Uuid>,
    // pub work_placement_type_meta: Option<JsonValue>,
    // pub work_location_id: Option<Uuid>,
    // pub work_location_meta: Option<JsonValue>,
    // pub total_hired: Option<i32>,
    // pub keywords: Option<Vec<String>>,
    // pub filterable_attribute: Option<JsonValue>,
    // pub sla: Option<i32>,
    // pub province_id: Option<Uuid>,
    // pub province_meta: Option<JsonValue>,
    // pub city_id: Option<Uuid>,
    // pub city_meta: Option<JsonValue>,
    // pub completion_status: Option<String>,
    // pub current_hired: i32, // Mapping dari "currentHired"
}

pub struct IngestionHrisAttedanceLocationsSource {
    pub pool: Arc<HrisClient>,
}

impl BuildTableTrait<HrisIngestionTable> for IngestionHrisAttedanceLocationsSource {
    fn table(&self) -> HrisIngestionTable {
        HrisIngestionTable::AttedanceLocations
    }

    fn columns(&self) -> &'static [&'static str] {
        &[
            "id",
            // "updated_at",
            // "created_at",
            // "deleted_at",
            // "tenant_id",
            // "group_id",
            "name",
            // "description",
            // "requirement_stage_template_id",
            // "recruitment_form_template_id",
            // "pre_onboarding_template_id",
            // "snapshot",
            // "active_publish_keys",
            // "created_by",
            // "permission_keys",
            // "job_functions",
            // "expired_date",
            // "is_confidential",
            // "headcount_quota",
            // "salary_min",
            // "salary_max",
            // "is_hide_salary",
            // "verified_status",
            // "status",
            // "is_locked",
            // "publish_at",
            // "job_position_id",
            // "job_position_meta",
            // "organization_id",
            // "organization_meta",
            // "employment_status_id",
            // "employment_status_meta",
            // "work_placement_type_id",
            // "work_placement_type_meta",
            // "work_location_id",
            // "work_location_meta",
            // "total_hired",
            // "keywords",
            // "filterable_attribute",
            // "sla",
            // "province_id",
            // "province_meta",
            // "city_id",
            // "city_meta",
            // "completion_status",
            // "\"currentHired\"",
        ]
    }
}

#[async_trait::async_trait]
impl DataFetcherJson for IngestionHrisAttedanceLocationsSource {
    #[instrument(skip(self), fields(types = "attendance locations data fetcher"))]
    async fn fetch(
        &self,
        filters: Option<Vec<WhereField>>,
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        let sql = format!(
            "SELECT {} FROM {}",
            self.columns().join(", "),
            self.table().table_name()
        );

        let mut qb = QueryBuilder::new(&sql);

        let f_list = match filters.clone() {
            Some(f) if !f.is_empty() => f,
            _ => return Ok(vec![]),
        };

        qb.push(" WHERE ");

        for (i, filter) in f_list.iter().enumerate() {
            if i > 0 {
                qb.push(" AND ");
            }

            qb.push(&filter.field);

            // 1. CEK OTOMATIS: Jika `values` ada isinya, langsung pakai IN
            if let Some(vals) = &filter.values {
                if !vals.is_empty() {
                    qb.push(" IN (");
                    let mut sep = qb.separated(", ");
                    for val in vals {
                        // BINDING LANGSUNG SEBAGAI STRING
                        // Menghindari error "character varying = uuid"
                        sep.push_bind(val.clone());
                    }
                    qb.push(")");

                    continue;
                }
            }

            // 2. FALLBACK: Jika `values` kosong/None, gunakan `value` tunggal
            if let Some(val) = &filter.value {
                qb.push(format!(" {} ", filter.operator));

                // BINDING LANGSUNG SEBAGAI STRING
                // Menghindari error "character varying = uuid"
                qb.push_bind(val.clone());
            }
        }

        let parse_result = fetch_data_json::<IngestionHrisAttedanceLocations>(
            &mut qb,
            None,
            &self.pool.pool,
            None,
        )
        .await;

        match parse_result {
            Ok(result) => {
                tracing::debug!("employee source {:?}", result.clone());
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
impl SourceIncrementalExtract for IngestionHrisAttedanceLocationsSource {
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
            .build_query_as::<IngestionHrisAttedanceLocations>()
            .fetch_all(&self.pool.pool)
            .await?;
        let last_cursor_string = result.last().map(|last| last.id.clone());
        let last_cursor: Option<Uuid> = last_cursor_string.map(|id| id.parse().unwrap());

        let parse_result = result
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<serde_json::Value>, _>>()?;

        tracing::debug!(" candidate source {:?}", parse_result.clone());
        Ok((parse_result, last_cursor))
    }
}
