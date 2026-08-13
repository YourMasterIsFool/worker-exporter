use std::sync::Arc;

use crate::{
    core::traits::load::LoaderPersitence,
    infrastructure::{db::ats_client::AtsClient, loader::oltp::ats::entity_mapping::VACANCY},
};

use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::types::{JsonValue, Uuid};

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)] // Memudahkan matching dengan JSON frontend/DB
pub struct OltpAtsVacancyLoader {
    pub id: Uuid,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
    pub tenant_id: Uuid,
    pub group_id: Uuid,
    pub name: String,
    pub description: Option<String>,

    // Template IDs
    pub requirement_stage_template_id: Option<Uuid>,
    pub recruitment_form_template_id: Option<Uuid>,
    pub pre_onboarding_template_id: Option<Uuid>,
    pub pre_onboarding_template_meta: Option<JsonValue>,

    pub snapshot: Option<JsonValue>,
    pub active_publish_keys: Option<Vec<String>>,
    pub created_by: Option<Uuid>,
    pub permission_keys: Option<Vec<String>>,
    pub job_functions: Option<JsonValue>,
    pub job_function_ids: Option<Vec<Uuid>>,
    pub branch_id: Option<Uuid>,
    pub branch_meta: Option<JsonValue>,
    pub expired_date: Option<NaiveDateTime>,

    pub is_confidential: bool,
    pub headcount_quota: i32,
    pub salary_min: Option<i64>,
    pub salary_max: Option<i64>,
    pub is_hide_salary: bool,
    pub currency_code: Option<String>,
    pub is_no_expired_date: bool,
    pub version: i32,

    pub verified_status: Option<JsonValue>,
    pub status: String, // Sebaiknya gunakan enum khusus jika sudah ada type-nya di sqlx
    pub is_locked: bool,
    pub publish_at: Option<NaiveDateTime>,

    // Meta Data fields
    pub job_position_id: Option<Uuid>,
    pub job_position_meta: Option<JsonValue>,
    pub organization_id: Option<Uuid>,
    pub organization_meta: Option<JsonValue>,
    pub employment_status_id: Option<Uuid>,
    pub employment_status_meta: Option<JsonValue>,
    pub work_placement_type_id: Option<Uuid>,
    pub work_placement_type_meta: Option<JsonValue>,
    pub work_location_id: Option<Uuid>,
    pub work_location_meta: Option<JsonValue>,

    pub total_hired: Option<i32>,
    pub keywords: Option<Vec<String>>,
    pub filterable_attribute: Option<JsonValue>,
    pub sla: Option<i32>,
    pub province_id: Option<Uuid>,
    pub province_meta: Option<JsonValue>,
    pub city_id: Option<Uuid>,
    pub city_meta: Option<JsonValue>,
    pub completion_status: Option<String>,
    pub logging_id: Option<String>,
    #[sqlx(rename = "currentHired")]
    pub current_hired: i32,
}
pub struct AtsVacancyLoader {
    pub client: Arc<AtsClient>,
}

impl LoaderPersitence for AtsVacancyLoader {
    fn table(&self) -> &'static str {
        VACANCY.table()
    }

    fn columns(&self) -> &'static [&'static str] {
        VACANCY.columns()
    }

    fn conflicted_columns(&self) -> &'static [&'static str] {
        VACANCY.conflicted_columns()
    }
}
