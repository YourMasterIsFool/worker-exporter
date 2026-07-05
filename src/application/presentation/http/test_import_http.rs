use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde_json::{json, Value};
use std::sync::Arc;

use crate::app_state::AppState;

const PAYLOAD_TEMPLATE: &str = r#"{
  "tenant_id": "019e95a5-f5f3-7dd8-952b-0ce16845d9bd",
  "email": "verrandyb@gmail.com",
  "actor_id": "vearara",
  "limit": 1000,
  "orchestrator_name": "import",
  "export_config": null,
  "ingestion_config": null,
  "import_config": {
    "tenant_id": "019e95a5-f5f3-7dd8-952b-0ce16845d9bd",
    "group_id": "019e95a5-f5f3-7dd8-952b-0ce16845d9bd",
    "dataset": "dynamic_dataset",
    "master_table": "vacancy",
    "sequential_table": [
      { "table": "vacancy_custom_field", "is_custom": true },
      { "table": "vacancy_pics", "is_custom": false },
      { "table": "vacancy_stage", "is_custom": false },
      { "table": "vacancy_requirement", "is_custom": false }
    ],
    "key": "v6/HRMS/019c6a92-5b74-7af0-8d7d-0d97d28a74b3/document/20260702-e7d662bc-b998-4aa5-ad08-5f7e4d1cfd5b-template_excel.xlsx",
    "data_source_type": "buffer",
    "pipeline": "vacancy_import_pipeline",
    "fields": [
      { "header": "no_RR", "column": "vacancy_custom_field.noRR", "fetcher": null, "type_data": "TEXT", "is_custom": true },
      { "header": "position_name", "column": "vacancy.job_position_id|vacancy.job_position_meta", "fetcher": "JOB_POSITION", "type_data": "UUID", "is_custom": false },
      { "header": "position_alias", "column": "vacancy.name", "fetcher": null, "type_data": "TEXT", "is_custom": false },
      { "header": "pic", "column": "vacancy_pics.employee_id", "fetcher": "EMPLOYEE", "type_data": "UUID", "is_custom": false },
      { "header": "district", "column": "vacancy_custom_field.districtId", "fetcher": "MASTER_GEO", "type_data": "UUID", "is_custom": true },
      { "header": "region", "column": "vacancy_custom_field.regionId", "fetcher": "MASTER_ITEMS", "type_data": "UUID", "is_custom": true },
      { "header": "province", "column": "vacancy.province_id|vacancy.province_meta|vacancy_custom_fields.province_id", "fetcher": "MASTER_GEO", "type_data": "UUID", "is_custom": false },
      { "header": "city", "column": "vacancy.city_id|vacancy.city_meta|vacancy_custom_fields.city_id", "fetcher": "MASTER_GEO", "type_data": "UUID", "is_custom": false },
      { "header": "work_location_group", "column": "vacancy_custom_field.workLocationGroupId", "fetcher": "MASTER_ITEMS", "type_data": "UUID", "is_custom": true },
      { "header": "work_location", "column": "vacancy.work_location_id|vacancy.work_location_meta", "fetcher": "hris_attendance_locations_fetcher", "type_data": "UUID", "is_custom": false },
      { "header": "employee_status", "column": "vacancy.employment_status_id|vacancy.employment_status_meta", "fetcher": "EMPLOYMENT_STATUS", "type_data": "UUID", "is_custom": false },
      { "header": "job_grade", "column": "vacancy_custom_field.jobGradeId", "fetcher": "JOB_GRADE", "type_data": "UUID", "is_custom": true },
      { "header": "job_level", "column": "vacancy_custom_field.jobLevelId", "fetcher": "JOB_LEVEL", "type_data": "UUID", "is_custom": true },
      { "header": "organization_unit", "column": "vacancy.organization_id|vacancy.organization_meta", "fetcher": "ORGANIZATION", "type_data": "UUID", "is_custom": false },
      { "header": "vacancy_status", "column": "vacancy_custom_field.vacancyType", "fetcher": null, "type_data": "TEXT", "is_custom": true },
      { "header": "approve_date", "column": "vacancy_custom_field.approveDate", "fetcher": null, "type_data": "DATE", "is_custom": true },
      { "header": "job_objective", "column": "vacancy_custom_field.jobDescriptionObjective", "fetcher": null, "type_data": "TEXT", "is_custom": true },
      { "header": "responsibility", "column": "vacancy_custom_field.taskResponsibilityOther", "fetcher": null, "type_data": "TEXT", "is_custom": true },
      { "header": "reason", "column": "vacancy_custom_field.reasonId", "fetcher": "REASON", "type_data": "UUID", "is_custom": true },
      { "header": "sla", "column": "vacancy_custom_field.sla", "fetcher": null, "type_data": "UUID", "is_custom": true },
      { "header": "total_vacan", "column": "vacancy.total_vacan", "fetcher": null, "type_data": "NUMBER", "is_custom": false },
      { "header": "stage_template", "column": "vacancy.requirement_stage_template_id", "fetcher": "STAGE_TEMPLATE", "type_data": "TEXT", "is_custom": false },
      { "header": "requirement_template", "column": "vacancy.recruitment_form_template_id", "fetcher": "REQUIREMENT_TEMPLATE", "type_data": "TEXT", "is_custom": false }
    ]
  },
  "filters": []
}"#;

pub async fn test_import_http(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<Value>, // Menerima payload langsung dari body request
) -> impl IntoResponse {
    match state
        .publisher
        .publish("stream:import", payload.clone())
        .await
    {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({ "status": "published", "payload": payload })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("gagal publish ke stream: {e}") })),
        )
            .into_response(),
    }
}
