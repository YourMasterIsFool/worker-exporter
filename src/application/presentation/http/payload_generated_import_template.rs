use crate::{
    app_state::AppState, application::usecases::generated_email_template_usecase::ExampleValue,
    errors::AppError,
};
use axum::{
    body::Body,
    extract::State,
    http::{header, Response},
    response::IntoResponse,
    Json,
};
use serde::Deserialize;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct PayloadGeneratedImportTemplate {
    pub headers: Vec<String>,
    #[serde(alias = "tenantId")]
    pub tenant_id: String,
    #[serde(default, alias = "exampleValue", alias = "example_values")]
    pub example_value: Vec<ExampleValue>,
}

pub async fn generated_import_template_http(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<PayloadGeneratedImportTemplate>,
) -> impl IntoResponse {
    let usecases = state.usecases.clone();
    let usecase = usecases.generated_email_template_usecase.clone();
    let file_path = format!("excel/{}.xlsx", Uuid::now_v7());

    let result = usecase
        .execute(
            &payload.headers,
            &payload.example_value,
            &file_path,
            &payload.tenant_id,
        )
        .await;

    // Cek result DULU sebelum baca file
    let generation_ok = result.is_ok();

    let response = match result {
        Ok(_) => match tokio::fs::read(&file_path).await {
            Ok(bytes) => Response::builder()
                .header(
                    header::CONTENT_TYPE,
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
                .header(
                    header::CONTENT_DISPOSITION,
                    "attachment; filename=\"template_excel.xlsx\"",
                )
                .body(Body::from(bytes))
                .unwrap()
                .into_response(),
            Err(e) => AppError::InternalServerError(format!("gagal baca file hasil generate: {e}"))
                .into_response(),
        },
        Err(err) => AppError::InternalServerError(err.to_string()).into_response(),
    };

    // Hapus file HANYA kalau memang sempat dibuat (generation_ok)
    if generation_ok {
        let path = file_path.clone();
        tokio::spawn(async move {
            if let Err(e) = tokio::fs::remove_file(&path).await {
                tracing::warn!("gagal hapus temp file {path}: {e}");
            }
        });
    }

    response
}
