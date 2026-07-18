use crate::{
    app_state::AppState,
    application::usecases::generated_email_template_usecase::{ExampleValue, TemplateHeader},
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

#[derive(Debug, Deserialize)]
pub struct PayloadGeneratedImportTemplate {
    pub headers: Vec<TemplateHeader>,
    #[serde(default, alias = "exampleValue", alias = "example_values")]
    pub example_value: Vec<ExampleValue>,
    pub payload: serde_json::Value,
}

pub async fn generated_import_template_http(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<PayloadGeneratedImportTemplate>,
) -> impl IntoResponse {
    let usecases = state.usecases.clone();
    let usecase = usecases.generated_email_template_usecase.clone();

    let result = usecase
        .execute(&payload.headers, &payload.example_value, payload.payload)
        .await;

    match result {
        Ok(bytes) => Response::builder()
            .header(
                header::CONTENT_TYPE,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            .header(
                header::CONTENT_DISPOSITION,
                "attachment; filename=\"template_import.xlsx\"",
            )
            .body(Body::from(bytes))
            .unwrap()
            .into_response(),
        Err(err) => AppError::InternalServerError(err.to_string()).into_response(),
    }
}
