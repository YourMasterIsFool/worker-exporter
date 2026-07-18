use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde_json::{json, Value};
use std::sync::Arc;

use crate::app_state::AppState;

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
