use std::sync::Arc;

use axum::{extract::State, response::IntoResponse, Json};
use serde::Deserialize;

use crate::{app_state::AppState, errors::AppError};

#[derive(Debug, Deserialize)]
pub struct PayloadLogging {
    pub tenant_id: String,
}

pub async fn list_log_by_tenant_id_http(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<PayloadLogging>,
) -> impl IntoResponse {
    let usecases = state.usecases.clone();
    let usecase = usecases.list_log_by_tenant_id_usceases.clone();
    let log_entries = usecase.execute(&payload.tenant_id).await;

    match log_entries {
        Ok(log_entries) => Ok(Json(log_entries)),
        Err(err) => Err(AppError::InternalServerError(err.to_string())),
    }
}
