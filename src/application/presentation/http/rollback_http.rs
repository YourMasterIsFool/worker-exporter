use std::sync::Arc;

use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::app_state::AppState;

const DEFAULT_PIPELINE: &str = "CONFIG_ROLLBACK_PIPELINE";

#[derive(Debug, Deserialize)]
pub struct RollbackPayload {
    #[serde(alias = "tenantId")]
    pub tenant_id: String,
    pub email: String,
    #[serde(alias = "actorId")]
    pub actor_id: String,
    #[serde(alias = "loggingId")]
    pub logging_id: String,
    pub loaders: Vec<String>,
    #[serde(default = "default_pipeline")]
    pub pipeline: String,
    #[serde(default)]
    pub title: Option<String>,
}

fn default_pipeline() -> String {
    DEFAULT_PIPELINE.to_string()
}

fn build_rollback_job(payload: RollbackPayload) -> Result<Value, &'static str> {
    if payload.tenant_id.trim().is_empty()
        || payload.email.trim().is_empty()
        || payload.actor_id.trim().is_empty()
        || payload.logging_id.trim().is_empty()
    {
        return Err("tenantId, email, actorId, dan loggingId wajib diisi");
    }
    if payload.loaders.is_empty()
        || payload
            .loaders
            .iter()
            .any(|loader| loader.trim().is_empty())
    {
        return Err("loaders wajib berisi minimal satu loader yang valid");
    }

    Ok(json!({
        "tenant_id": payload.tenant_id,
        "title": payload.title,
        "email": payload.email,
        "actor_id": payload.actor_id,
        "orchestrator_name": "ROLLBACK",
        "rollback_config": {
            "pipeline": payload.pipeline,
            "logging_id": payload.logging_id,
            "loaders": payload.loaders,
        }
    }))
}

pub async fn rollback_http(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<RollbackPayload>,
) -> impl IntoResponse {
    let logging_id = payload.logging_id.clone();
    let job = match build_rollback_job(payload) {
        Ok(job) => job,
        Err(error) => {
            return (StatusCode::BAD_REQUEST, Json(json!({ "error": error }))).into_response()
        }
    };

    match state.publisher.publish("stream:rollback", job).await {
        Ok(()) => (
            StatusCode::ACCEPTED,
            Json(json!({
                "status": "queued",
                "stream": "stream:rollback",
                "logging_id": logging_id
            })),
        )
            .into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("gagal enqueue rollback: {error}") })),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{build_rollback_job, RollbackPayload, DEFAULT_PIPELINE};

    fn payload() -> RollbackPayload {
        RollbackPayload {
            tenant_id: "tenant-1".into(),
            email: "user@example.com".into(),
            actor_id: "actor-1".into(),
            logging_id: "logging-1".into(),
            loaders: vec!["ATS_VACANCY_ROLLBACK".into()],
            pipeline: DEFAULT_PIPELINE.into(),
            title: None,
        }
    }

    #[test]
    fn builds_worker_compatible_job() {
        assert_eq!(
            build_rollback_job(payload()).unwrap(),
            json!({
                "tenant_id": "tenant-1",
                "title": null,
                "email": "user@example.com",
                "actor_id": "actor-1",
                "orchestrator_name": "ROLLBACK",
                "rollback_config": {
                    "pipeline": DEFAULT_PIPELINE,
                    "logging_id": "logging-1",
                    "loaders": ["ATS_VACANCY_ROLLBACK"]
                }
            })
        );
    }

    #[test]
    fn rejects_empty_rollback_fields() {
        let mut payload = payload();
        payload.logging_id = " ".into();

        assert!(build_rollback_job(payload).is_err());
    }
}
