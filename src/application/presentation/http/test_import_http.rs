use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde_json::{json, Value};
use std::sync::Arc;

use crate::app_state::AppState;

fn stream_from_payload(payload: &Value) -> Result<String, &'static str> {
    let job_type = payload
        .get("type")
        .or_else(|| payload.get("orchestrator_name"))
        .and_then(Value::as_str)
        .map(str::to_ascii_lowercase)
        .ok_or("payload.type wajib diisi")?;

    match job_type.as_str() {
        "import" | "export" | "sync" | "ingestion" => Ok(format!("stream:{job_type}")),
        _ => Err("payload.type harus import, export, sync, atau ingestion"),
    }
}

pub async fn test_import_http(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<Value>, // Menerima payload langsung dari body request
) -> impl IntoResponse {
    let stream = match stream_from_payload(&payload) {
        Ok(stream) => stream,
        Err(error) => {
            return (StatusCode::BAD_REQUEST, Json(json!({ "error": error }))).into_response()
        }
    };

    match state.publisher.publish(&stream, payload.clone()).await {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({ "status": "published", "stream": stream, "payload": payload })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("gagal publish ke stream: {e}") })),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::stream_from_payload;

    #[test]
    fn resolves_stream_from_payload_type() {
        assert_eq!(
            stream_from_payload(&json!({"type": "export"})),
            Ok("stream:export".into())
        );
    }
}
