use axum::Json;
use chrono::Utc;
use serde_json::{json, Value};

pub async fn healthz_http() -> Json<Value> {
    Json(json!({
        "status": "ok",
        "date": Utc::now().to_rfc3339(),
    }))
}
