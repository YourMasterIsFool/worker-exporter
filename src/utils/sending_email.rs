use std::sync::Arc;

use serde::Serialize;

use crate::core::traits::publisher::Publisher;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")] //
pub struct EmailPublisher {
    pub email: String,
    pub subject: String,
    pub template_id: Option<String>,
    pub template_code: Option<String>,
    pub template_structure_code: Option<String>,
    pub provider: String,
    pub payload: serde_json::Value,
}

pub async fn sending_email(
    publisher: Arc<dyn Publisher>,
    stream: &str,
    payload: &EmailPublisher,
) -> anyhow::Result<()> {
    publisher
        .publish(stream, serde_json::to_value(payload)?)
        .await
}
