use ::anyhow::Result;
use uuid::Uuid;

#[async_trait::async_trait]
pub trait IncrementalExtract {
    async fn extract(
        &self,
        cursor: Option<Uuid>,
        limit: i64,
        params: Option<serde_json::Value>,
    ) -> Result<(Vec<serde_json::Value>, Option<Uuid>)>;
}
