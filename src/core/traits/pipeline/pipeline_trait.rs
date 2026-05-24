use anyhow::Result;
use uuid::Uuid;

#[async_trait::async_trait]
pub trait PipelineTrait: Send + Sync {
    async fn run(
        &self,
        cursor: Option<Uuid>,
        limit: i64,
        source: &str,
    ) -> Result<(Option<Uuid>, Vec<serde_json::Value>)>;
}
