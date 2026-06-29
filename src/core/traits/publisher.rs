#[async_trait::async_trait]
pub trait Publisher: Send + Sync {
    async fn publish(&self, stream: &str, payload: serde_json::Value) -> anyhow::Result<()>;
}
