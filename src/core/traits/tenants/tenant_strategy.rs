#[async_trait::async_trait]
pub trait TenantStrategy: Send + Sync {
    async fn process(&self) -> anyhow::Result<()>;
}
