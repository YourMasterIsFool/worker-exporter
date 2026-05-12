#[async_trait::async_trait]
pub trait ExportPipeline: Send + Sync {
    async fn run(&self) -> anyhow::Result<()>;
}
