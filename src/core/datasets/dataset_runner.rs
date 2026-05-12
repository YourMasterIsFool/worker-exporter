use anyhow::Result;

#[async_trait::async_trait]
pub trait DatasetRunner: Send + Sync {
    fn name(&self) -> &'static str;
    async fn run(&self) -> Result<()>;
}
