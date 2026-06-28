use std::any::Any;

#[async_trait::async_trait]
pub trait GrpcExecutor: Send + Sync {
    async fn execute(&self, request: Box<dyn Any + Send>) -> anyhow::Result<Box<dyn Any + Send>>;
}
