use crate::core::datasets::dataset_runner::DatasetRunner;

use crate::core::traits::pipeline::export_pipeline::ExportPipeline;
use crate::core::traits::tenants::tenant_strategy::TenantStrategy;
use crate::core::traits::writter::writter_trait::WritterTrait;

use anyhow::Result;

pub struct CandidatePoolPipeline<D, W, T>
where
    D: DatasetRunner + Send + Sync,
    W: WritterTrait + Send + Sync,
    T: TenantStrategy + Send + Sync,
{
    pub tenant: T,
    pub writer: W,
    pub dataset: D,
}

#[async_trait::async_trait]
impl<D, W, T> ExportPipeline for CandidatePoolPipeline<D, W, T>
where
    D: DatasetRunner + Send + Sync,
    W: WritterTrait + Send + Sync,
    T: TenantStrategy + Send + Sync,
{
    async fn run(&self) -> Result<()> {
        Ok(())
    }
}
