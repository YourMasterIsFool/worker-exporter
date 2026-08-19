use std::sync::Arc;

use anyhow::{bail, Result};
use tracing::info;

use crate::{
    application::{
        loader_registry::LoaderRegistry, pipelines::pipeline_registry::PipelineRegistry,
    },
    core::traits::{
        load::IncrementalLoadPersitence,
        orchestrator::{Orchestrator, OrchestratorJob},
        pipeline::pipeline_trait::PipelineTrait,
    },
};

pub struct IngestionOrchestrator {
    pub pipeline: Arc<PipelineRegistry>,
    pub loader: Arc<LoaderRegistry>,
}

#[async_trait::async_trait]
impl Orchestrator for IngestionOrchestrator {
    async fn run(&self, job: &OrchestratorJob, msg_id: &str) -> Result<()> {
        let config = job
            .ingestion_config
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("ingestion_config is required"))?;
        if config.chunk <= 0 {
            bail!("ingestion_config.chunk must be positive");
        }

        let pipeline = self.pipeline.get(&config.pipeline)?;
        let loader = self.loader.get(&config.loader)?;
        let max_olap_updated_at = loader.fetch_max_source_updated_at().await?;
        let total_data = pipeline
            .count_new_data(&config.source, max_olap_updated_at)
            .await?;
        if total_data == 0 {
            pipeline.after_load(&config.loader).await?;
            info!(%msg_id, source = %config.source, loader = %config.loader, "ingestion skipped: no newer source rows");
            return Ok(());
        }

        // ponytail: loads the full delta at once; add an (updated_at, id) cursor when a delta outgrows memory.
        let (_, rows) = pipeline
            .run(None, total_data, &config.source, max_olap_updated_at)
            .await?;
        let loaded = loader.load(&rows).await?;
        pipeline.after_load(&config.loader).await?;

        info!(%msg_id, source = %config.source, loader = %config.loader, loaded, "ingestion completed");
        Ok(())
    }
}
