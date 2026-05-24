use std::sync::Arc;

use anyhow::Ok;
use tracing::instrument;

use crate::{
    app_state::AppState,
    application::{
        load::LoadRegistry, pipelines::pipeline_registry::PipelineRegistry,
        runner::loader_runner::LoaderRunner, writter::WritterRegistry,
    },
    core::traits::{
        orchestrator::{Orchestrator, OrchestratorJob},
        pipeline::pipeline_trait::PipelineTrait,
        writter::writter_trait::WritterTrait,
    },
    domain::logging::{
        entity::{LoggingEntity, LoggingStatus, LoggingType},
        repo::LoggingRepo,
    },
};

pub struct ExportOrchestrator {
    pub pipeline: Arc<PipelineRegistry>,
    pub writter: Arc<WritterRegistry>,
    pub logging: Arc<dyn LoggingRepo>,
    pub load: Arc<LoadRegistry>,
}

#[async_trait::async_trait]
impl Orchestrator for ExportOrchestrator {
    #[instrument(skip(self, job), fields(msg_id = %msg_id, tenant_id = %job.tenant_id))]
    async fn run(&self, job: &OrchestratorJob, msg_id: &str) -> anyhow::Result<()> {
        // just example
        let ingestion_pipeline = self.pipeline.get("ingestion")?;
        let loader = self.load.get("tenants")?;

        let logging_entity = LoggingEntity {
            id: None,
            logging_type: LoggingType::Export.to_string(),
            completed_at: None,
            download_url: None,
            status: LoggingStatus::Processing.to_string(),
            error: None,
            additional_info: None,
            processed_count: 0,
            current_chunk: 0,
            total_chunk: 0,
            cursor: None,
            stream_id: msg_id.to_string().clone(),
        };

        // create logging first
        self.logging.store(logging_entity).await?;

        // loader first data
        if let Err(err) = LoaderRunner::run(
            ingestion_pipeline.as_ref(),
            loader.as_ref(),
            self.logging.as_ref(),
            msg_id,
        )
        .await
        {
            self.logging
                .updateError(msg_id, Some(err.to_string()))
                .await?;

            return Err(err);
        }

        // export csv first
        //

        Ok(())
    }
}
