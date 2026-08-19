use std::sync::Arc;

use crate::{
    application::aggregate_pipeline_registry::AggregatePipelineRegistry,
    core::traits::orchestrator::{Orchestrator, OrchestratorJob},
    domain::logging::{
        entity::{LoggingEntity, LoggingStatus, LoggingType},
        repo::LoggingRepo,
    },
};

pub struct AggregateOrchestrator {
    pub pipelines: Arc<AggregatePipelineRegistry>,
    pub logging: Arc<dyn LoggingRepo>,
}

#[async_trait::async_trait]
impl Orchestrator for AggregateOrchestrator {
    async fn run(&self, job: &OrchestratorJob, msg_id: &str) -> anyhow::Result<()> {
        let config = job
            .aggregate_config
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("aggregate_config wajib diisi"))?;
        let log_id = self
            .logging
            .store(LoggingEntity {
                id: None,
                created_at: None,
                tenant_id: job.tenant_id.clone(),
                title: job.title.clone(),
                email: job.email.clone(),
                actor_id: job.actor_id.clone(),
                logging_type: LoggingType::Aggregate.to_string(),
                completed_at: None,
                download_url: None,
                s3_key: None,
                status: LoggingStatus::Processing.to_string(),
                error: None,
                additional_info: Some(serde_json::json!({
                    "aggregate_code": config.aggregate_code,
                    "pipeline": config.pipeline,
                })),
                processed_count: 0,
                total_count: 0,
                current_chunk: 0,
                total_chunk: 0,
                cursor: None,
                stream_id: msg_id.to_string(),
                config: Some(serde_json::to_value(job)?),
            })
            .await?;

        let result = self.pipelines.get(&config.pipeline)?.run(config).await;
        match result {
            Ok(processed) => {
                self.logging
                    .update_processed_count(&log_id, processed)
                    .await?;
                self.logging
                    .update_status(&log_id, LoggingStatus::Completed)
                    .await?;
                tracing::info!(
                    log_id = %log_id,
                    aggregate_code = %config.aggregate_code,
                    pipeline = %config.pipeline,
                    processed,
                    "aggregate pipeline completed"
                );
                Ok(())
            }
            Err(error) => {
                self.logging
                    .update_status(&log_id, LoggingStatus::Failed)
                    .await?;
                self.logging
                    .update_error(&log_id, Some(error.to_string()))
                    .await?;
                Err(error)
            }
        }
    }
}
