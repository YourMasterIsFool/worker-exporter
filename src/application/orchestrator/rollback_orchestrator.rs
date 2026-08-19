use std::sync::Arc;

use crate::{
    application::pipelines::rollback_pipeline_registry::RollbackPipelineRegistry,
    core::traits::orchestrator::{Orchestrator, OrchestratorJob},
    domain::logging::{
        entity::{LoggingEntity, LoggingStatus, LoggingType},
        repo::LoggingRepo,
    },
};

pub struct RollbackOrchestrator {
    pub pipeline: Arc<RollbackPipelineRegistry>,
    pub logging: Arc<dyn LoggingRepo>,
}

#[async_trait::async_trait]
impl Orchestrator for RollbackOrchestrator {
    async fn run(&self, job: &OrchestratorJob, msg_id: &str) -> anyhow::Result<()> {
        let config = job
            .rollback_config
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("rollback_config wajib diisi"))?;
        if config.logging_id.trim().is_empty() {
            anyhow::bail!("rollback_config.logging_id wajib diisi");
        }

        let rollback_log_id = self
            .logging
            .store(LoggingEntity {
                id: None,
                created_at: None,
                tenant_id: job.tenant_id.clone(),
                title: job.title.clone(),
                email: job.email.clone(),
                actor_id: job.actor_id.clone(),
                logging_type: LoggingType::Rollback.to_string(),
                completed_at: None,
                download_url: None,
                s3_key: None,
                status: LoggingStatus::Processing.to_string(),
                error: None,
                additional_info: Some(serde_json::json!({
                    "target_logging_id": config.logging_id,
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

        let result = self
            .pipeline
            .get(&config.pipeline)?
            .rollback(&config.logging_id, &config.loaders)
            .await;
        match result {
            Ok(deleted) => {
                self.logging
                    .update_processed_count(&rollback_log_id, deleted)
                    .await?;
                self.logging
                    .update_status(&rollback_log_id, LoggingStatus::Completed)
                    .await?;
                tracing::info!(
                    rollback_log_id = %rollback_log_id,
                    target_logging_id = %config.logging_id,
                    deleted,
                    "rollback completed"
                );
                Ok(())
            }
            Err(error) => {
                self.logging
                    .update_status(&rollback_log_id, LoggingStatus::Failed)
                    .await?;
                self.logging
                    .update_error(&rollback_log_id, Some(error.to_string()))
                    .await?;
                Err(error)
            }
        }
    }
}
