use std::sync::Arc;

use tracing::instrument;

use crate::{
    application::sync_aggregator_registry::SyncAggregatorRegistry,
    core::{
        traits::orchestrator::{Orchestrator, OrchestratorJob},
        types::SyncPayloadDataset,
    },
    domain::logging::{entity::LoggingStatus, repo::LoggingRepo},
};

pub struct SyncOrchestrator {
    pub sync_aggregator: Arc<SyncAggregatorRegistry>,
    pub logging: Arc<dyn LoggingRepo>,
}

#[async_trait::async_trait]
impl Orchestrator for SyncOrchestrator {
    #[instrument(skip(self, job), fields(msg_id = %msg_id, tenant_id = %job.tenant_id, types = "sync"))]
    async fn run(&self, job: &OrchestratorJob, msg_id: &str) -> anyhow::Result<()> {
        let Some(sync_config) = &job.sync_config else {
            tracing::error!(
                msg_id = %msg_id,
                tenant_id = %job.tenant_id,
                "sync orchestrator received job without sync_config"
            );
            return Err(anyhow::anyhow!(
                "sync_config wajib diisi untuk sync orchestrator"
            ));
        };

        tracing::info!(
            msg_id = %msg_id,
            tenant_id = %job.tenant_id,
            sync_aggregate = %sync_config.sync_aggregate,
            logging_id = ?sync_config.logging_id,
            "sync orchestrator received job"
        );

        let logging_id = match sync_config.logging_id.clone() {
            Some(val) => val,
            None => "".to_string(),
        };
        if logging_id.is_empty() {
            tracing::error!(
                msg_id = %msg_id,
                tenant_id = %job.tenant_id,
                sync_aggregate = %sync_config.sync_aggregate,
                "sync_config.logging_id kosong"
            );
            return Err(anyhow::anyhow!("sync_config.logging_id wajib diisi"));
        }

        let payload = SyncPayloadDataset {
            config: sync_config.clone(),
            logging_id: logging_id.to_string(),
        };

        tracing::info!(
            msg_id = %msg_id,
            tenant_id = %job.tenant_id,
            logging_id = %logging_id,
            sync_aggregate = %sync_config.sync_aggregate,
            chunk = sync_config.chunk,
            "sync orchestrator running"
        );

        let sync_result = async {
            self.logging
                .update_status(&logging_id, LoggingStatus::SyncProgress)
                .await?;
            tracing::info!(
                msg_id = %msg_id,
                logging_id = %logging_id,
                sync_aggregate = %sync_config.sync_aggregate,
                "sync status updated to SYNC_PROGRESS"
            );

            let aggregator = self.sync_aggregator.get(&sync_config.sync_aggregate)?;
            tracing::info!(
                msg_id = %msg_id,
                logging_id = %logging_id,
                sync_aggregate = %sync_config.sync_aggregate,
                "sync aggregator resolved"
            );

            aggregator.run(&payload, &logging_id).await
        }
        .await;

        match sync_result {
            Ok(processed_count) => {
                self.logging
                    .update_processed_count(&logging_id, processed_count)
                    .await?;
                self.logging
                    .update_status(&logging_id, LoggingStatus::SyncSuccess)
                    .await?;

                tracing::info!(msg_id = %msg_id, processed_count, "sync dataset formatted");
                Ok(())
            }
            Err(err) => {
                tracing::error!(
                    msg_id = %msg_id,
                    logging_id = %logging_id,
                    sync_aggregate = %sync_config.sync_aggregate,
                    error = %err,
                    "sync orchestrator failed"
                );
                self.logging
                    .update_status(&logging_id, LoggingStatus::SyncFailed)
                    .await?;
                self.logging
                    .update_error(&logging_id, Some(err.to_string()))
                    .await?;
                Err(err)
            }
        }
    }
}
