use std::sync::Arc;

use tracing::instrument;

use crate::{
    application::{bulk_loader_registry::BulkLoaderRegistry, dataset::SyncDatasetRegistry},
    core::{
        traits::orchestrator::{Orchestrator, OrchestratorJob},
        types::SyncPayloadDataset,
    },
    domain::logging::{
        entity::{LoggingEntity, LoggingStatus, LoggingType},
        repo::LoggingRepo,
    },
    infrastructure::db::ats_client::AtsClient,
};

pub struct SyncOrchestrator {
    pub sync_dataset: Arc<SyncDatasetRegistry>,
    pub bulk_loader_registry: Arc<BulkLoaderRegistry>,
    pub ats_client: Arc<AtsClient>,
    pub logging: Arc<dyn LoggingRepo>,
}

impl SyncOrchestrator {
    const LOADERS: &'static [&'static str] = &[
        "ATS_VACANCY",
        "ATS_VACANCY_CUSTOM_FIELD",
        "ATS_VACANCY_STAGE",
        "ATS_VACANCY_REQUIREMENT",
        "ATS_VACANCY_PIC",
    ];
}

#[async_trait::async_trait]
impl Orchestrator for SyncOrchestrator {
    #[instrument(skip(self, job), fields(msg_id = %msg_id, tenant_id = %job.tenant_id, types = "sync"))]
    async fn run(&self, job: &OrchestratorJob, msg_id: &str) -> anyhow::Result<()> {
        let Some(sync_config) = &job.sync_config else {
            return Err(anyhow::anyhow!(
                "sync_config wajib diisi untuk sync orchestrator"
            ));
        };

        let logging_entity = LoggingEntity {
            id: None,
            tenant_id: job.tenant_id.clone(),
            email: job.email.clone(),
            actor_id: job.actor_id.clone(),
            logging_type: LoggingType::Sync.to_string(),
            completed_at: None,
            download_url: None,
            status: LoggingStatus::Processing.to_string(),
            error: None,
            additional_info: None,
            processed_count: 0,
            current_chunk: 0,
            total_chunk: 0,
            cursor: None,
            stream_id: msg_id.to_string(),
            config: Some(serde_json::to_value(sync_config.clone())?),
        };

        let logging_id = self.logging.store(logging_entity).await?;
        let dataset = self.sync_dataset.get(&sync_config.dataset)?;
        let payload = SyncPayloadDataset {
            config: sync_config.clone(),
            logging_id,
        };

        let sync_result = async {
            let result = dataset.format(&payload).await?;
            let mut tx = self.ats_client.pool.begin().await?;
            let mut processed_count = 0;

            for loader_name in Self::LOADERS {
                let loader = self.bulk_loader_registry.get(loader_name)?;
                processed_count += loader.load(&mut tx, &result).await?;
            }

            tx.commit().await?;

            anyhow::Ok(processed_count)
        }
        .await;

        match sync_result {
            Ok(processed_count) => {
                self.logging
                    .update_processed_count(msg_id, processed_count)
                    .await?;
                self.logging
                    .update_status(msg_id, LoggingStatus::SyncSuccess)
                    .await?;

                tracing::info!(msg_id = %msg_id, processed_count, "sync dataset formatted");
                Ok(())
            }
            Err(err) => {
                self.logging
                    .update_status(msg_id, LoggingStatus::SyncFailed)
                    .await?;
                self.logging
                    .update_error(msg_id, Some(err.to_string()))
                    .await?;
                Err(err)
            }
        }
    }
}
