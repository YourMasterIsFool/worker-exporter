use std::sync::Arc;

use anyhow::{Ok, Result};
use tracing::{error, info, warn};

use crate::{
    app_state::AppState,
    application::{
        pipelines::ingestion_pipeline::IngestionPipeline, runner::loader_runner::LoaderRunner,
    },
    core::traits::{
        consumer::{Consumer, ConsumerMessage},
        handler::{JobHandler, JobStruct},
        orchestrator::{Orchestrator, OrchestratorJob},
    },
    domain::logging::entity::{LoggingEntity, LoggingStatus, LoggingType},
    errors::AppError,
};

pub struct IngestionHandler {
    pub consumer: Arc<dyn Consumer<Message = ConsumerMessage> + Send + Sync>,
    pub app_state: Arc<AppState>,
}

impl IngestionHandler {
    async fn build_orchestrator(
        &self,
        orchestrator_job: &OrchestratorJob,
        msg: &str,
    ) -> Result<()> {
        let orchestrator = self
            .app_state
            .orchestrator_registry
            .get(&orchestrator_job.orchestrator_type)?;

        orchestrator.run(orchestrator_job, msg).await?;

        Ok(())
    }
}
#[async_trait::async_trait]
impl JobHandler for IngestionHandler {
    async fn handler(&self) -> Result<()> {
        loop {
            let messages = self.consumer.consume().await?; // ← block disini

            if messages.is_empty() {
                continue; // timeout, loop lagi
            }

            for msg in messages {
                let job = match serde_json::from_str::<JobStruct>(&msg.payload) {
                    Result::Ok(data) => Some(data),
                    Err(e) => {
                        error!(msg_id = %msg.id, error = %e, "Invalid payload, skipping");
                        None // Kita tandai sebagai tidak valid
                    }
                };

                if let Some(job) = job {
                    let orchestrator_job = OrchestratorJob {
                        limit: 100,
                        source: job.source.clone(),
                        tenant_id: job.tenant.clone().unwrap_or_default(),
                        orchestrator_type: "export".into(),
                    };

                    info!("debug orchestrator job {:?}", orchestrator_job.clone());

                    // jika ada error di configuration orchestrator maka orchestrator tidak akan dijalankan
                    // dan akan memunculkan log lalu consumer di ack

                    match self.build_orchestrator(&orchestrator_job, &msg.id).await {
                        Result::Ok(_) => {}
                        Err(err) => {
                            error!("{err}");
                        }
                    }
                }

                self.consumer.ack(&msg.id).await?;
            }
        }
    }
}
