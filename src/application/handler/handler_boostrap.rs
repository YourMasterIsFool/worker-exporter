use std::sync::Arc;

use tracing::{error, info};

use crate::{
    app_state::{self, AppState},
    application::orchestrator::orchestrator_registry::{self, OrchestratorRegistry},
    core::traits::{
        consumer::{Consumer, ConsumerMessage},
        handler::JobHandler,
        orchestrator::OrchestratorJob,
        strategy::registry::Registry,
    },
};

pub type HandlerRegistry = Registry<dyn JobHandler>;

async fn build_orchestrator(
    orchestrator_type: &str,
    orchestrator_job: &OrchestratorJob,
    orchestrator_registry: Arc<OrchestratorRegistry>,
    msg: &str,
) -> anyhow::Result<()> {
    let orchestrator = orchestrator_registry.get(orchestrator_type)?;

    orchestrator.run(orchestrator_job, msg).await?;

    Ok(())
}
pub async fn consumer_handler(
    consumer: Arc<dyn Consumer<Message = ConsumerMessage> + Send + Sync>,
    orchestrator_registry: Arc<OrchestratorRegistry>,
    orchestrator_type: &str,
) -> anyhow::Result<()> {
    loop {
        let messages = consumer.consume().await?; // ← block disini

        if messages.is_empty() {
            continue; // timeout, loop lagi
        }

        for msg in messages {
            let job = match serde_json::from_str::<OrchestratorJob>(&msg.payload) {
                Result::Ok(data) => Some(data),
                Err(e) => {
                    error!(msg_id = %msg.id, error = %e, "Invalid payload, skipping");
                    None // Kita tandai sebagai tidak valid
                }
            };

            info!("debug orchestrator job {:?}", job.clone());
            // jika ada error di configuration orchestrator maka orchestrator tidak akan dijalankan
            // dan akan memunculkan log lalu consumer di ack

            if let Some(job) = job {
                match build_orchestrator(
                    orchestrator_type,
                    &job,
                    orchestrator_registry.clone(),
                    &msg.id,
                )
                .await
                {
                    Result::Ok(_) => {}
                    Err(err) => {
                        error!("{err}");
                    }
                }
            }

            consumer.ack(&msg.id).await?;
        }
    }
}
