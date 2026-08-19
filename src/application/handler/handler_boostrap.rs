use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, error, info};

use crate::{
    application::orchestrator::orchestrator_registry::OrchestratorRegistry,
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
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue; // timeout, loop lagi
        }

        for msg in messages {
            info!(
                msg_id = %msg.id,
                orchestrator_type = orchestrator_type,
                "consumer received message"
            );

            let job = match serde_json::from_str::<OrchestratorJob>(&msg.payload) {
                Result::Ok(data) => Some(data),
                Err(e) => {
                    error!(msg_id = %msg.id, error = %e, "Invalid payload, skipping");
                    None // Kita tandai sebagai tidak valid
                }
            };

            debug!("debug orchestrator job {:?}", job.clone());
            // jika ada error di configuration orchestrator maka orchestrator tidak akan dijalankan
            // dan akan memunculkan log lalu consumer di ack

            if let Some(job) = job {
                let resolved_orchestrator = match orchestrator_type {
                    "EXPORT" if job.dynamic_export_config.is_some() => "DYNAMIC_EXPORT",
                    "IMPORT"
                        if job
                            .import_config
                            .as_ref()
                            .is_some_and(|config| config.dataset == "DYNAMIC_IMPORT_DATASET") =>
                    {
                        "DYNAMIC_IMPORT"
                    }
                    _ => orchestrator_type,
                };
                match build_orchestrator(
                    resolved_orchestrator,
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
