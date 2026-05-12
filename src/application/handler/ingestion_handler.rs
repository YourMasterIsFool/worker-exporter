use std::sync::Arc;

use anyhow::{Ok, Result};
use tracing::{info, warn};

use crate::{
    app_state::AppState,
    application::{
        pipelines::ingestion_pipeline::IngestionPipeline, runner::ingestion_runner::IngestionRunner,
    },
    core::traits::{
        consumer::{Consumer, ConsumerMessage},
        handler::{JobHandler, JobStruct},
    },
};

pub struct IngestionHandler {
    pub consumer: Arc<dyn Consumer<Message = ConsumerMessage> + Send + Sync>,
    pub app_state: Arc<AppState>,
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
                let job: JobStruct = serde_json::from_str(&msg.payload)?;
                info!("payload {:?}", msg);
                let source = match self.app_state.source_registry.get(&job.source) {
                    anyhow::Result::Ok(source) => source,
                    Err(err) => {
                        warn!("Failed to get source: {}", err);
                        continue;
                    }
                };
                let ingestion_pipeline = IngestionPipeline { source: source };
                let anyhow::Result::Ok(loader) =
                    self.app_state.incremental_load_registry.get(&job.source)
                else {
                    warn!(
                        "Gagal mengambil loader untuk '{}', melewati job...",
                        job.source
                    );
                    continue;
                };

                IngestionRunner::run(&ingestion_pipeline, &loader).await?;
                self.consumer.ack(&msg.id).await?;
            }
        }
    }
}
