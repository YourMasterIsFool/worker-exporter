use std::sync::Arc;

use anyhow::{Ok, Result};

use crate::{
    app_state::AppState,
    application::handler::handler_boostrap::consumer_handler,
    core::traits::{
        consumer::{Consumer, ConsumerMessage},
        handler::JobHandler,
    },
};

pub struct IngestionHandler {
    pub consumer: Arc<dyn Consumer<Message = ConsumerMessage> + Send + Sync>,
    pub app_state: Arc<AppState>,
}

impl IngestionHandler {}
#[async_trait::async_trait]
impl JobHandler for IngestionHandler {
    async fn handler(&self) -> Result<()> {
        consumer_handler(
            self.consumer.clone(),
            self.app_state.orchestrator_registry.clone(),
            "ingestion",
        )
        .await?;

        Ok(())
    }
}
