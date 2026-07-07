use std::sync::Arc;

use crate::{
    app_state::AppState,
    application::handler::handler_boostrap::consumer_handler,
    core::traits::{
        consumer::{Consumer, ConsumerMessage},
        handler::JobHandler,
    },
};

pub struct SyncHandler {
    pub consumer: Arc<dyn Consumer<Message = ConsumerMessage> + Send + Sync>,
    pub app_state: Arc<AppState>,
}

#[async_trait::async_trait]
impl JobHandler for SyncHandler {
    async fn handler(&self) -> anyhow::Result<()> {
        consumer_handler(
            self.consumer.clone(),
            self.app_state.orchestrator_registry.clone(),
            "SYNC",
        )
        .await?;

        Ok(())
    }
}
