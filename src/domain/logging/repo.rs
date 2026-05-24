use tokio::time::error;
use uuid::Uuid;

use crate::domain::logging::entity::{self, LoggingEntity, LoggingStatus};

#[async_trait::async_trait]
pub trait LoggingRepo: Send + Sync {
    async fn store(&self, entity: LoggingEntity) -> anyhow::Result<String>;
    async fn updateCurrentChunk(&self, id: &str, current_chunk: i64) -> anyhow::Result<()>;
    async fn updateProcessedCount(&self, id: &str, processed_count: i64) -> anyhow::Result<()>;
    async fn updateCursor(&self, id: &str, cursor: Option<String>) -> anyhow::Result<()>;
    async fn updateCursorAndProcessedCount(
        &self,
        id: &str,
        cursor: Option<String>,
        processed_count: i64,
    ) -> anyhow::Result<()>;
    async fn markAsCompleted(&self, id: &str) -> anyhow::Result<()>;
    async fn updateStatus(&self, id: &str, status: LoggingStatus) -> anyhow::Result<()>;
    async fn updateError(&self, id: &str, error: Option<String>) -> anyhow::Result<()>;
    async fn updateDownloadUrl(&self, id: &str, url: String) -> anyhow::Result<()>;
}
