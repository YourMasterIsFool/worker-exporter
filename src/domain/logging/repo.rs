use crate::domain::logging::entity::{LoggingEntity, LoggingStatus};

#[async_trait::async_trait]
pub trait LoggingRepo: Send + Sync {
    async fn store(&self, entity: LoggingEntity) -> anyhow::Result<String>;
    async fn update_current_chunk(&self, id: &str, current_chunk: i64) -> anyhow::Result<()>;
    async fn update_processed_count(&self, id: &str, processed_count: i64) -> anyhow::Result<()>;
    async fn update_cursor(&self, id: &str, cursor: Option<String>) -> anyhow::Result<()>;
    async fn update_cursor_and_processed_count(
        &self,
        id: &str,
        cursor: Option<String>,
        processed_count: i64,
    ) -> anyhow::Result<()>;
    async fn mark_as_completed(&self, id: &str) -> anyhow::Result<()>;
    async fn update_status(&self, id: &str, status: LoggingStatus) -> anyhow::Result<()>;
    async fn update_error(&self, id: &str, error: Option<String>) -> anyhow::Result<()>;
    async fn update_download_url(&self, id: &str, url: String) -> anyhow::Result<()>;
    async fn list_log_by_tenant_id(&self, tenant_id: &str) -> anyhow::Result<Vec<LoggingEntity>>;
}
