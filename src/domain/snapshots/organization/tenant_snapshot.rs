use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct TenantSnapshot {
    pub tenant_id: String,
    pub tenant_name: String,
    pub tenant_code: String,
    pub id: Option<String>,
}

#[async_trait::async_trait]
pub trait TenantSnapshotQueryRepository {
    async fn find_by_ids(&self, tenant_ids: &[String]) -> anyhow::Result<Vec<TenantSnapshot>>;
    async fn find_last_cursor(&self) -> anyhow::Result<String>;
    async fn find_first_cursor(&self) -> anyhow::Result<String>;
}
