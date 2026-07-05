use chrono::NaiveDateTime;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct TenantIngestionEntity {
    pub id: String,
    pub tenant_code: String,
    pub tenant_name: String,
    pub tenant_level_id: String,
    pub status: Option<String>,
    pub tenant_description: Option<String>,
    pub tenant_vision: Option<String>,
    pub created_at: NaiveDateTime,
    pub deleted_at: Option<NaiveDateTime>,
}

#[async_trait::async_trait]
pub trait TenantIngestionRepo {
    async fn find_by_ids(&self, tenant_ids: Vec<String>) -> Vec<TenantIngestionEntity>;
}
