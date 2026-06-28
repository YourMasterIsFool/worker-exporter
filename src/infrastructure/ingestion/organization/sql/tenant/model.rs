use chrono::NaiveDateTime;
use serde::Serialize;
use sqlx::prelude::FromRow;
use uuid::Uuid;

use crate::domain::ingestion::organization::tenant::TenantIngestionEntity;

#[derive(Serialize, FromRow, Debug)]
pub struct TenantModel {
    pub id: Uuid,
    pub tenant_code: String,
    pub tenant_name: String,
    pub tenant_level_id: Uuid,
    pub status: Option<String>,
    pub tenant_description: Option<String>,
    pub tenant_vision: Option<String>,
    pub created_at: NaiveDateTime,
    pub deleted_at: Option<NaiveDateTime>,
}

impl From<TenantModel> for TenantIngestionEntity {
    fn from(model: TenantModel) -> Self {
        TenantIngestionEntity {
            id: model.id.to_string(),
            tenant_code: model.tenant_code,
            tenant_name: model.tenant_name,
            tenant_level_id: model.tenant_level_id.to_string(),
            status: model.status,
            tenant_description: model.tenant_description,
            tenant_vision: model.tenant_vision,
            created_at: model.created_at,
            deleted_at: model.deleted_at,
        }
    }
}
