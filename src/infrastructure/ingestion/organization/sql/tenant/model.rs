use chrono::{DateTime, NaiveDateTime, Utc};
use serde::Serialize;
use sqlx::prelude::FromRow;
use uuid::Uuid;

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
