use anyhow::{Context, Ok};
use serde::Serialize;
use sqlx::prelude::FromRow;

use crate::utils::reformated_value;

#[derive(Debug, Clone, FromRow, Serialize)]
pub struct TenantSnapshotModel {
    pub tenant_id: String,
    pub tenant_name: String,
    pub tenant_code: String,
    pub id: Option<String>,
    pub created_at: String,
}

impl TryFrom<&serde_json::Value> for TenantSnapshotModel {
    type Error = anyhow::Error;
    fn try_from(value: &serde_json::Value) -> Result<Self, Self::Error> {
        Ok(Self {
            tenant_name: reformated_value("tenant_name", value)?,
            tenant_code: reformated_value("tenant_code", value)?,
            id: None,
            created_at: reformated_value("created_at", value)?,
            tenant_id: reformated_value("id", value)?,
        })
    }
}
