use anyhow::Ok;

use serde::Serialize;
use sqlx::prelude::FromRow;

use crate::{
    domain::snapshots::organization::tenant_snapshot::TenantSnapshot, utils::reformated_value,
};

#[derive(Debug, Clone, FromRow, Serialize)]
pub struct TenantSnapshotModel {
    pub tenant_id: String,
    pub tenant_name: String,
    pub tenant_code: String,
    pub id: Option<String>,
}

impl TryFrom<&serde_json::Value> for TenantSnapshotModel {
    type Error = anyhow::Error;
    fn try_from(value: &serde_json::Value) -> Result<Self, Self::Error> {
        Ok(Self {
            tenant_name: reformated_value("tenant_name", value)?,
            tenant_code: reformated_value("tenant_code", value)?,
            id: None,
            tenant_id: reformated_value("id", value)?,
        })
    }
}

impl From<&TenantSnapshotModel> for TenantSnapshot {
    fn from(value: &TenantSnapshotModel) -> Self {
        Self {
            tenant_id: value.tenant_id.clone(),
            tenant_name: value.tenant_name.clone(),
            tenant_code: value.tenant_code.clone(),
            id: value.id.clone(),
        }
    }
}
