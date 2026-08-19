use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use sqlx::types::JsonValue;
use uuid::Uuid;

#[derive(Debug, Deserialize, Serialize, sqlx::FromRow)]
pub struct GeneralOrganization {
    pub id: Uuid,
    pub updated_at: Option<NaiveDateTime>,
    pub created_at: NaiveDateTime,
    pub deleted_at: Option<NaiveDateTime>,
    pub filterable_attribute: Option<JsonValue>,
    pub keywords: Option<Vec<String>>,
    pub tenant_id: Uuid,
    pub group_id: Option<Uuid>,
    pub code: String,
    #[sqlx(rename = "organization_type")]
    #[serde(rename = "type")]
    pub organization_type: String,
    pub name: String,
    pub is_default: bool,
    pub permission_keys: Vec<String>,
    pub additional_fields: JsonValue,
}
