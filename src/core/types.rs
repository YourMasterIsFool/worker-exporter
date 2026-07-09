use std::collections::HashMap;

use calamine::Data;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use crate::core::traits::orchestrator::{ImporterColumn, ImporterConfig, SyncConfig, TypeDataEnum};

#[derive(Debug, Clone, Deserialize, serde::Serialize)]
pub struct WhereField {
    pub field: String,
    pub operator: String,
    pub value: Option<String>,
    pub values: Option<Vec<String>>,
    pub casting: Option<CastingEnum>,
}

#[derive(Debug, Clone, Deserialize, serde::Serialize)]
pub enum CastingEnum {
    Uuid,
    Varchar,
    Enum(String),
}

impl CastingEnum {
    pub fn as_str(&self) -> &str {
        match self {
            CastingEnum::Uuid => "uuid",
            CastingEnum::Varchar => "varchar",
            CastingEnum::Enum(enum_str) => enum_str.as_str(),
        }
    }
}

pub trait Identifierable {
    fn get_id(&self) -> Uuid;
}


// struct buat sync data
#[derive(Debug, Serialize, Clone)]
pub struct SyncPayloadDataset {
    pub config: SyncConfig,
    pub logging_id: String,
}

#[derive(Debug, Serialize, Clone, Deserialize)]
pub struct SourceMappingTable {
    pub columns: Vec<String>,
    pub values: Vec<Vec<serde_json::Value>>,
}

#[derive(Debug, Serialize, Clone)]
pub struct SourceSyncConfig {
    pub sources: HashMap<String, SourceMappingTable>,
}

#[derive(Debug, Serialize, Clone)]
pub struct SycnTableResult {
    pub config: SyncConfig,
    pub sources: SourceSyncConfig,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ImportDatasetConfig {
    pub where_fields: Option<Vec<WhereField>>,
    pub import_config: ImporterConfig,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ImportRow {
    pub field: String,
    pub value: serde_json::Value,
    pub fetcher: Option<String>,
    pub type_data: Option<TypeDataEnum>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ImportRowBulk {
    pub fields: Vec<ImportRow>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ImportBulkRequest {
    pub rows: Vec<ImportRowBulk>,
    pub table: String,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExportRow {
    pub fields: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum TableStrategy {
    Main,
    HasMany,
    Eav,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ImportTableResult {
    pub strategy: TableStrategy,
    pub columns: Vec<String>,
    pub values: Vec<Vec<serde_json::Value>>,
}

impl ExportRow {
    pub fn validate_json<T: serde::Serialize>(
        value: T,
        columns: &[String],
    ) -> anyhow::Result<Self> {
        let parsed_json = serde_json::to_value(value)?;
        let mut result: Vec<String> = vec![];

        if let Some(object) = parsed_json.as_object() {
            for field in columns {
                if let Some(value) = object.get(field) {
                    // 💡 FIX: Ekstrak string tanpa membawa tanda petik dua JSON
                    let formatted_value = match value {
                        serde_json::Value::String(s) => s.clone(),
                        serde_json::Value::Number(n) => n.to_string(),
                        serde_json::Value::Bool(b) => b.to_string(),
                        serde_json::Value::Null => String::from("-"),
                        _ => value.to_string(), // Untuk array atau objek bersarang
                    };
                    result.push(formatted_value);
                } else {
                    result.push(String::from("-"));
                }
            }
        }

        Ok(Self { fields: result })
    }
}
