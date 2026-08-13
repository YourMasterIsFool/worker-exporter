use serde::{Deserialize, Serialize};

use crate::core::types::ValidationConfig;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ExporterColumn {
    pub name: String,
    #[serde(default)]
    pub column: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fetcher: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fetcher_field_filter: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fetcher_field_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fetcher_key: Option<String>,
    #[serde(default)]
    pub is_cache: bool,
    #[serde(default)]
    pub relation: Option<String>,
    #[serde(default)]
    pub custom_key: Option<String>,
    #[serde(default)]
    pub transform: Option<String>,
    #[serde(default, rename = "default")]
    pub default_value: Option<String>,
}
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ExporterConfig {
    pub fields: Vec<ExporterColumn>,
    pub dataset: String,
    pub format: String,
    pub template_code: String,
    pub structure_template_code: String,
    pub redirected_url: String,
    pub chunk: i64,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DynamicExportCursor {
    pub column: String,
    #[serde(default = "default_cursor_direction")]
    pub direction: String,
}

fn default_cursor_direction() -> String {
    "asc".into()
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DynamicExportSource {
    pub database: String,
    pub schema: String,
    pub table: String,
    #[serde(default)]
    pub cursor: Option<DynamicExportCursor>,
    #[serde(default)]
    pub tenant_field: Option<String>,
    #[serde(default)]
    pub tenant_operator: Option<String>,
    #[serde(default)]
    pub where_fields: Vec<crate::core::types::WhereField>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DynamicExportRelation {
    pub name: String,
    #[serde(default)]
    pub depends_on: Option<String>,
    pub source: DynamicExportSource,
    pub local_key: String,
    pub foreign_key: String,
    pub cardinality: String,
    #[serde(default)]
    pub is_custom: bool,
    #[serde(default)]
    pub is_cache: bool,
    #[serde(default)]
    pub field_key_column: Option<String>,
    #[serde(default)]
    pub field_type_column: Option<String>,
    #[serde(default)]
    pub value_columns: Option<std::collections::HashMap<String, String>>,
    #[serde(default)]
    pub order_by: Option<DynamicExportCursor>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DynamicExportConfig {
    pub dataset: String,
    pub format: String,
    pub fields: Vec<ExporterColumn>,
    pub source: DynamicExportSource,
    #[serde(default)]
    pub relations: Vec<DynamicExportRelation>,
    pub template_code: String,
    pub structure_template_code: String,
    pub redirected_url: String,
    pub chunk: i64,
}

impl DynamicExportConfig {
    pub fn export_config(&self) -> ExporterConfig {
        ExporterConfig {
            fields: self.fields.clone(),
            dataset: self.dataset.clone(),
            format: self.format.clone(),
            template_code: self.template_code.clone(),
            structure_template_code: self.structure_template_code.clone(),
            redirected_url: self.redirected_url.clone(),
            chunk: self.chunk,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct IngestionConfig {
    #[serde(default)]
    pub dataset: Option<String>,
    pub source: String,
    pub loader: String,
    #[serde(default = "default_ingestion_pipeline")]
    pub pipeline: String,
    #[serde(default = "default_ingestion_chunk")]
    pub chunk: i64,
}

fn default_ingestion_pipeline() -> String {
    "ingestion".to_string()
}

fn default_ingestion_chunk() -> i64 {
    500
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct SyncConfig {
    pub sync_aggregate: String,
    pub logging_id: Option<String>,
    pub chunk: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TypeDataEnum {
    #[serde(rename = "TEXT")]
    Text,
    #[serde(rename = "UUID")]
    Uuid,
    #[serde(rename = "DATE")]
    Date,
    #[serde(rename = "NUMBER")]
    Number,
    #[serde(rename = "Boolean")]
    Boolean,
}

impl From<TypeDataEnum> for String {
    fn from(value: TypeDataEnum) -> Self {
        match value {
            TypeDataEnum::Boolean => "BOOLEAN".into(),
            TypeDataEnum::Uuid => "UUID".into(),
            TypeDataEnum::Date => "DATE".into(),
            TypeDataEnum::Number => "NUMBER".into(),
            TypeDataEnum::Boolean => "Boolean".into(),
            TypeDataEnum::Text => "TEXT".into(),
        }
    }
}
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ImporterColumn {
    pub header: String,
    pub column: String,
    pub fetcher: Option<String>,
    #[serde(default)]
    pub template: Option<serde_json::Value>,
    pub type_data: Option<TypeDataEnum>,
    pub is_custom: bool,
    pub fetcher_field_filter: Option<String>,
    pub fetcher_field_key: Option<String>,
    pub fetcher_key: Option<String>,
    pub casting: Option<crate::core::types::CastingEnum>,
    #[serde(
        default,
        alias = "validations",
        alias = "validation_config",
        alias = "validationConfig"
    )]
    pub validation_rules: Option<Vec<ValidationConfig>>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct SequentialTable {
    pub(crate) table: String,
    pub is_custom: bool,
    pub fk_column: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Filterable {
    pub main_column: String,
    pub import_column: String,
    #[serde(default, alias = "isArray")]
    pub is_array: bool,
    #[serde(default, alias = "sourceColumn")]
    pub source_column: Option<String>,
    #[serde(
        default,
        alias = "fieldKey",
        alias = "customFieldKey",
        alias = "custom_field_key"
    )]
    pub field_key: Option<String>,
    #[serde(default, alias = "valueColumn")]
    pub value_column: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ImporterConfig {
    pub fields: Vec<ImporterColumn>,
    pub chunk: i32,
    pub tenant_id: String,
    pub group_id: String,
    pub dataset: String,
    pub auto_sync: bool,
    pub main_filterable: Option<Vec<Filterable>>,
    pub main_searchable: Option<Vec<String>>,
    pub key: String,
    pub data_source_type: String,
    pub pipeline: String,
    pub loader: String,
    #[serde(alias = "main_table")]
    pub master_table: String,
    pub primary_column: String,
    pub sequential_table: Option<Vec<SequentialTable>>,
    pub fetcher_caching: Option<Vec<String>>,

    // start row for reading excel
    // example start from row 1
    pub start_row: Option<u64>,
    pub permission_key: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct OrchestratorJob {
    pub(crate) tenant_id: String,
    pub(crate) email: String,
    pub(crate) actor_id: String,
    pub(crate) permissions_key: Option<Vec<String>>,

    pub(crate) orchestrator_name: String,
    pub(crate) export_config: Option<ExporterConfig>,
    pub(crate) dynamic_export_config: Option<DynamicExportConfig>,
    pub(crate) ingestion_config: Option<IngestionConfig>,
    pub(crate) import_config: Option<ImporterConfig>,
    pub(crate) sync_config: Option<SyncConfig>,
    pub(crate) filters: Option<Vec<crate::core::types::WhereField>>, // pub(crate) fields: Vec<ExportFields>,
}
#[async_trait::async_trait]
pub trait Orchestrator: Send + Sync {
    async fn run(&self, job: &OrchestratorJob, msg_id: &str) -> anyhow::Result<()>;
}
