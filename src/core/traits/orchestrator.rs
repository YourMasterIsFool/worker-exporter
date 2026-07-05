use serde::{Deserialize, Serialize};

use crate::core::types::WhereField;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ExporterColumn {
    pub name: String,
    pub column: String,
}
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ExporterConfig {
    pub fields: Vec<ExporterColumn>,
    pub dataset: String,
    pub format: String,
    pub template_code: String,
    pub structure_template_code: String,
    pub redirected_url: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct IngestionConfig {
    pub dataset: String,
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
    pub type_data: Option<TypeDataEnum>,
    pub is_custom: bool,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct SequentialTable {
    pub(crate) table: String,
    pub is_custom: bool,
    pub fk_column: String,
}
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ImporterConfig {
    pub fields: Vec<ImporterColumn>,
    pub chunk: i32,
    pub tenant_id: String,
    pub group_id: String,
    pub dataset: String,
    pub key: String,
    pub data_source_type: String,
    pub pipeline: String,
    pub master_table: String,
    pub primary_column: String,
    pub sequential_table: Option<Vec<SequentialTable>>,
    pub fetcher_caching: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct OrchestratorJob {
    pub(crate) tenant_id: String,
    pub(crate) email: String,
    pub(crate) actor_id: String,
    pub(crate) limit: i64,
    pub(crate) orchestrator_name: String,
    pub(crate) export_config: Option<ExporterConfig>,
    pub(crate) ingestion_config: Option<IngestionConfig>,
    pub(crate) import_config: Option<ImporterConfig>,
    pub(crate) filters: Option<Vec<WhereField>>, // pub(crate) fields: Vec<ExportFields>,
}
#[async_trait::async_trait]
pub trait Orchestrator: Send + Sync {
    async fn run(&self, job: &OrchestratorJob, msg_id: &str) -> anyhow::Result<()>;
}
