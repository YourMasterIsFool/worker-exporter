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
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct IngestionConfig {
    pub dataset: String,
}

#[derive(Debug,  Deserialize, Serialize, Clone)]
pub struct ImporterColumn {
    pub header: String,
    pub column: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ImporterConfig {
    pub fields: Vec<ImporterColumn>,
    pub dataset: String,
    pub format: String,
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
