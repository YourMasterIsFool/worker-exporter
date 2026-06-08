use serde::{Deserialize, Serialize};

use crate::core::traits::extractor::incremental_source::WhereField;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ExporterColumn {
    pub name: String,
    pub column: String,
}
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ExporterConfig {
    pub fields: Vec<ExporterColumn>,
    pub source: String,
    pub format: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct IngestionConfig {
    pub source: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct OrchestratorJob {
    pub(crate) tenant_id: String,
    pub(crate) limit: i64,
    pub(crate) source: String,
    pub(crate) export_config: Option<ExporterConfig>,
    pub(crate) ingestion_config: Option<IngestionConfig>,
    pub(crate) filters: Option<Vec<WhereField>>, // pub(crate) fields: Vec<ExportFields>,
}
#[async_trait::async_trait]
pub trait Orchestrator: Send + Sync {
    async fn run(&self, job: &OrchestratorJob, msg_id: &str) -> anyhow::Result<()>;
}
