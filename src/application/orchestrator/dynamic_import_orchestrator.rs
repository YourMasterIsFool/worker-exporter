use std::sync::Arc;

use crate::{
    application::orchestrator::import_orchestrator::ImportOrchestrator,
    core::traits::orchestrator::{Orchestrator, OrchestratorJob},
};

/// Explicit entry point for fully JSON-driven imports. The normal import
/// orchestrator owns workbook handling and logging, so this wrapper only
/// validates the dataset contract before delegating.
pub struct DynamicImportOrchestrator {
    pub import: Arc<ImportOrchestrator>,
}

#[async_trait::async_trait]
impl Orchestrator for DynamicImportOrchestrator {
    async fn run(&self, job: &OrchestratorJob, msg_id: &str) -> anyhow::Result<()> {
        let dataset = job
            .import_config
            .as_ref()
            .map(|config| config.dataset.as_str())
            .ok_or_else(|| anyhow::anyhow!("DYNAMIC_IMPORT requires import_config"))?;
        if dataset != "DYNAMIC_IMPORT_DATASET" {
            anyhow::bail!("DYNAMIC_IMPORT requires import_config.dataset=DYNAMIC_IMPORT_DATASET");
        }

        self.import.run(job, msg_id).await
    }
}
