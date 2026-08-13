use std::sync::Arc;

use crate::{
    application::orchestrator::export_orchestrator::ExportOrchestrator,
    core::traits::orchestrator::{Orchestrator, OrchestratorJob},
};

pub struct DynamicExportOrchestrator {
    pub export: Arc<ExportOrchestrator>,
}

#[async_trait::async_trait]
impl Orchestrator for DynamicExportOrchestrator {
    async fn run(&self, job: &OrchestratorJob, msg_id: &str) -> anyhow::Result<()> {
        if job.dynamic_export_config.is_none() {
            anyhow::bail!("DYNAMIC_EXPORT requires dynamic_export_config");
        }
        self.export.run_dynamic(job, msg_id).await
    }
}
