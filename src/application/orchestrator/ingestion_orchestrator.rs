use std::sync::Arc;

use crate::{
    app_state::AppState,
    core::traits::{
        extractor::source_incremental_extract::SourceIncrementalExtract,
        load::LoaderWarehouse,
        orchestrator::{Orchestrator, OrchestratorJob},
    },
};

pub struct IngestionOrchestrator {
    pub state: Arc<AppState>,
}

#[async_trait::async_trait]
impl Orchestrator for IngestionOrchestrator {
    async fn run(&self, job: &OrchestratorJob, msg_id: &str) -> anyhow::Result<()> {
        let tenant_id = &job.tenant_id;
        let limit = job.limit;
        let source = &job.source;

        Ok(())
    }
}
