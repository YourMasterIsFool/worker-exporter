use std::sync::Arc;

use crate::application::dataset::ImporterDatasetRegistry;
use crate::infrastructure::db::warehouse_client::WarehouseClient;
use crate::{
    application::grpcs::gprc::GrpcService,
    core::traits::orchestrator::{Orchestrator, OrchestratorJob},
};

pub struct ImportOrchestrator {
    pub warehouse: Arc<WarehouseClient>,
    pub grpc_service: Arc<GrpcService>,
    pub importer: Arc<ImporterDatasetRegistry>,
}

#[async_trait::async_trait]
impl Orchestrator for ImportOrchestrator {
    async fn run(&self, job: &OrchestratorJob, msg_id: &str) -> anyhow::Result<()> {
        if let Some(import_config) = &job.import_config {}
        Ok(())
    }
}
