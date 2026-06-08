use std::sync::Arc;

use crate::application::orchestrator::orchestrator_registry::OrchestratorRegistry;
#[derive(Clone)]
pub struct AppState {
    // pub warehouse_client: Arc<WarehouseClient>,
    // pub organization_db: Arc<OrganizatinPostgresClient>,
    // pub pipeline_registry: Arc<PipelineRegistry>,
    // pub source_registry: Arc<SourceRegistry>,
    // pub incremental_load_registry: Arc<LoadRegistry>,
    // pub warehouse_repo: Arc<WarehouseRepo>,
    pub orchestrator_registry: Arc<OrchestratorRegistry>,
    // pub writter_registry: Arc<WritterRegistry>,
}
