use std::sync::Arc;

use crate::application::{
    grpcs::facilitator::S3FacilitatorGrpc,
    orchestrator::orchestrator_registry::OrchestratorRegistry, usecases::usecases::Usecases,
};
#[derive(Clone)]
pub struct AppState {
    // pub warehouse_client: Arc<WarehouseClient>,
    // pub organization_db: Arc<OrganizatinPostgresClient>,
    // pub pipeline_registry: Arc<PipelineRegistry>,
    // pub source_registry: Arc<SourceRegistry>,
    // pub incremental_load_registry: Arc<LoadRegistry>,
    // pub warehouse_repo: Arc<WarehouseRepo>,
    pub orchestrator_registry: Arc<OrchestratorRegistry>,
    pub s3_facilitator_grpc: Arc<S3FacilitatorGrpc>,
    pub usecases: Arc<Usecases>, // pub writter_registry: Arc<WritterRegistry>,
}
