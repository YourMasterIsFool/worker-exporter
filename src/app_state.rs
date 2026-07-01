use std::sync::Arc;

use excelstream::ExcelWriter;
use redis::aio::ConnectionManager;

use crate::{
    application::{
        grpcs::gprc::GrpcService, orchestrator::orchestrator_registry::OrchestratorRegistry,
        usecases::usecases::Usecases,
    },
    core::traits::publisher::Publisher,
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
    pub grpc_service: Arc<GrpcService>,
    pub publisher: Arc<dyn Publisher + Send + Sync>,
    pub manager: Arc<ConnectionManager>,
    pub usecases: Arc<Usecases>,
    // pub writter_registry: Arc<WritterRegistry>,
}
