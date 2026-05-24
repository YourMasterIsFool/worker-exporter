use std::sync::Arc;

use crate::{
    application::{
        handler::handler_registry::HandlerRegistry, load::LoadRegistry,
        orchestrator::orchestrator_registry::OrchestratorRegistry,
        pipelines::pipeline_registry::PipelineRegistry, source::source_registry::SourceRegistry,
        writter::WritterRegistry,
    },
    core::traits::{consumer::Consumer, orchestrator::Orchestrator},
    infrastructure::{
        ingestion::organization::organization_postgres_client::OrganizatinPostgresClient,
        warehouse::warehouse_client::WarehouseClient,
    },
    repo::WarehouseRepo,
};

#[derive(Clone)]
pub struct AppState {
    pub warehouse_client: Arc<WarehouseClient>,
    pub organization_db: Arc<OrganizatinPostgresClient>,
    // pub pipeline_registry: Arc<PipelineRegistry>,
    pub source_registry: Arc<SourceRegistry>,
    pub incremental_load_registry: Arc<LoadRegistry>,
    pub warehouse_repo: Arc<WarehouseRepo>,
    pub orchestrator_registry: Arc<OrchestratorRegistry>,
    // pub writter_registry: Arc<WritterRegistry>,
}
