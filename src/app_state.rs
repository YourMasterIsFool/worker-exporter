use std::sync::Arc;

use crate::{
    application::{
        handler::handler_registry::HandlerRegistry, load::LoadRegistry,
        pipelines::pipeline_registry::PipelineRegistry, source::source_registry::SourceRegistry,
    },
    core::traits::consumer::Consumer,
    infrastructure::{
        ingestion::organization::organization_postgres_client::OrganizatinPostgresClient,
        warehouse::warehouse_client::WarehouseClient,
    },
};

#[derive(Clone)]
pub struct AppState {
    pub warehouse_client: Arc<WarehouseClient>,
    pub organization_db: Arc<OrganizatinPostgresClient>,
    // pub pipeline_registry: Arc<PipelineRegistry>,
    pub source_registry: Arc<SourceRegistry>,
    pub incremental_load_registry: Arc<LoadRegistry>,
}
