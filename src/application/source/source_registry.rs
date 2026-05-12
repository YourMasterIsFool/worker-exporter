use std::sync::Arc;

use crate::{
    app_state::AppState,
    core::traits::{
        extractor::incremental_extract::IncrementalExtract, strategy::registry::Registry,
    },
    infrastructure::ingestion::organization::{
        organization_postgres_client::OrganizatinPostgresClient, sql::tenant::source::TenantSource,
    },
};
pub type SourceRegistry = Registry<dyn IncrementalExtract + Send + Sync>;

pub fn build_source_registry(organization_db: Arc<OrganizatinPostgresClient>) -> SourceRegistry {
    let mut registry = SourceRegistry::new();
    registry.register(
        "tenants",
        Arc::new(TenantSource {
            pool: organization_db.clone(),
        }),
    );
    registry
}
