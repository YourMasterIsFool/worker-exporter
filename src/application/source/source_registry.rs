use std::sync::Arc;

use crate::{
    core::traits::{
        extractor::source_incremental_extract::SourceIncrementalExtract,
        strategy::registry::Registry,
    },
    infrastructure::ingestion::organization::{
        organization_postgres_client::OrganizatinPostgresClient, sql::tenant::source::TenantSource,
    },
};
pub type SourceRegistry = Registry<dyn SourceIncrementalExtract + Send + Sync>;

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
