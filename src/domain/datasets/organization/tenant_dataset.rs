use std::sync::Arc;

use crate::{
    core::{
        datasets::dataset_trait::Dataset,
        traits::{
            extractor::incremental_source::IncrementalSource, writter::writter_trait::ExportRow,
        },
    },
    domain::snapshots::organization::tenant_snapshot::TenantSnapshot,
    infrastructure::warehouse::{
        snapshots::organization::tenants::query::TenantPoolQuerySnapshot,
        warehouse_client::WarehouseClient,
    },
};

pub struct TenantDataset {
    pub client: Arc<WarehouseClient>,
}

impl From<TenantSnapshot> for ExportRow {
    fn from(value: TenantSnapshot) -> Self {
        Self {
            fields: vec![
                value.tenant_id,
                value.tenant_name,
                value.tenant_code,
                value.id.unwrap_or_default(),
            ],
        }
    }
}

#[async_trait::async_trait]
impl Dataset for TenantDataset {
    async fn fetch_batch(
        &self,
        cursor: Option<String>,
        limit: i64,
    ) -> anyhow::Result<(Vec<ExportRow>, Option<String>)> {
        let tenant_snapshot = TenantPoolQuerySnapshot {
            pool: self.client.clone(),
        };

        let tenants = tenant_snapshot.fetch(limit, cursor, None).await?;

        let rows: Vec<ExportRow> = tenants.into_iter().map(|t| t.into()).collect();

        Ok((rows, None))
    }
}
