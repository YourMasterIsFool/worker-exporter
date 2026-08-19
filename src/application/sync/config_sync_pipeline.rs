use std::sync::Arc;

use crate::{
    application::{
        bulk_loader_registry::BulkLoaderRegistry, dataset::SyncDatasetRegistry,
        ingestion::source_registry::SourceRegistry, runner::sync_runner::SyncRunner,
    },
    core::{
        traits::load::SyncAggregator,
        types::{CastingEnum, SyncPayloadDataset, WhereField},
    },
    domain::logging::repo::LoggingRepo,
    infrastructure::db::ats_client::AtsClient,
};

/// Sync pipeline selected by JSON. `source`, `dataset`, and ordered `loaders`
/// are registry keys supplied by `sync_config`.
pub struct ConfigSyncPipeline {
    pub source: Arc<SourceRegistry>,
    pub dataset: Arc<SyncDatasetRegistry>,
    pub bulk_loader: Arc<BulkLoaderRegistry>,
    pub ats_client: Arc<AtsClient>,
    pub logging: Arc<dyn LoggingRepo>,
}

#[async_trait::async_trait]
impl SyncAggregator for ConfigSyncPipeline {
    async fn run(&self, payload: &SyncPayloadDataset, logging_id: &str) -> anyhow::Result<i64> {
        let source_key = payload
            .config
            .source
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| anyhow::anyhow!("sync_config.source wajib diisi"))?;
        let dataset_key = payload
            .config
            .dataset
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| anyhow::anyhow!("sync_config.dataset wajib diisi"))?;
        if payload.config.loaders.is_empty() {
            anyhow::bail!("sync_config.loaders wajib diisi");
        }

        let source = self.source.get(source_key)?;
        let dataset = self.dataset.get(dataset_key)?;
        let loader_names = payload
            .config
            .loaders
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let filters = vec![WhereField {
            field: "logging_id".into(),
            operator: "=".into(),
            casting: Some(CastingEnum::Varchar),
            values: Some(vec![logging_id.to_string()]),
            value: None,
        }];
        let mut cursor = None;
        let limit = payload.config.chunk.max(1);

        let result = SyncRunner::run(
            source.as_ref(),
            dataset.as_ref(),
            self.bulk_loader.as_ref(),
            self.ats_client.as_ref(),
            self.logging.as_ref(),
            logging_id,
            payload,
            &mut cursor,
            limit,
            &loader_names,
            Some(filters),
        )
        .await?;

        Ok(result.processed_count)
    }
}
