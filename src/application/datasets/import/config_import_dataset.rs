use std::{collections::HashMap, sync::Arc};

use calamine::Data;
use moka::future::Cache;
use serde_json::Value;

use crate::{
    application::{
        datasets::import::dynamci_dataset_v1::DynamicDatasetV1,
        fetch_source_regitry::DataFetcherJsonRegistry,
    },
    core::{
        traits::dataset_trait::ImporterDatasetV1,
        types::{ImportDatasetConfig, ImportTableResult},
    },
};

/// JSON-driven import mapper. Tables, fields, fetchers, and EAV routing come
/// entirely from `import_config`; this wrapper gives that mode a stable key.
pub struct ConfigImportDataset {
    inner: DynamicDatasetV1,
}

impl ConfigImportDataset {
    pub fn new(fetcher: Arc<DataFetcherJsonRegistry>) -> Self {
        Self {
            inner: DynamicDatasetV1::new(fetcher),
        }
    }
}

#[async_trait::async_trait]
impl ImporterDatasetV1 for ConfigImportDataset {
    async fn import(
        &self,
        rows: &[Data],
        config: &ImportDatasetConfig,
        cache: &Cache<String, Value>,
    ) -> anyhow::Result<HashMap<String, ImportTableResult>> {
        self.inner.import(rows, config, cache).await
    }
}
