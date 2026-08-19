use std::sync::Arc;

use crate::{
    application::import::import_registry::ImportLoaderRegistry,
    core::{
        traits::pipeline::pipeline_trait::PipelineImport,
        types::{ImportDatasetConfig, ImportTableResult},
    },
};

/// Config-driven import pipeline. Each configured temporary loader receives
/// the same mapped chunk and may choose the tables it owns.
pub struct VacancyImportPipeline {
    pub loader: Arc<ImportLoaderRegistry>,
}

#[async_trait::async_trait]
impl PipelineImport for VacancyImportPipeline {
    async fn load(
        &self,
        rows: &[std::collections::HashMap<String, ImportTableResult>],
        logging_id: &str,
        config: &ImportDatasetConfig,
    ) -> anyhow::Result<i64> {
        let loaders = if config.import_config.loaders.is_empty() {
            std::slice::from_ref(&config.import_config.loader)
        } else {
            config.import_config.loaders.as_slice()
        };

        if loaders.iter().any(|loader| loader.trim().is_empty()) {
            anyhow::bail!("import_config.loader atau import_config.loaders wajib diisi");
        }

        let mut loaded = 0;
        for loader_name in loaders {
            let loader = self.loader.get(loader_name)?;
            loaded += loader.load(rows, logging_id, config).await?;
        }
        Ok(loaded)
    }
}
