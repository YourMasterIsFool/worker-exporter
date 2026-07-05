use std::{collections::HashMap, sync::Arc};

use crate::{
    application::loader_registry::LoaderRegistry,
    core::{traits::pipeline::pipeline_trait::PipelineImport, types::ImportRowBulk},
};

pub struct VacancyImportPipeline {
    pub loader: Arc<LoaderRegistry>,
}

#[async_trait::async_trait]
impl PipelineImport for VacancyImportPipeline {
    async fn import(
        &self,
        mapper_data: &HashMap<String, Vec<ImportRowBulk>>,
        logging_id: &str,
    ) -> anyhow::Result<()> {
        // Asumsi: get() mengembalikan Result<T, AppError> atau Option
        // Jika return-nya Result, gunakan ? untuk langsung return error ke fungsi import

        let vacancy_loader = self
            .loader
            .get("warehouse_ats_vacancy_temporary")
            .map_err(|e| anyhow::anyhow!("Loader not found: {}", e))?;

        for (key, rows) in mapper_data {}

        // if let Some(rows) = mapper_data.get("main") {
        //     let mapped_value = rows.iter().map(|f| f.value.clone()).collect::<Vec<_>>();

        //     // Perbaikan: map_err hanya perlu mengembalikan AppError, bukan Err(AppError)
        //     vacancy_loader
        //         .load(&mapped_value)
        //         .await
        //         .map_err(|err| anyhow::anyhow!("Vacancy Ingestion pipeline error: {}", err))?;

        //     info!("Pipeline Import Vacancy IngestionPipeline success");
        // }

        for (key, rows) in mapper_data {}
        Ok(())
    }
}
