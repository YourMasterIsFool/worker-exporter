use std::{path::PathBuf, sync::Arc};

use anyhow::Ok;
use tracing::instrument;

use crate::{
    application::{
        dataset::DatasetRegistry,
        load::LoadRegistry,
        pipelines::pipeline_registry::PipelineRegistry,
        runner::{export_runner::ExportRunner, loader_runner::LoaderRunner},
        writter::WritterRegistry,
    },
    core::{
        datasets::dataset_trait::Dataset,
        traits::orchestrator::{Orchestrator, OrchestratorJob},
    },
    domain::logging::{
        entity::{LoggingEntity, LoggingStatus, LoggingType},
        repo::LoggingRepo,
    },
};

pub struct ExportOrchestrator {
    pub pipeline: Arc<PipelineRegistry>,
    pub writter: Arc<WritterRegistry>,
    pub logging: Arc<dyn LoggingRepo>,
    pub load: Arc<LoadRegistry>,
    pub dataset: Arc<DatasetRegistry>,
}

#[async_trait::async_trait]
impl Orchestrator for ExportOrchestrator {
    #[instrument(skip(self, job), fields(msg_id = %msg_id, tenant_id = %job.tenant_id))]
    async fn run(&self, job: &OrchestratorJob, msg_id: &str) -> anyhow::Result<()> {
        // just example
        let ingestion_pipeline = self.pipeline.get("ingestion")?;
        let loader = self.load.get("tenants")?;

        let logging_entity = LoggingEntity {
            id: None,
            logging_type: LoggingType::Export.to_string(),
            completed_at: None,
            download_url: None,
            status: LoggingStatus::Processing.to_string(),
            error: None,
            additional_info: None,
            processed_count: 0,
            current_chunk: 0,
            total_chunk: 0,
            cursor: None,
            stream_id: msg_id.to_string().clone(),
        };

        // create logging first
        self.logging.store(logging_entity).await?;

        // loader first data
        if let Err(err) = LoaderRunner::run(
            ingestion_pipeline.as_ref(),
            loader.as_ref(),
            self.logging.as_ref(),
            msg_id,
        )
        .await
        {
            self.logging
                .updateError(msg_id, Some(err.to_string()))
                .await?;

            return Err(err);
        }

        // =================================================================
        // PERBAIKAN DI SINI
        // =================================================================

        // 1. Tambahkan `mut` di sini agar objek Box gres yang kamu dapatkan
        // dari Factory bisa dimutasi secara lokal di dalam fungsi ini.
        let mut writter = self.writter.get("csv")?;

        // init file path
        let safe_msg_id: String = msg_id
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        let file_path: PathBuf = PathBuf::from("exports").join(format!("{safe_msg_id}.csv"));
        writter.init(&file_path.to_string_lossy())?;

        let dataset = self.dataset.get("tenants")?;

        // 2. Gunakan writter.as_mut() untuk memberikan `&mut dyn WritterTrait` ke runner
        if let Err(err) = ExportRunner::run(
            dataset.as_ref(),
            self.logging.as_ref(),
            writter.as_mut(),
            msg_id,
        )
        .await
        {
            self.logging
                .updateError(msg_id, Some(err.to_string()))
                .await?;
            return Err(err);
        }

        self.logging.markAsCompleted(msg_id).await?;

        Ok(())
    }
}
