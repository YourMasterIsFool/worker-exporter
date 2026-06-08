use std::{path::PathBuf, sync::Arc};

use anyhow::Ok;
use tracing::{info, instrument};

use crate::{
    application::{
        dataset::DatasetRegistry,
        load::LoadRegistry,
        orchestrator::export_orchestrator,
        pipelines::pipeline_registry::PipelineRegistry,
        runner::{export_runner::ExportRunner, loader_runner::LoaderRunner},
        source::source_registry::SourceRegistry,
        writter::WritterRegistry,
    },
    core::traits::orchestrator::{Orchestrator, OrchestratorJob},
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
        // let ingestion_pipeline = self.pipeline.get("ingestion")?;
        // let loader = self.load.get("tenants")?;

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
        // if let Err(err) = LoaderRunner::run(
        //     ingestion_pipeline.as_ref(),
        //     loader.as_ref(),
        //     self.logging.as_ref(),
        //     msg_id,
        // )
        // .await
        // {
        //     self.logging
        //         .update_error(msg_id, Some(err.to_string()))
        //         .await?;

        //     return Err(err);
        // }

        // =================================================================
        // PERBAIKAN DI SINI
        // =================================================================

        // 1. Tambahkan `mut` di sini agar objek Box gres yang kamu dapatkan
        // dari Factory bisa dimutasi secara lokal di dalam fungsi ini.
        //

        if let Some(config) = &job.export_config {
            let export_columns = config.fields.clone();

            let mut format = String::from("csv");
            let mut format_file = String::from("csv");

            if let Some(format_str) = &job.export_config.as_ref().map(|c| c.format.as_str()) {
                format = String::from(*format_str);
            }

            if format == "csv" {
                format_file = String::from("csv");
            } else if format == "excel" {
                format_file = String::from("xlsx");
            }

            let mut writter = self.writter.get(&format)?;

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
            let file_path: PathBuf =
                PathBuf::from("exports").join(format!("{safe_msg_id}.{}", format_file));

            let mut writter = tokio::task::spawn_blocking(move || {
                writter.init(&file_path.to_string_lossy()).map(|_| writter)
            })
            .await??;

            let dataset = self.dataset.get("tenants")?;
            let mut cursor: Option<String> =
                Some(dataset.as_ref().get_min_cursor(job.filters.clone()).await?);
            info!("cursor {:?}", cursor);
            // 2. Gunakan writter.as_mut() untuk memberikan `&mut dyn WritterTrait` ke runner
            ExportRunner::run(
                dataset.as_ref(),
                self.logging.as_ref(),
                writter.as_mut(),
                &export_columns,
                msg_id,
                job.filters.clone(),
                &mut cursor,
            )
            .await;
            tokio::task::spawn_blocking(move || {
                if let Err(finish_err) = writter.finish() {
                    tracing::error!("Gagal mengunci dan menutup stream file: {:?}", finish_err);
                }
            })
            .await?;
        }

        self.logging.mark_as_completed(msg_id).await?;

        Ok(())
    }
}
