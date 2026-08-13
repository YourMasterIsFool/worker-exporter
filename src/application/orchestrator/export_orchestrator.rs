use std::{path::PathBuf, sync::Arc};

use anyhow::Ok;
use moka::future::Cache;
use serde_json::json;
use tracing::instrument;

use crate::{
    application::{
        dataset::DatasetRegistry,
        grpcs::{facilitator::s3_proto::UploadFileRequest, gprc::GrpcService},
        loader_registry::LoaderRegistry,
        pipelines::pipeline_registry::PipelineRegistry,
        runner::export_runner::ExportRunner,
        types::EmailProvider,
        writter::WritterRegistry,
    },
    core::{
        traits::{
            orchestrator::{Orchestrator, OrchestratorJob},
            publisher::Publisher,
        },
        types::{CastingEnum, WhereField},
    },
    domain::logging::{
        entity::{LoggingEntity, LoggingStatus, LoggingType},
        repo::LoggingRepo,
    },
    utils::sending_email::{sending_email, EmailPublisher},
};

pub struct ExportOrchestrator {
    pub pipeline: Arc<PipelineRegistry>,
    pub writter: Arc<WritterRegistry>,
    pub logging: Arc<dyn LoggingRepo>,
    pub load: Arc<LoaderRegistry>,
    pub dataset: Arc<DatasetRegistry>,
    pub grpc_service: Arc<GrpcService>,
    pub publisher: Arc<dyn Publisher>,
    pub local_cache: Cache<String, serde_json::Value>,
}

#[async_trait::async_trait]
impl Orchestrator for ExportOrchestrator {
    #[instrument(skip(self, job), fields(msg_id = %msg_id, tenant_id = %job.tenant_id, types="export"))]
    async fn run(&self, job: &OrchestratorJob, msg_id: &str) -> anyhow::Result<()> {
        // just example
        // let ingestion_pipeline = self.pipeline.get("ingestion")?;
        // let loader = self.load.get("tenants")?;

        let logging_entity = LoggingEntity {
            id: None,
            created_at: None,
            tenant_id: job.tenant_id.clone(),
            email: job.email.clone(),
            actor_id: job.actor_id.clone(),
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
            stream_id: msg_id.to_string(),
            config: Some(serde_json::to_value(job.clone())?),
            created_at: None,
        };
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

        if let Some(export_config) = &job.export_config {
            let export_columns = export_config.fields.clone();

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

            // Inisialisasi file path
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

            // =================================================================
            // PERBAIKAN 1: Ekstrak String di sini agar tidak memindahkan `file_path`
            // =================================================================
            let file_path_str = file_path.to_string_lossy().into_owned();

            let mut writter = tokio::task::spawn_blocking(move || {
                // Sekarang yang pindah (move) hanyalah string `file_path_str`,
                // variabel `file_path` (PathBuf) di atas tetap aman & utuh!
                writter.init(&file_path_str).map(|_| writter)
            })
            .await??;

            let dataset = self.dataset.get(&export_config.dataset)?;
            let tenant_id = job
                .import_config
                .as_ref()
                .map(|config| config.tenant_id.clone())
                .filter(|tenant_id| !tenant_id.is_empty())
                .unwrap_or_else(|| job.tenant_id.clone());

            let mut job_filter = vec![WhereField {
                field: "tenant_id".into(),
                operator: "=".to_string(),
                value: Some(tenant_id),
                values: None,
                casting: Some(CastingEnum::Uuid),
            }];

            if let Some(filters) = &job.filters {
                job_filter.extend(filters.clone());
            }
            let mut cursor: Option<String> = Some(
                dataset
                    .as_ref()
                    .get_min_cursor(Some(job_filter.clone()))
                    .await?,
            );

            let expor_runner = ExportRunner::run(
                dataset.as_ref(),
                self.logging.as_ref(),
                writter.as_mut(),
                &export_columns,
                msg_id,
                Some(job_filter),
                &mut cursor,
                export_config.chunk,
                &self.local_cache,
            )
            .await;

            if let Err(err) = expor_runner {
                tracing::error!("gagal export runner {:?}", err);
                return Err(err);
            }

            tokio::task::spawn_blocking(move || {
                if let Err(finish_err) = writter.finish() {
                    tracing::error!("Gagal mengunci dan menutup stream file: {:?}", finish_err);
                }
            })
            .await?;

            // =================================================================
            // PERBAIKAN 2: Lewatkan referensi `&file_path` untuk dibaca oleh Tokio
            // =================================================================
            let buffer_data = tokio::fs::read(&file_path)
                .await
                .map_err(|e| anyhow::anyhow!("Gagal membaca file hasil export: {}", e))?;

            let upload = self
                .grpc_service
                .s3_facilitator_grpc
                .upload_file(UploadFileRequest {
                    usage: "DOCUMENT".into(),
                    bucket_type: "PUBLIC".into(),
                    app_type: "HRMS".into(),
                    tenant_id: job.tenant_id.clone(),
                    data: buffer_data,
                    file_name: file_path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .unwrap_or("")
                        .into(),
                    file_type: file_path
                        .extension()
                        .and_then(|extension| extension.to_str())
                        .unwrap_or("")
                        .into(),
                })
                .await?;
            let key = upload.get_ref().key.clone();

           
            let download_url = export_config.redirected_url.replace("{{url}}", &key);
                self.logging
                .update_download_url(msg_id, download_url.clone()).await?;
            let payload = json!({
                "exporterName": job.email.clone(),
                "downloadUrl": download_url
            });
            let email = EmailPublisher {
                email: job.email.clone(),
                subject: "Send Email".into(),
                template_id: None,
                // template_code: Some("LSI_EMAIL_EXPORT".into()),
                template_code: Some(export_config.template_code.clone()),
                template_structure_code: Some(export_config.structure_template_code.clone()),
                // template_structure_code: Some("LSI_EXPORT_STRUCTURE".into()),
                provider: EmailProvider::SendGrid.to_string(),
                payload,
            };

            if let Some(send_email) = export_config.send_email {
                if send_email {
                    sending_email(self.publisher.clone(), "stream:email", &email).await?;
                }
            }

            // remove temporary excel after upload into grpc

            // if let Err(err) = remove_file(&file_path) {
            //     tracing::warn!("failed to remove file: {:?}", err);
            // }
        }
        self.logging.mark_as_completed(msg_id).await?;
        self.local_cache.invalidate_all();

        Ok(())
    }
}
