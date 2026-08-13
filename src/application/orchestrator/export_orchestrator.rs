use std::{path::PathBuf, sync::Arc};

use anyhow::Ok;
use serde::Serialize;
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
        config::config,
        traits::{
            dataset_trait::Dataset,
            orchestrator::{Orchestrator, OrchestratorJob},
            publisher::Publisher,
        },
        types::WhereField,
    },
    domain::logging::{
        entity::{LoggingEntity, LoggingStatus, LoggingType},
        repo::LoggingRepo,
    },
};

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")] //
struct EmailPublisher {
    email: String,
    subject: String,
    template_id: Option<String>,
    template_code: Option<String>,
    template_structure_code: Option<String>,
    provider: String,
    payload: serde_json::Value,
}
pub struct ExportOrchestrator {
    pub pipeline: Arc<PipelineRegistry>,
    pub writter: Arc<WritterRegistry>,
    pub logging: Arc<dyn LoggingRepo>,
    pub load: Arc<LoaderRegistry>,
    pub dataset: Arc<DatasetRegistry>,
    pub grpc_service: Arc<GrpcService>,
    pub publisher: Arc<dyn Publisher>,
}

impl ExportOrchestrator {
    pub async fn run_dynamic(&self, job: &OrchestratorJob, msg_id: &str) -> anyhow::Result<()> {
        <Self as Orchestrator>::run(self, job, msg_id).await
    }

    fn dynamic_filters(
        job: &OrchestratorJob,
        config: &crate::core::traits::orchestrator::DynamicExportConfig,
    ) -> Vec<WhereField> {
        let tenant_field = config.source.tenant_field.as_deref().unwrap_or("tenant_id");
        let tenant_operator = config.source.tenant_operator.as_deref().unwrap_or("=");
        let mut filters = vec![WhereField {
            field: tenant_field.into(),
            operator: tenant_operator.to_string(),
            value: (tenant_operator == "=").then_some(job.tenant_id.clone()),
            values: (tenant_operator != "=").then_some(vec![job.tenant_id.clone()]),
            casting: None,
        }];
        if let Some(extra_filters) = &job.filters {
            filters.extend(extra_filters.clone());
        }
        filters
    }
}

#[async_trait::async_trait]
impl Orchestrator for ExportOrchestrator {
    #[instrument(skip(self, job), fields(msg_id = %msg_id, tenant_id = %job.tenant_id, types="export"))]
    async fn run(&self, job: &OrchestratorJob, msg_id: &str) -> anyhow::Result<()> {
        let dynamic_export_config = job.dynamic_export_config.as_ref();
        let dynamic_legacy_config;
        let config = match (&job.export_config, dynamic_export_config) {
            (Some(config), None) => config,
            (None, Some(config)) => {
                dynamic_legacy_config = config.export_config();
                &dynamic_legacy_config
            }
            (Some(_), Some(_)) => {
                anyhow::bail!("use export_config or dynamic_export_config, not both")
            }
            (None, None) => anyhow::bail!("export configuration is required"),
        };
        let dynamic_filters =
            dynamic_export_config.map(|config| Self::dynamic_filters(job, config));
        let (total_count, total_chunk) = match dynamic_export_config {
            Some(config) => {
                if config.chunk <= 0 {
                    anyhow::bail!("dynamic export chunk must be greater than zero");
                }
                let total_count = self
                    .dataset
                    .get(&config.dataset)?
                    .count_with_dynamic_export_config(
                        &crate::core::traits::load::LoadEnum::Select(dynamic_filters.clone(), None),
                        config,
                    )
                    .await?;
                let total_chunk = total_count.saturating_add(config.chunk - 1) / config.chunk;
                (total_count, total_chunk)
            }
            None => (0, 0),
        };

        let logging_entity = LoggingEntity {
            id: None,
            created_at: None,
            tenant_id: job.tenant_id.clone(),
            title: job.title.clone(),
            email: job.email.clone(),
            actor_id: job.actor_id.clone(),
            logging_type: LoggingType::Export.to_string(),
            completed_at: None,
            download_url: None,
            status: LoggingStatus::Processing.to_string(),
            error: None,
            additional_info: None,
            processed_count: 0,
            total_count,
            current_chunk: 0,
            total_chunk: total_chunk
                .try_into()
                .map_err(|_| anyhow::anyhow!("total chunk exceeds i32"))?,
            cursor: None,
            stream_id: msg_id.to_string().clone(),
            config: Some(serde_json::to_value(job.clone())?),
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

        {
            let export_columns = config.fields.clone();

            let format = config.format.as_str();
            let format_file = match format {
                "csv" => "csv",
                "excel" => "xlsx",
                _ => anyhow::bail!("format must match a writer registry key: csv or excel"),
            };

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

            let dataset = self.dataset.get(&config.dataset)?;
            let job_filter = dynamic_filters.clone().unwrap_or_else(|| {
                let mut filters = vec![WhereField {
                    field: "tenant_id".into(),
                    operator: "=".into(),
                    value: Some(job.tenant_id.clone()),
                    values: None,
                    casting: None,
                }];
                if let Some(extra_filters) = &job.filters {
                    filters.extend(extra_filters.clone());
                }
                filters
            });
            let mut cursor: Option<String> = match dynamic_export_config {
                Some(_) => None,
                None => Some(
                    dataset
                        .as_ref()
                        .get_min_cursor(Some(job_filter.clone()))
                        .await?,
                ),
            };

            let expor_runner = ExportRunner::run(
                dataset.as_ref(),
                self.logging.as_ref(),
                writter.as_mut(),
                &export_columns,
                msg_id,
                Some(job_filter),
                &mut cursor,
                config.chunk,
                dynamic_export_config,
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

            // Panggil client gRPC Anda secara mutable menggunakan Arc::clone jika dibutuhkan,
            // namun karena upload_file butuh `&mut self`, pastikan s3_grpc di struktur struct
            // dikelola secara mandiri atau gunakan fungsi internal jika trait membolehkan.
            // Di sini kita berasumsi pembungkus s3_grpc Anda mengizinkan pemanggilan langsung.
            let payload = self
                .grpc_service
                .s3_facilitator_grpc
                .upload_file(UploadFileRequest {
                    usage: "DOCUMENT".into(),
                    bucket_type: "PUBLIC".into(),
                    app_type: "HRMS".into(),
                    tenant_id: job.tenant_id.clone(), // Tambahkan .clone() jika tenant_id adalah String agar tidak ter-move
                    data: buffer_data,
                    // AMAN: `file_path` di bawah ini sekarang valid untuk digunakan berkali-kali
                    file_name: file_path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or("")
                        .into(),
                    file_type: file_path
                        .extension()
                        .and_then(|e| e.to_str())
                        .unwrap_or("")
                        .into(),
                })
                .await?;

            let key = payload.get_ref().key.clone();
            self.logging
                .update_download_url(msg_id, key.clone())
                .await?;
            let download_url = config.redirected_url.replace("{{url}}", &key);
            let payload = json!({
                "exporterName": job.email.clone(),
                "downloadUrl": download_url
            });
            let email = EmailPublisher {
                email: job.email.clone(),
                subject: "Send Email".into(),
                template_id: None,
                // template_code: Some("LSI_EMAIL_EXPORT".into()),
                template_code: Some(config.template_code.clone()),
                template_structure_code: Some(config.structure_template_code.clone()),
                // template_structure_code: Some("LSI_EXPORT_STRUCTURE".into()),
                provider: EmailProvider::SendGrid.to_string(),
                payload,
            };

            let payload_email = serde_json::to_value(email)?;

            match self.publisher.publish("stream:email", payload_email).await {
                Result::Ok(_) => {
                    tracing::info!("Successfully stream email")
                }
                Err(err) => {
                    tracing::error!("error stream publish email {}", err);
                }
            }

            // remove temporary excel after upload into grpc

            // if let Err(err) = remove_file(&file_path) {
            //     tracing::warn!("failed to remove file: {:?}", err);
            // }
        }
        self.logging.mark_as_completed(msg_id).await?;

        Ok(())
    }
}
