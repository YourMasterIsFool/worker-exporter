use calamine::{open_workbook_auto, open_workbook_auto_from_rs, DataType, Reader};
use std::collections::HashMap;
use std::result;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tracing::{debug, info, instrument};
use tracing_subscriber::field;

use crate::application::dataset::{ImporterDatasetRegistry, ImporterDatasetRegistryV1};
use crate::application::import::import_registry::ImportLoaderRegistry;
use crate::application::loader_registry::LoaderRegistry;
use crate::application::pipelines::pipeline_import_registry::PipelineImportRegistry;
use crate::core::traits::dataset_trait::{ImporterDataset, ImporterDatasetV1}; // 🔥 Pastikan mengimpor ImporterDatasetV1
use crate::core::traits::load::{self, ImportLoadPersitence, IncrementalLoadPersitence};
use crate::core::traits::orchestrator::ImporterColumn;
use crate::core::traits::pipeline::pipeline_trait::PipelineImport;
use crate::core::types::{
    CastingEnum, ImportBulkRequest, ImportDatasetConfig, ImportRow, ImportRowBulk,
    ImportTableResult, TableStrategy,
};
use crate::domain::logging;
use crate::domain::logging::entity::{LoggingEntity, LoggingStatus, LoggingType};
use crate::domain::logging::repo::LoggingRepo;
use crate::infrastructure;
use crate::infrastructure::db::warehouse_client::WarehouseClient;
use crate::infrastructure::pipeline::ats::vacancy_import_pipeline;
use crate::{
    application::grpcs::gprc::GrpcService,
    core::traits::orchestrator::{Orchestrator, OrchestratorJob},
};
use futures_util::StreamExt;
use serde_json::{json, Value};
use std::io::Cursor; // 🔥 Diperlukan untuk byte_stream.next()

pub struct ImportOrchestrator {
    pub grpc_service: Arc<GrpcService>,
    pub imporer_dataset: Arc<ImporterDatasetRegistry>,
    pub import_datasetV1: Arc<ImporterDatasetRegistryV1>,
    pub import_pipeline: Arc<PipelineImportRegistry>,
    pub logging: Arc<dyn LoggingRepo>,
    pub import_load_registry: Arc<ImportLoaderRegistry>,
}

impl ImportOrchestrator {
    pub async fn process_stream_mode(
        &self,
        url: &str,
        import_pipeline: &dyn PipelineImport,
        dataset: &dyn ImporterDatasetV1, // 🔥 PERBAIKAN: Parameter ditambahkan kembali
        logging_id: &str,
        dataset_config: &ImportDatasetConfig,
        import_load_registry: &dyn ImportLoadPersitence,
    ) -> anyhow::Result<()> {
        // 1. Ambil network bytes stream dari AWS S3 via Reqwest
        let response = reqwest::get(url).await?;
        let mut byte_stream = response.bytes_stream();

        // 2. Buat file temporary di storage lokal server (/tmp)
        let tmp_file_path = format!("/tmp/import_{}.xlsx", logging_id);
        let mut tmp_file = File::create(&tmp_file_path).await?;

        tracing::info!(
            logging_id = logging_id,
            "Memulai streaming download dari S3 ke disk lokal..."
        );

        // 3. Alirkan chunk network bytes langsung ke disk secara gradual
        while let Some(chunk_result) = byte_stream.next().await {
            let chunk = chunk_result?;
            tmp_file.write_all(&chunk).await?;
        }

        // 🔥 PENTING: Flush sisa buffer OS dan pastikan file lock dilepas (drop)
        tmp_file.flush().await?;
        drop(tmp_file);

        tracing::info!(
            logging_id = logging_id,
            "Download selesai. Membuka file via Calamine secara lazy."
        );

        // 4. Buka file dari disk menggunakan Calamine di dalam scope terisolasi `{}`
        {
            let mut workbook = calamine::open_workbook_auto(&tmp_file_path).map_err(|err| {
                tracing::error!("Gagal membuka streaming workbook: {}", err);
                err
            })?;

            let sheet_name = workbook.sheet_names()[0].clone();
            let range = workbook.worksheet_range(&sheet_name)?;

            // Pengaturan chunk batch memori RAM
            let mut chunk_batch: Vec<HashMap<String, ImportTableResult>> = vec![];
            let chunk_limit = dataset_config.import_config.chunk as usize;

            // Iterasi baris excel (skip header)
            for row in range.rows().skip(1) {
                let mut row_data = dataset.import(row, &dataset_config).await?;

                // Inject 'logging_id' secara dinamis ke matriks skalar (Bypass tipe Eav)
                for (_table_name, table_result) in row_data.iter_mut() {
                    if table_result.strategy == TableStrategy::Eav {
                        continue;
                    }
                    table_result.columns.push("logging_id".to_string());
                    for row_values in table_result.values.iter_mut() {
                        row_values.push(Value::String(logging_id.to_string()));
                    }
                }

                chunk_batch.push(row_data);

                // 🔥 JIKA MENCAPAI LIMIT, KIRIM DATA KE LOADER & KOSONGKAN BATCH RAM
                if chunk_batch.len() >= chunk_limit {
                    tracing::info!(
                        logging_id = logging_id,
                        "Flushing stream chunk batch: {} rows",
                        chunk_batch.len()
                    );

                    import_load_registry
                        .load(&chunk_batch, logging_id, &dataset_config)
                        .await?;

                    chunk_batch.clear();
                }
            }

            // Jaga-jaga jika ada sisa baris terakhir di Excel yang totalnya tidak genap kelipatan 500
            if !chunk_batch.is_empty() {
                tracing::info!(
                    logging_id = logging_id,
                    "Flushing sisa akhir stream batch: {} rows",
                    chunk_batch.len()
                );
                import_load_registry
                    .load(&chunk_batch, logging_id, &dataset_config)
                    .await?;
            }
        }

        // 5. Bersihkan file sampah temporary `.xlsx` dari disk lokal server
        if let Err(e) = std::fs::remove_file(&tmp_file_path) {
            tracing::error!(
                "Gagal menghapus temporary file '{}': {:?}",
                tmp_file_path,
                e
            );
        } else {
            tracing::info!(
                "Berhasil menghapus temporary file disk '{}' secara bersih",
                tmp_file_path
            );
        }

        Ok(())
    }

    async fn process_buffer_mode(
        &self,
        url: &str,
        import_pipeline: &dyn PipelineImport,
        dataset: &dyn ImporterDatasetV1, // 🔥 PERBAIKAN: Parameter ditambahkan ke signature buffer mode
        logging_id: &str,
        dataset_config: &ImportDatasetConfig,
        import_load_registry: &dyn ImportLoadPersitence,
    ) -> anyhow::Result<()> {
        // 1. Download file dari S3 URL (menggunakan reqwest)
        let response = reqwest::get(url).await?;
        info!("logging id {}", logging_id);

        let bytes = std::fs::read("template_excel_vacancy.xlsx")?;
        // 2. Wrap bytes dalam Cursor
        let cursor = Cursor::new(bytes);

        // 3. Buka workbook
        let mut workbook = open_workbook_auto_from_rs(cursor).map_err(|err| {
            tracing::error!("error open workbook {}", err);
            err
        })?;

        let sheet_name = workbook.sheet_names()[0].clone();
        let range = workbook.worksheet_range(&sheet_name)?;

        // Tipe penampung batch matriks baru
        let mut batch: Vec<HashMap<String, ImportTableResult>> = vec![];

        // 4. Iterasi baris excel (skip header)
        for row in range.rows().skip(1) {
            let mut row_data: HashMap<String, ImportTableResult> =
                dataset.import(row, &dataset_config).await?;

            // 5. Inject 'logging_id' menggunakan pengecekan Enum TableStrategy
            for (_table_name, table_result) in row_data.iter_mut() {
                if table_result.strategy == TableStrategy::Eav {
                    continue;
                }

                table_result.columns.push("logging_id".to_string());

                for row_values in table_result.values.iter_mut() {
                    row_values.push(Value::String(logging_id.to_string()));
                }
            }

            batch.push(row_data);
        }

        info!("import progressing, total rows: {}", batch.len());
        if !batch.is_empty() {
            info!("import data batch sample: {:?}", batch[0]);
        }

        // 6. Simpan batch data matriks ke database melalui loader registry
        match import_load_registry
            .load(&batch, logging_id, &dataset_config)
            .await
        {
            Ok(_) => {
                info!("import successfully");
            }
            Err(err) => {
                tracing::error!("error import loader into temporary: {:?}", err);
                return Err(err);
            }
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl Orchestrator for ImportOrchestrator {
    #[instrument(skip(self, job), fields(msg_id = %msg_id, tenant_id = %job.tenant_id, types="import"))]
    async fn run(&self, job: &OrchestratorJob, msg_id: &str) -> anyhow::Result<()> {
        tracing::info!("running import orchestrator");
        if let Some(import_config) = &job.import_config {
            let logging_entity = LoggingEntity {
                id: None,
                tenant_id: job.tenant_id.clone(),
                email: job.email.clone(),
                actor_id: job.actor_id.clone(),
                logging_type: LoggingType::Import.to_string(),
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

            let logging_id = self.logging.store(logging_entity).await?;
            let import_load_registry = self
                .import_load_registry
                .get("warehouse_ats_vacancy_temporary")?;

            let dataset = self.import_datasetV1.get("dynamic_dataset_v1")?;
            let key = &import_config.key;

            let payload_download_url = vec![key.clone()];
            let get_file = self
                .grpc_service
                .s3_facilitator_grpc
                .get_download_url(payload_download_url)
                .await?;

            let items_download = get_file.into_inner().items;
            tracing::info!("items_download: {:?}", items_download);

            if items_download.is_empty() {
                tracing::error!("No download URL returned");
                return Err(anyhow::anyhow!("No download URL returned"));
            }

            let download_url = items_download[0].url.clone();
            let import_pipeline = self.import_pipeline.get(&import_config.pipeline)?;
            let dataset_config = ImportDatasetConfig {
                where_fields: None,
                import_config: import_config.clone(),
            };

            // 🔥 PERBAIKAN: Mengirim dataset.as_ref() dengan benar ke kedua metode mode
            if import_config.data_source_type == "buffer" {
                match self
                    .process_buffer_mode(
                        &download_url,
                        import_pipeline.as_ref(),
                        dataset.as_ref(), // Dipasang di sini
                        &logging_id,
                        &dataset_config,
                        import_load_registry.as_ref(),
                    )
                    .await
                {
                    Ok(_) => {
                        self.logging
                            .update_status(msg_id, LoggingStatus::Completed)
                            .await?;
                    }
                    Err(e) => {
                        tracing::error!("process buffer mode {}", e);
                        self.logging
                            .update_status(msg_id, LoggingStatus::Failed)
                            .await?;

                        self.logging
                            .update_error(msg_id, Some(e.to_string()))
                            .await?;
                        return Err(e);
                    }
                };
            } else {
                match self
                    .process_stream_mode(
                        &download_url,
                        import_pipeline.as_ref(),
                        dataset.as_ref(), // Dipasang di sini
                        &logging_id,
                        &dataset_config,
                        import_load_registry.as_ref(),
                    )
                    .await
                {
                    Ok(_) => {
                        self.logging
                            .update_status(msg_id, LoggingStatus::Completed)
                            .await?;
                    }
                    Err(e) => {
                        tracing::error!("process stream mode {}", e);
                        self.logging
                            .update_status(msg_id, LoggingStatus::Failed)
                            .await?;

                        self.logging
                            .update_error(msg_id, Some(e.to_string()))
                            .await?;
                        return Err(e);
                    }
                };
            }
        }

        Ok(())
    }
}
