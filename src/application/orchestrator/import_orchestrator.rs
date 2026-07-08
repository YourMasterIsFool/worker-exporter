use calamine::{open_workbook_auto, open_workbook_auto_from_rs, DataType, Reader};
use moka::future::Cache;
use std::collections::{HashMap, HashSet};
use std::result;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tracing::{debug, info, instrument};
use tracing_subscriber::field;
use tracing_subscriber::fmt::format;
use tracing_subscriber::registry::Data;

use crate::application::dataset::{ImporterDatasetRegistry, ImporterDatasetRegistryV1};
use crate::application::fetch_source_regitry::DataFetcherJsonRegistry;
use crate::application::import::import_registry::ImportLoaderRegistry;
use crate::application::loader_registry::LoaderRegistry;
use crate::application::pipelines::pipeline_import_registry::PipelineImportRegistry;
use crate::core::traits::dataset_trait::{ImporterDataset, ImporterDatasetV1};
use crate::core::traits::etl::DataFetcherJson;
// 🔥 Pastikan mengimpor ImporterDatasetV1
use crate::core::traits::load::{self, ImportLoadPersitence, IncrementalLoadPersitence};
use crate::core::traits::orchestrator::ImporterColumn;
use crate::core::traits::pipeline::pipeline_trait::PipelineImport;
use crate::core::traits::publisher::Publisher;
use crate::core::types::{
    CastingEnum, ImportBulkRequest, ImportDatasetConfig, ImportRow, ImportRowBulk,
    ImportTableResult, TableStrategy, WhereField,
};
use crate::domain::logging;
use crate::domain::logging::entity::{LoggingEntity, LoggingStatus, LoggingType};
use crate::domain::logging::repo::LoggingRepo;
use crate::infrastructure;
use crate::infrastructure::datasets::import;
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
    pub local_cache: Cache<String, serde_json::Value>,
    pub fetcher: Arc<DataFetcherJsonRegistry>,
    pub publisher: Arc<dyn Publisher + Send + Sync>,
}

impl ImportOrchestrator {
    async fn publish_auto_sync(
        &self,
        job: &OrchestratorJob,
        logging_id: &str,
    ) -> anyhow::Result<()> {
        let Some(sync_config) = &job.sync_config else {
            tracing::warn!(
                logging_id = logging_id,
                "auto_sync aktif tapi sync_config kosong, skip publish sync"
            );
            return Ok(());
        };

        let mut sync_config = sync_config.clone();
        sync_config.logging_id = Some(logging_id.to_string());

        let sync_job = OrchestratorJob {
            tenant_id: job.tenant_id.clone(),
            email: job.email.clone(),
            actor_id: job.actor_id.clone(),
            orchestrator_name: "SYNC".to_string(),
            export_config: None,
            ingestion_config: None,
            import_config: None,
            sync_config: Some(sync_config),
            filters: job.filters.clone(),
        };

        let payload = serde_json::to_value(&sync_job)?;
        let sync_aggregate = sync_job
            .sync_config
            .as_ref()
            .map(|config| config.sync_aggregate.as_str())
            .unwrap_or("-");

        tracing::info!(
            stream = "stream:sync",
            logging_id = logging_id,
            sync_aggregate = sync_aggregate,
            payload = %payload,
            "publishing auto sync payload"
        );

        match tokio::time::timeout(
            Duration::from_secs(5),
            self.publisher.publish("stream:sync", payload.clone()),
        )
        .await
        {
            Ok(Ok(_)) => {
                tracing::info!(
                    stream = "stream:sync",
                    logging_id = logging_id,
                    sync_aggregate = sync_aggregate,
                    payload = %payload,
                    "publish sync success"
                );
            }
            Ok(Err(err)) => {
                tracing::error!(
                    stream = "stream:sync",
                    logging_id = logging_id,
                    sync_aggregate = sync_aggregate,
                    error = %err,
                    payload = %payload,
                    "publish sync failed"
                );
                return Err(err);
            }
            Err(err) => {
                tracing::error!(
                    stream = "stream:sync",
                    logging_id = logging_id,
                    sync_aggregate = sync_aggregate,
                    error = %err,
                    payload = %payload,
                    "publish sync timeout"
                );
                return Err(anyhow::anyhow!("publish stream:sync timeout: {err}"));
            }
        }

        Ok(())
    }

    async fn pre_fetch_chunk_dependencies(
        &self,
        raw_rows: &[&[calamine::Data]], // 🔥 PERBAIKAN 1: Menggunakan calamine::Data (ini tipe enum konkretnya)
        dataset_config: &ImportDatasetConfig,
    ) -> anyhow::Result<()> {
        // 1. Kumpulkan token yang Cache Miss berdasarkan nama Fetcher
        let mut missing_fetcher_keys: HashMap<String, HashSet<(String, String)>> = HashMap::new();

        // 🔥 PERBAIKAN 2: Gunakan `.fields` karena field `.fetchers` tidak eksis di ImporterConfig Anda
        // Loop isi array fields untuk mencari mana kolom yang butuh fetcher
        for (col_idx, field_cfg) in dataset_config.import_config.fields.iter().enumerate() {
            // Asumsi: di dalam field_cfg Anda memiliki penanda nama fetcher yang valid
            // (Sesuaikan `field_cfg.fetcher_name` dengan nama properti asli di struct column field Anda, misal .fetcher atau .relation)
            let fetcher_name = match &field_cfg.fetcher {
                Some(name) if !name.trim().is_empty() => name.trim().to_string(),
                _ => continue, // Jika kolom ini tidak pakai fetcher, skip!
            };

            // Jika Anda memakai index eksplisit dari config gunakan itu,
            // jika tidak, gunakan `col_idx` dari posisi urutan array.
            let target_col = col_idx;

            for row in raw_rows {
                if let Some(cell_data) = row.get(target_col) {
                    // 🔥 PERBAIKAN 3: Cocokkan varian berdasarkan enum calamine::Data
                    let raw_token = match cell_data {
                        calamine::Data::String(s) => s.to_string(),
                        calamine::Data::Int(i) => i.to_string(),
                        calamine::Data::Float(f) => f.to_string(),
                        _ => cell_data.to_string(),
                    };

                    let token_string = raw_token.trim().to_string();
                    if !token_string.is_empty() {
                        let clean_fetcher = fetcher_name.clone();
                        let cache_key = format!("{}:{}", clean_fetcher, token_string);

                        if !self.local_cache.contains_key(&cache_key) {
                            missing_fetcher_keys
                                .entry(fetcher_name.clone())
                                .or_default()
                                .insert((token_string, cache_key));
                        }
                    }
                }
            }
        }

        // 2. Eksekusi Bulk Fetcher
        for (fetcher_name, tokens_and_keys) in missing_fetcher_keys {
            if tokens_and_keys.is_empty() {
                continue;
            }

            let tokens_vec: Vec<String> = tokens_and_keys.iter().map(|(t, _)| t.clone()).collect();

            tracing::info!(
                "🔥 BATCH FETCH MISS: Memanggil Fetcher '{}' untuk {} data...",
                fetcher_name,
                tokens_vec.len()
            );

            // Ambil instance fetcher dari engine registry Anda
            let fetcher = self.fetcher.get(&fetcher_name).map_err(|e| {
                anyhow::anyhow!("Fetcher '{}' tidak ditemukan: {}", fetcher_name, e)
            })?;

            let filter = vec![WhereField {
                field: "id".into(),
                operator: "=".into(),
                casting: Some(CastingEnum::Uuid),
                values: Some(tokens_vec),
                value: None,
            }];

            let rows = fetcher.fetch(Some(filter)).await?;

            for hit_row in rows {
                if let Some(id_val) = hit_row.get("id") {
                    if let Some(id_str) = id_val.as_str() {
                        let clean_fetcher = fetcher_name.to_uppercase();
                        let cache_key = format!("{}:{}", clean_fetcher, id_str.trim());

                        self.local_cache.insert(cache_key, hit_row.clone()).await;
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn process_stream_mode(
        &self,
        url: &str,
        import_pipeline: &dyn PipelineImport,
        dataset: &dyn ImporterDatasetV1,
        logging_id: &str,
        dataset_config: &ImportDatasetConfig,
        import_load_registry: &dyn ImportLoadPersitence,
    ) -> anyhow::Result<()> {
        let _ = url;
        let _ = import_pipeline;

        // Sementara matikan alur stream S3.
        let response = reqwest::get(url).await?;
        let mut byte_stream = response.bytes_stream();
        let tmp_file_path = format!("/tmp/import_{}.xlsx", logging_id);
        let mut tmp_file = File::create(&tmp_file_path).await?;
        tracing::info!(
            logging_id = logging_id,
            "Memulai streaming download dari S3 ke disk lokal..."
        );
        while let Some(chunk_result) = byte_stream.next().await {
            let chunk = chunk_result?;
            tmp_file.write_all(&chunk).await?;
        }
        tmp_file.flush().await?;
        drop(tmp_file);

        // let tmp_file_path = "template_excel_vacancy.xlsx";

        tracing::info!(
            logging_id = logging_id,
            file = tmp_file_path,
            "Mode stream sementara baca file lokal dari root project."
        );

        let mut workbook = calamine::open_workbook_auto(tmp_file_path).map_err(|err| {
            tracing::error!("Gagal membuka workbook lokal: {}", err);
            err
        })?;

        let sheet_name = workbook.sheet_names()[0].clone();
        let range = workbook.worksheet_range(&sheet_name)?;

        let chunk_limit = dataset_config.import_config.chunk as usize;

        let mut raw_row_buffer = vec![];
        let mut row_iter = range.rows().skip(1);

        loop {
            raw_row_buffer.clear();
            for _ in 0..chunk_limit {
                if let Some(row) = row_iter.next() {
                    raw_row_buffer.push(row);
                } else {
                    break;
                }
            }

            if raw_row_buffer.is_empty() {
                break;
            }

            if let Err(err) = self
                .pre_fetch_chunk_dependencies(&raw_row_buffer, dataset_config)
                .await
            {
                tracing::error!("Gagal menjalankan pre-fetch batch cache: {:?}", err);
            }

            let mut chunk_batch: Vec<HashMap<String, ImportTableResult>> = vec![];

            for row in &raw_row_buffer {
                let mut row_data = dataset
                    .import(*row, &dataset_config, &self.local_cache)
                    .await?;

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
            }

            if !chunk_batch.is_empty() {
                tracing::info!(
                    logging_id = logging_id,
                    "Flushing optimized stream chunk batch: {} rows",
                    chunk_batch.len()
                );

                import_load_registry
                    .load(&chunk_batch, logging_id, &dataset_config)
                    .await?;
            }
        }

        Ok(())
    }

    async fn process_buffer_mode(
        &self,
        url: &str,
        import_pipeline: &dyn PipelineImport,
        dataset: &dyn ImporterDatasetV1,
        logging_id: &str,
        dataset_config: &ImportDatasetConfig,
        import_load_registry: &dyn ImportLoadPersitence,
    ) -> anyhow::Result<()> {
        let response = reqwest::get(url).await?;
        info!("logging id {}", logging_id);

        let bytes = std::fs::read("template_excel_vacancy.xlsx")?;
        let cursor = Cursor::new(bytes);

        let mut workbook = open_workbook_auto_from_rs(cursor).map_err(|err| {
            tracing::error!("error open workbook {}", err);
            err
        })?;

        let sheet_name = workbook.sheet_names()[0].clone();
        let range = workbook.worksheet_range(&sheet_name)?;

        let chunk_limit = dataset_config.import_config.chunk as usize;
        let mut raw_row_buffer = vec![];
        let mut row_iter = range.rows().skip(1);

        loop {
            raw_row_buffer.clear();
            for _ in 0..chunk_limit {
                if let Some(row) = row_iter.next() {
                    raw_row_buffer.push(row);
                } else {
                    break;
                }
            }

            if raw_row_buffer.is_empty() {
                break;
            }

            // 🔥 OPTIMASI UTAMA: Jalankan Pre-fetch massal untuk Buffer Mode juga
            let _ = self
                .pre_fetch_chunk_dependencies(&raw_row_buffer, dataset_config)
                .await;

            let mut chunk_batch: Vec<HashMap<String, ImportTableResult>> = vec![];

            for row in &raw_row_buffer {
                let mut row_data: HashMap<String, ImportTableResult> = dataset
                    .import(*row, &dataset_config, &self.local_cache)
                    .await?;

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
            }

            if !chunk_batch.is_empty() {
                import_load_registry
                    .load(&chunk_batch, logging_id, &dataset_config)
                    .await?;
            }
        }

        info!("Buffer processing completely loaded into target temporary tables.");
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
                config: Some(serde_json::to_value(import_config.clone())?),
            };

            let logging_id = self.logging.store(logging_entity).await?;
            let import_load_registry = self.import_load_registry.get(&import_config.loader)?;

            let dataset = self.import_datasetV1.get(&import_config.dataset)?;
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
                        if import_config.auto_sync {
                            self.publish_auto_sync(job, &logging_id).await?;
                        }
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
                        if import_config.auto_sync {
                            self.publish_auto_sync(job, &logging_id).await?;
                        }
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
        self.local_cache.invalidate_all();
        Ok(())
    }
}
