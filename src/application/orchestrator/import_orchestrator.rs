use calamine::{open_workbook_auto_from_rs, DataType, Reader};
use moka::future::Cache;
use std::collections::{HashMap, HashSet};
use std::result;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, instrument};
use tracing_subscriber::field;
use tracing_subscriber::fmt::format;
use tracing_subscriber::registry::Data;

use crate::application::dataset::{ImporterDatasetRegistry, ImporterDatasetRegistryV1};
use crate::application::fetch_source_regitry::DataFetcherJsonRegistry;
use crate::application::import::import_registry::ImportLoaderRegistry;
use crate::application::loader_registry::LoaderRegistry;
use crate::application::pipelines::pipeline_import_registry::PipelineImportRegistry;
use crate::core::traits::dataset_trait::ImporterDatasetV1;
use crate::core::traits::etl::DataFetcherJson;
// 🔥 Pastikan mengimpor ImporterDatasetV1
use crate::application::datasets::import;
use crate::core::traits::load::{self, ImportLoadPersitence, IncrementalLoadPersitence};
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
use crate::infrastructure::db::warehouse_client::WarehouseClient;
use crate::infrastructure::pipeline::ats::vacancy_import_pipeline;
use crate::utils::{resolve_template_column_indexes, resolve_template_row};
use crate::{
    application::grpcs::gprc::GrpcService,
    core::traits::orchestrator::{Orchestrator, OrchestratorJob},
};
use serde_json::{json, Value};
use std::io::Cursor;

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
    fn normalized_lookup_token(value: &str) -> String {
        value.trim().to_lowercase()
    }

    fn lookup_operator(casting: Option<&CastingEnum>) -> &'static str {
        match casting {
            Some(CastingEnum::Uuid)
            | Some(CastingEnum::Number)
            | Some(CastingEnum::Date)
            | Some(CastingEnum::Enum(_)) => "=",
            Some(CastingEnum::Varchar) | None => "ILIKE",
        }
    }

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
            permissions_key: job.permissions_key.clone(),
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
        // 1. Kumpulkan token yang Cache Miss berdasarkan konfigurasi fetcher.
        // Fetcher yang sama dapat dipakai oleh beberapa kolom dengan field/key
        // yang berbeda, sehingga konfigurasi tersebut menjadi bagian dari key map.
        let mut missing_fetcher_keys: HashMap<
            (String, String, String),
            (HashSet<(String, String)>, Option<CastingEnum>),
        > = HashMap::new();

        // 🔥 PERBAIKAN 2: Gunakan `.fields` karena field `.fetchers` tidak eksis di ImporterConfig Anda
        // Loop isi array fields untuk mencari mana kolom yang butuh fetcher
        for (col_idx, field_cfg) in dataset_config.import_config.fields.iter().enumerate() {
            // Asumsi: di dalam field_cfg Anda memiliki penanda nama fetcher yang valid
            // (Sesuaikan `field_cfg.fetcher_name` dengan nama properti asli di struct column field Anda, misal .fetcher atau .relation)
            let fetcher_name = match &field_cfg.fetcher {
                Some(name) if !name.trim().is_empty() => name.trim().to_string(),
                _ => continue, // Jika kolom ini tidak pakai fetcher, skip!
            };

            let fetcher_field_key = field_cfg
                .fetcher_field_key
                .as_deref()
                .map(str::trim)
                .filter(|field| !field.is_empty())
                .unwrap_or("id")
                .to_string();
            let fetcher_key = field_cfg
                .fetcher_key
                .as_deref()
                .map(str::trim)
                .filter(|field| !field.is_empty())
                .unwrap_or("id")
                .to_string();

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
                        let clean_fetcher = fetcher_name.to_uppercase();
                        let cache_key = format!(
                            "{}:{}",
                            clean_fetcher,
                            Self::normalized_lookup_token(&token_string)
                        );

                        if !self.local_cache.contains_key(&cache_key) {
                            let entry = missing_fetcher_keys
                                .entry((
                                    fetcher_name.clone(),
                                    fetcher_field_key.clone(),
                                    fetcher_key.clone(),
                                ))
                                .or_insert_with(|| (HashSet::new(), field_cfg.casting.clone()));
                            entry.0.insert((token_string, cache_key));
                        }
                    }
                }
            }
        }

        // 2. Eksekusi Bulk Fetcher
        for (
            (fetcher_name, fetcher_field_key, fetcher_key),
            (tokens_and_keys, configured_casting),
        ) in missing_fetcher_keys
        {
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

            let casting = configured_casting.or_else(|| {
                fetcher_field_key
                    .eq_ignore_ascii_case("id")
                    .then_some(CastingEnum::Uuid)
            });
            let filter = vec![WhereField {
                operator: Self::lookup_operator(casting.as_ref()).into(),
                casting,
                field: fetcher_field_key.clone(),
                values: Some(tokens_vec),
                value: None,
            }];

            let rows = fetcher.fetch(Some(filter)).await?;

            for hit_row in rows {
                let cache_values = [&fetcher_key, &fetcher_field_key]
                    .into_iter()
                    .filter_map(|field| hit_row.get(field))
                    .filter_map(|value| match value {
                        Value::String(value) if !value.trim().is_empty() => {
                            Some(value.trim().to_string())
                        }
                        Value::Null => None,
                        value => Some(value.to_string()),
                    })
                    .collect::<HashSet<_>>();

                for fetcher_value in cache_values {
                    let clean_fetcher = fetcher_name.to_uppercase();
                    let cache_key = format!(
                        "{}:{}",
                        clean_fetcher,
                        Self::normalized_lookup_token(&fetcher_value)
                    );

                    self.local_cache.insert(cache_key, hit_row.clone()).await;
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
        let _ = import_pipeline;

        
        let response = reqwest::get(url).await?.error_for_status()?;
        let bytes = response.bytes().await?;
        
        // let bytes = tokio::fs::read("template_import_v35.xlsx").await?;

        // tracing::info!(
        //     logging_id = logging_id,
        //     url = url,
        //     bytes = bytes.len(),
        //     "Membuka workbook import dari template lokal."
        // );

        let mut workbook = open_workbook_auto_from_rs(Cursor::new(bytes)).map_err(|err| {
            tracing::error!("Gagal membuka workbook dari response: {}", err);
            err
        })?;

        let sheet_name = workbook.sheet_names()[0].clone();
        let range = workbook.worksheet_range(&sheet_name)?;
        let header_row = range.rows().next().unwrap_or(&[]);
        let field_headers = dataset_config
            .import_config
            .fields
            .iter()
            .map(|field| field.header.as_str())
            .collect::<Vec<_>>();
        let template_column_indexes = resolve_template_column_indexes(header_row, &field_headers);

        tracing::info!(
            fields = ?dataset_config
                .import_config
                .fields
                .iter()
                .map(|field| field.header.as_str())
                .collect::<Vec<_>>(),
            column_indexes = ?template_column_indexes,
            "Resolved import values from workbook headers"
        );

        let chunk_limit = dataset_config.import_config.chunk as usize;

        let mut raw_row_buffer: Vec<Vec<calamine::Data>> = vec![];
        // `start_row` menggunakan nomor baris Excel (1-based). Default mulai
        // dari baris 2 agar baris pertama yang biasanya berisi header dilewati.
        let start_row = dataset_config.import_config.start_row.unwrap_or(2);
        let mut row_iter = range.rows().skip(start_row.saturating_sub(1) as usize);

        loop {
            raw_row_buffer.clear();
            for _ in 0..chunk_limit {
                if let Some(row) = row_iter.next() {
                    raw_row_buffer.push(resolve_template_row(row, &template_column_indexes));
                } else {
                    break;
                }
            }

            if raw_row_buffer.is_empty() {
                break;
            }

            let raw_row_refs = raw_row_buffer.iter().map(Vec::as_slice).collect::<Vec<_>>();

            if let Err(err) = self
                .pre_fetch_chunk_dependencies(&raw_row_refs, dataset_config)
                .await
            {
                tracing::error!("Gagal menjalankan pre-fetch batch cache: {:?}", err);
            }

            let mut chunk_batch = dataset
                .import_batch(&raw_row_refs, dataset_config, &self.local_cache)
                .await?;

            for row_data in &mut chunk_batch {
                for (_table_name, table_result) in row_data.iter_mut() {
                    if table_result.strategy == TableStrategy::Eav {
                        continue;
                    }
                    table_result.columns.push("logging_id".to_string());
                    for row_values in table_result.values.iter_mut() {
                        row_values.push(Value::String(logging_id.to_string()));
                    }
                }
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
        let response = reqwest::get(url).await?.error_for_status()?;
        info!("logging id {}", logging_id);

        let bytes = response.bytes().await?;
        let cursor = Cursor::new(bytes);

        let mut workbook = open_workbook_auto_from_rs(cursor).map_err(|err| {
            tracing::error!("error open workbook from response {}", err);
            err
        })?;

        let sheet_name = workbook.sheet_names()[0].clone();
        let range = workbook.worksheet_range(&sheet_name)?;

        let chunk_limit = dataset_config.import_config.chunk as usize;
        let mut raw_row_buffer = vec![];
        // `start_row` menggunakan nomor baris Excel (1-based). Default mulai
        // dari baris 2 agar baris pertama yang biasanya berisi header dilewati.
        let start_row = dataset_config.import_config.start_row.unwrap_or(2);
        let mut row_iter = range.rows().skip(start_row.saturating_sub(1) as usize);

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

            let mut chunk_batch = dataset
                .import_batch(&raw_row_buffer, dataset_config, &self.local_cache)
                .await?;

            for row_data in &mut chunk_batch {
                for (_table_name, table_result) in row_data.iter_mut() {
                    if table_result.strategy == TableStrategy::Eav {
                        continue;
                    }
                    table_result.columns.push("logging_id".to_string());
                    for row_values in table_result.values.iter_mut() {
                        row_values.push(Value::String(logging_id.to_string()));
                    }
                }
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
                // Simpan payload IMPORT lengkap agar halaman detail dapat memakai
                // konfigurasi asli (termasuk fields/template/sync_config) untuk
                // memetakan data temporary kembali ke header Excel.
                config: Some(serde_json::to_value(job)?),

                //creatd_at

                created_at: None
            };

            let logging_id = self.logging.store(logging_entity).await?;
            let import_load_registry = self.import_load_registry.get(&import_config.loader)?;

            let dataset = self.import_datasetV1.get(&import_config.dataset)?;
            let download_url = if import_config.data_source_type == "buffer" {
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

                items_download[0].url.clone()
            } else {
                // Sementara proses stream memakai workbook yang sudah ada di root
                // untuk pengecekan lokal, tanpa upload atau request URL baru.
                "template_import_v35.xlsx".to_string()
            };
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
                        dataset.as_ref(),
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
                        dataset.as_ref(),
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
