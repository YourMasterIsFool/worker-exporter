use std::sync::Arc;

use uuid::Uuid;

use crate::{
    application::{
        bulk_loader_registry::BulkLoaderRegistry, dataset::SyncDatasetRegistry,
        ingestion::source_registry::SourceRegistry, runner::sync_runner::SyncRunner,
    },
    core::{
        traits::load::SyncAggregator,
        types::{SyncPayloadDataset, WhereField},
    },
    domain::logging::repo::LoggingRepo,
    infrastructure::db::ats_client::AtsClient,
};

pub struct AtsVacancyAtsSyncAggregator {
    pub source: Arc<SourceRegistry>,
    pub dataset: Arc<SyncDatasetRegistry>,
    pub bulk_loader: Arc<BulkLoaderRegistry>,
    pub ats_client: Arc<AtsClient>,
    pub logging: Arc<dyn LoggingRepo>,
}

impl AtsVacancyAtsSyncAggregator {
    const SOURCE: &'static str = "warehouse_ats_vacancy_temporary";
    const DATASET: &'static str = "LSI_VACANCY_SYNC_DATASET";
    const LOADERS: &'static [&'static str] = &[
        "ATS_VACANCY",
        "ATS_VACANCY_CUSTOM_FIELD",
        "ATS_VACANCY_PIC",
        "ATS_VACANCY_STAGE",
        "ATS_VACANCY_REQUIREMENT",
    ];
}

#[async_trait::async_trait]
impl SyncAggregator for AtsVacancyAtsSyncAggregator {
    #[tracing::instrument(
        skip(self, payload),
        fields(types = "sync_aggregator", sync_aggregate = "LSI_VACANCY_SYNC", logging_id = %logging_id)
    )]
    async fn run(&self, payload: &SyncPayloadDataset, logging_id: &str) -> anyhow::Result<i64> {
        // Logging awal untuk tracing
        tracing::info!(
            logging_id = logging_id,
            "Memulai proses sinkronisasi AtsVacancy"
        );

        let source = self.source.get(Self::SOURCE).map_err(|e| {
            tracing::error!(
                logging_id = logging_id,
                "Gagal mendapatkan source '{}': {:?}",
                Self::SOURCE,
                e
            );
            e
        })?;

        let dataset = self.dataset.get(Self::DATASET).map_err(|e| {
            tracing::error!(
                logging_id = logging_id,
                "Gagal mendapatkan dataset '{}': {:?}",
                Self::DATASET,
                e
            );
            e
        })?;

        let filters = vec![WhereField {
            field: "logging_id".into(),
            operator: "=".into(),
            casting: Some(crate::core::types::CastingEnum::Uuid),
            values: Some(vec![logging_id.into()]),
            value: None,
        }];

        // Mengambil cursor dengan error handling
        let cursor_str = source
            .get_min_id(Some(filters.clone()))
            .await
            .map_err(|e| {
                tracing::error!(
                    logging_id = logging_id,
                    "Gagal mengambil min_id dari source: {:?}",
                    e
                );
                e
            })?
            .unwrap_or_default();

        if cursor_str.is_empty() {
            tracing::warn!(
                logging_id = logging_id,
                "Tidak ada vacancy temporary untuk logging_id ini"
            );
            return Ok(0);
        }

        Uuid::parse_str(&cursor_str).map_err(|e| {
            tracing::error!(
                logging_id = logging_id,
                "Gagal parsing UUID cursor '{}': {:?}",
                cursor_str,
                e
            );
            e
        })?;

        let mut uuid_cursor = None;

        tracing::info!(
            logging_id = logging_id,
            min_id = cursor_str,
            "Min id vacancy temporary ditemukan"
        );

        let limit = if payload.config.chunk > 0 {
            payload.config.chunk
        } else {
            100
        };

        // Menjalankan runner dengan error handling
        let result = SyncRunner::run(
            source.as_ref(),
            dataset.as_ref(),
            self.bulk_loader.as_ref(),
            self.ats_client.as_ref(),
            self.logging.as_ref(),
            logging_id,
            payload,
            &mut uuid_cursor,
            limit,
            Self::LOADERS,
            Some(filters),
        )
        .await
        .map_err(|e| {
            tracing::error!(
                logging_id = logging_id,
                "SyncRunner gagal mengeksekusi sinkronisasi: {:?}",
                e
            );
            e
        })?;

        tracing::info!(
            logging_id = logging_id,
            "Sinkronisasi selesai. Baris diproses: {}",
            result.processed_count
        );

        Ok(result.processed_count)
    }
}
