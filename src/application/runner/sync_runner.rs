use uuid::Uuid;

use crate::{
    application::bulk_loader_registry::BulkLoaderRegistry,
    core::{
        traits::{
            dataset_trait::SyncDataset,
            extractor::source_incremental_extract::SourceIncrementalExtract, load::LoadEnum,
        },
        types::SyncPayloadDataset,
    },
    domain::logging::repo::LoggingRepo,
    infrastructure::db::ats_client::AtsClient,
};

pub struct SyncRunner;

pub struct SyncRunnerResult {
    pub processed_count: i64,
    pub cursor: Option<Uuid>,
}

impl SyncRunner {
    #[tracing::instrument(
        skip(source, dataset, bulk_loader_registry, ats_client, logging, payload, cursor, filters),
        fields(types = "sync_runner", stream_id = %stream_id, limit = %limit)
    )]
    pub async fn run<S, D, L>(
        source: &S,
        dataset: &D,
        bulk_loader_registry: &BulkLoaderRegistry,
        ats_client: &AtsClient,
        logging: &L,
        stream_id: &str,
        payload: &SyncPayloadDataset,
        cursor: &mut Option<Uuid>,
        limit: i64,
        loader_names: &[&str],
        filters: Option<Vec<crate::core::types::WhereField>>,
    ) -> anyhow::Result<SyncRunnerResult>
    where
        S: SourceIncrementalExtract + Send + Sync + ?Sized,
        D: SyncDataset + Send + Sync + ?Sized,
        L: LoggingRepo + Send + Sync + ?Sized,
    {
        let mut processed_count = 0;

        loop {
            let (master_rows, next_cursor) = source
                .extract(*cursor, limit, &LoadEnum::Select(filters.clone(), None))
                .await?;

            if master_rows.is_empty() {
                break;
            }

            let sync_result = dataset.format_from_master(payload, master_rows).await?;
            let mut tx = ats_client.pool.begin().await?;

            for loader_name in loader_names {
                let loader = bulk_loader_registry.get(loader_name)?;
                processed_count += loader.load(&mut tx, &sync_result).await?;
            }

            tx.commit().await?;

            *cursor = next_cursor;
            logging
                .update_cursor_and_processed_count(
                    stream_id,
                    cursor.map(|cursor| cursor.to_string()),
                    processed_count,
                )
                .await?;

            if cursor.is_none() {
                break;
            }
        }

        Ok(SyncRunnerResult {
            processed_count,
            cursor: *cursor,
        })
    }
}
