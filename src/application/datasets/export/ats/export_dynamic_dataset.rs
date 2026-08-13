use serde_json::Value;
use std::sync::Arc;

use crate::{
    application::{
        fetch_source_regitry::DataFetcherJsonRegistry, ingestion::source_registry::SourceRegistry,
    },
    core::{
        traits::{
            dataset_trait::{Dataset, ExportDataset},
            load::LoadEnum,
            orchestrator::ExporterColumn,
        },
        types::{ExportRow, WhereField},
    },
    utils::{export_value_to_string, resolve_export_value},
};
use crate::application::datasets::export::ats::applicants::applicant_buckets::applicant_dataset::ApplicantDataset;

/// Export dataset driven by the `export_config.fields` definition.
///
/// In the normal `Dataset` flow it delegates source, relation, fetcher, and
/// cache handling to the applicant loader. Its `ExportDataset` implementation
/// is the small mapper for rows that have already been resolved.
pub struct ExportDynamicDataset {
    delegate: ApplicantDataset,
}

impl ExportDynamicDataset {
    pub fn new(source: Arc<SourceRegistry>, fetcher: Arc<DataFetcherJsonRegistry>) -> Self {
        Self {
            delegate: ApplicantDataset { source, fetcher },
        }
    }

    pub fn format_rows(rows: &[Value], fields: &[ExporterColumn]) -> Vec<ExportRow> {
        rows.iter()
            .map(|row| {
                let sources = [("row", row)];
                let fields = fields
                    .iter()
                    .map(|field| {
                        field
                            .column
                            .as_deref()
                            .map(|column| {
                                export_value_to_string(resolve_export_value(column, &sources))
                            })
                            .unwrap_or_else(|| "-".to_string())
                    })
                    .collect();

                ExportRow { fields }
            })
            .collect()
    }
}

#[async_trait::async_trait]
impl Dataset for ExportDynamicDataset {
    async fn fetch_batch(
        &self,
        strategy_enum: &LoadEnum,
        cursor: Option<String>,
        limit: i64,
    ) -> anyhow::Result<(Vec<ExportRow>, Option<String>)> {
        self.delegate
            .fetch_batch(strategy_enum, cursor, limit)
            .await
    }

    async fn get_min_cursor(&self, filters: Option<Vec<WhereField>>) -> anyhow::Result<String> {
    }

    async fn fetch_batch_with_config(
        &self,
        strategy_enum: &LoadEnum,
        cursor: Option<String>,
        limit: i64,
        fields: &[ExporterColumn],
        cache: &moka::future::Cache<String, Value>,
    ) -> anyhow::Result<(Vec<ExportRow>, Option<String>)> {
        self.delegate
            .fetch_batch_with_config(strategy_enum, cursor, limit, fields, cache)
            .await
    }
}

#[async_trait::async_trait]
impl ExportDataset for ExportDynamicDataset {
    async fn format(
        &self,
        rows: &[Value],
        fields: &[ExporterColumn],
    ) -> anyhow::Result<Vec<ExportRow>> {
        Ok(Self::format_rows(rows, fields))
    }
}
