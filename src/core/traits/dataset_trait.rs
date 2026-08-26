use std::collections::HashMap;

use anyhow::{anyhow, Result};
use calamine::Data;
use moka::future::Cache;

use crate::core::{
    traits::{
        load::LoadEnum,
        orchestrator::{DynamicExportConfig, ExporterColumn, ImporterColumn, ImporterConfig},
    },
    types::{
        ExportRow, ImportDatasetConfig, ImportTableResult, SycnTableResult, SyncPayloadDataset,
        WhereField,
    },
};

#[async_trait::async_trait]
pub trait DatasetTrait {
    async fn fetch_batch(
        &self,
        watermark: Option<i64>,
        limit: i64,
    ) -> Result<(Vec<ExportRow>, Option<i64>)>;
}

#[async_trait::async_trait]
pub trait Dataset {
    async fn fetch_batch(
        &self,
        strategy_enum: &LoadEnum,
        cursor: Option<String>,
        limit: i64,
    ) -> Result<(Vec<ExportRow>, Option<String>)>;

    async fn get_min_cursor(&self, filters: Option<Vec<WhereField>>) -> anyhow::Result<String>;

    /// Returns a snapshot count when a static dataset can provide one.
    /// `None` preserves the legacy behavior for datasets that do not support totals.
    async fn count(&self, _strategy_enum: &LoadEnum) -> anyhow::Result<Option<i64>> {
        Ok(None)
    }

    async fn configure_export(
        &self,
        _fields: &[ExporterColumn],
        _cache: &Cache<String, serde_json::Value>,
    ) -> Result<()> {
        Ok(())
    }

    async fn fetch_batch_with_dynamic_export_config(
        &self,
        strategy_enum: &LoadEnum,
        cursor: Option<String>,
        limit: i64,
        _config: &DynamicExportConfig,
    ) -> Result<(Vec<ExportRow>, Option<String>)> {
        self.fetch_batch(strategy_enum, cursor, limit).await
    }

    async fn count_with_dynamic_export_config(
        &self,
        _strategy_enum: &LoadEnum,
        _config: &DynamicExportConfig,
    ) -> anyhow::Result<i64> {
        anyhow::bail!("dataset does not support dynamic export counts")
    }
}

#[async_trait::async_trait]
pub trait ImporterDataset: Send + Sync {
    async fn import(
        &self,
        rows: &[Data],
        config: &ImportDatasetConfig,
    ) -> anyhow::Result<HashMap<String, Vec<crate::core::types::ImportRow>>>;
}
#[async_trait::async_trait]
pub trait ImporterDatasetV1: Send + Sync {
    async fn import(
        &self,
        rows: &[Data],
        config: &ImportDatasetConfig,
        cache: &Cache<String, serde_json::Value>,
    ) -> anyhow::Result<HashMap<String, ImportTableResult>>;

    async fn import_batch(
        &self,
        rows: &[&[Data]],
        config: &ImportDatasetConfig,
        cache: &Cache<String, serde_json::Value>,
    ) -> anyhow::Result<Vec<HashMap<String, ImportTableResult>>> {
        let mut result = Vec::with_capacity(rows.len());

        for row in rows {
            result.push(self.import(row, config, cache).await?);
        }

        Ok(result)
    }
}

#[async_trait::async_trait]
pub trait SyncDataset: Send + Sync {
    async fn format(&self, payload: &SyncPayloadDataset) -> anyhow::Result<SycnTableResult>;

    async fn format_from_master(
        &self,
        payload: &SyncPayloadDataset,
        _master_rows: Vec<serde_json::Value>,
    ) -> anyhow::Result<SycnTableResult> {
        self.format(payload).await
    }
}
