use std::collections::HashMap;

use anyhow::{anyhow, Result};
use calamine::Data;
use moka::future::Cache;

use crate::core::{
    traits::{
        load::LoadEnum,
        orchestrator::{ImporterColumn, ImporterConfig},
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
