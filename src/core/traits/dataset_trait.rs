use std::collections::HashMap;

use anyhow::Result;
use calamine::Data;

use crate::core::{
    traits::{
        load::LoadEnum,
        orchestrator::{ImporterColumn, ImporterConfig},
    },
    types::{ExportRow, ImportDatasetConfig, ImportTableResult, WhereField},
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
    ) -> anyhow::Result<HashMap<String, ImportTableResult>>;
}
