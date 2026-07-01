use std::collections::HashMap;

use anyhow::Result;
use excelstream::streaming_reader::RowIterator;

use crate::core::{
    traits::{load::LoadEnum, writter::writter_trait::ExportRow},
    types::WhereField,
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

pub struct ImportRow {
    pub field: String,
    pub value: serde_json::Value,
}

#[async_trait::async_trait]
pub trait ImporterDataset {
    async fn import(
        &self,
        rows: &[RowIterator],
        mapper_columns: &HashMap<i32, String>,
    ) -> anyhow::Result<HashMap<String, Vec<ImportRow>>>;
}
