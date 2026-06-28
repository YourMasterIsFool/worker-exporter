use anyhow::Result;

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
