use anyhow::Result;
use std::clone::Clone;

use crate::core::traits::writter::writter_trait::ExportRow;

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
        cursor: Option<String>,
        limit: i64,
    ) -> Result<(Vec<ExportRow>, Option<String>)>;
}
