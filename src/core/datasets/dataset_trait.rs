use anyhow::Result;
use std::clone::Clone;

use crate::core::traits::exporter::export_writer::ExportRow;

#[async_trait::async_trait]
pub trait DatasetTrait {
    async fn fetch_batch(
        &self,
        watermark: Option<i64>,
        limit: i64,
    ) -> Result<(Vec<ExportRow>, Option<i64>)>;
}
