use std::sync::Arc;

use anyhow::Ok;
use uuid::Uuid;

use crate::core::traits::{
    extractor::source_incremental_extract::SourceIncrementalExtract,
    pipeline::pipeline_trait::PipelineTrait,
};

pub struct ExportPipeline {
    pub source: Arc<dyn SourceIncrementalExtract + Send + Sync>,
}

#[async_trait::async_trait]
impl PipelineTrait for ExportPipeline {
    async fn run(
        &self,
        cursor: Option<Uuid>,
        limit: i64,
        source: &str,
    ) -> anyhow::Result<(Option<Uuid>, Vec<serde_json::Value>)> {
        let (rows, next_cursor) = self.source.extract(cursor, limit, None).await?;
        // simpan rows ke destination kalau perlu
        Ok((next_cursor, rows))
    }
}
