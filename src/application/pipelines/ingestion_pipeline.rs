use std::sync::Arc;

use anyhow::Ok;

use uuid::Uuid;

use crate::{
    application::source::source_registry::SourceRegistry,
    core::traits::pipeline::pipeline_trait::PipelineTrait,
};

pub struct IngestionPipeline {
    pub source: Arc<SourceRegistry>,
}

#[async_trait::async_trait]
impl PipelineTrait for IngestionPipeline {
    async fn run(
        &self,
        cursor: Option<Uuid>,
        limit: i64,
        source: &str,
    ) -> anyhow::Result<(Option<Uuid>, Vec<serde_json::Value>)> {
        // simpan rows ke destination kalau perlu

        let source = self.source.get(source)?;

        let (result, cursor) = source.extract(cursor, limit, None).await?;
        Ok((cursor, result))
    }
}
