use std::sync::Arc;

use anyhow::Result;
use uuid::Uuid;

use crate::{
    application::ingestion::source_registry::SourceRegistry,
    core::traits::{pipeline::pipeline_trait::PipelineTrait, publisher::Publisher},
};

pub struct IngestionPipeline {
    pub source: Arc<SourceRegistry>,
    pub publisher: Arc<dyn Publisher>,
}

#[async_trait::async_trait]
impl PipelineTrait for IngestionPipeline {
    async fn run(
        &self,
        cursor: Option<Uuid>,
        limit: i64,
        source: &str,
        updated_after: Option<chrono::NaiveDateTime>,
    ) -> Result<(Option<Uuid>, Vec<serde_json::Value>)> {
        let source_registry = self.source.get(source)?;
        let (rows, next_cursor) = source_registry
            .extract_updated_after(updated_after, limit)
            .await?;

        Ok((next_cursor, rows))
    }

    async fn count_new_data(
        &self,
        source: &str,
        updated_after: Option<chrono::NaiveDateTime>,
    ) -> Result<i64> {
        self.source
            .get(source)?
            .count_updated_after(updated_after)
            .await
    }

    async fn after_load(&self, loader: &str) -> Result<()> {
        if loader == "OLAP_ATS_VACANCY_DIMENSION" {
            self.publisher
                .publish(
                    "stream:aggregate",
                    serde_json::json!({
                        "tenant_id": "GLOBAL",
                        "email": "system@worker",
                        "actor_id": "system",
                        "orchestrator_name": "AGGREGATE",
                        "aggregate_config": {
                            "pipeline": "VACANCY_AGGREGATE",
                            "aggregate_code": "VACANCY_TOTAL",
                            "scope_key": "GLOBAL"
                        }
                    }),
                )
                .await?;
        }
        Ok(())
    }
}
