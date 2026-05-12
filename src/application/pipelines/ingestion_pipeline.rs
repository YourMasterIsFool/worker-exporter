// use crate::core::datasets::dataset_runner::DatasetRunner;
// use crate::core::datasets::dataset_trait::DatasetTrait;
// use crate::core::exporter::export_writer::ExportWriter;
// use crate::core::pipelines::export_pipeline::ExportPipeline;
// use crate::core::tenants::tenant_strategy::TenantStrategy;
// use anyhow::Result;
// use async_trait::async_trait;
// use std::sync::Arc;

// pub struct Cna<D, W, T>
// where
//     D: DatasetRunner + Send + Sync,
//     W: ExportWriter + Send + Sync,
//     T: TenantStrategy + Send + Sync,
// {
//     pub tenant: T,
//     pub writer: W,
//     pub dataset: D,
// }

// #[async_trait::async_trait]
// impl<D, W, T> ExportPipeline for Cna<D, W, T>
// where
//     D: DatasetRunner + Send + Sync,
//     W: ExportWriter + Send + Sync,
//     T: TenantStrategy + Send + Sync,
// {
//     async fn run(&self) -> Result<()> {
//         Ok(())
//     }
// }

use std::sync::Arc;

use anyhow::{Ok, Result};
use uuid::Uuid;

use crate::core::traits::{
    extractor::incremental_extract::IncrementalExtract, pipeline::pipeline_trait::PipelineTrait,
};

pub struct IngestionPipeline {
    pub source: Arc<dyn IncrementalExtract + Send + Sync>,
}

#[async_trait::async_trait]
impl PipelineTrait for IngestionPipeline {
    async fn run(
        &self,
        cursor: Option<Uuid>,
        limit: i64,
    ) -> anyhow::Result<(Option<Uuid>, Vec<serde_json::Value>)> {
        let (rows, next_cursor) = self.source.extract(cursor, limit, None).await?;
        // simpan rows ke destination kalau perlu
        Ok((next_cursor, rows))
    }
}
