use std::collections::HashMap;

use anyhow::Result;
use chrono::NaiveDateTime;
use uuid::Uuid;

use crate::core::{
    traits::orchestrator::AggregateConfig,
    types::{ImportDatasetConfig, ImportRow, ImportRowBulk, ImportTableResult},
};

#[async_trait::async_trait]
pub trait PipelineTrait: Send + Sync {
    async fn run(
        &self,
        cursor: Option<Uuid>,
        limit: i64,
        source: &str,
        updated_after: Option<NaiveDateTime>,
    ) -> Result<(Option<Uuid>, Vec<serde_json::Value>)>;

    async fn count_new_data(
        &self,
        source: &str,
        updated_after: Option<NaiveDateTime>,
    ) -> Result<i64>;

    async fn after_load(&self, loader: &str) -> Result<()> {
        let _ = loader;
        Ok(())
    }
}

#[async_trait::async_trait]
pub trait PipelineImport: Send + Sync {
    async fn load(
        &self,
        rows: &[HashMap<String, ImportTableResult>],
        logging_id: &str,
        config: &ImportDatasetConfig,
    ) -> Result<i64>;
}

#[async_trait::async_trait]
pub trait RollbackPipeline: Send + Sync {
    async fn rollback(&self, logging_id: &str, loaders: &[String]) -> Result<i64>;
}

#[async_trait::async_trait]
pub trait AggregatePipeline: Send + Sync {
    async fn run(&self, config: &AggregateConfig) -> Result<i64>;
}
