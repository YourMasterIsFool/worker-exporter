use std::collections::HashMap;

use anyhow::Result;
use chrono::NaiveDateTime;
use uuid::Uuid;

use crate::core::types::{ImportRow, ImportRowBulk};

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
}

#[async_trait::async_trait]
pub trait PipelineImport: Send + Sync {
    async fn import(
        &self,
        data: &HashMap<String, Vec<ImportRowBulk>>,
        logging_id: &str,
    ) -> Result<()>;
}
