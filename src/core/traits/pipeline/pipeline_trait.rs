use std::collections::HashMap;

use anyhow::Result;
use uuid::Uuid;

use crate::core::types::{ImportRow, ImportRowBulk};

#[async_trait::async_trait]
pub trait PipelineTrait: Send + Sync {
    async fn run(
        &self,
        cursor: Option<Uuid>,
        limit: i64,
        source: &str,
    ) -> Result<(Option<Uuid>, Vec<serde_json::Value>)>;
}

#[async_trait::async_trait]
pub trait PipelineImport: Send + Sync {
    async fn import(
        &self,
        data: &HashMap<String, Vec<ImportRowBulk>>,
        logging_id: &str,
    ) -> Result<()>;
}
