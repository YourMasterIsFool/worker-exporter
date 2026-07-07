use ::anyhow::Result;
use uuid::Uuid;

use crate::core::{traits::load::LoadEnum, types::WhereField};

#[async_trait::async_trait]
pub trait SourceIncrementalExtract {
    async fn extract(
        &self,
        cursor: Option<Uuid>,
        limit: i64,
        strategy_enum: &LoadEnum,
    ) -> Result<(Vec<serde_json::Value>, Option<Uuid>)>;

    async fn get_min_id(&self, filters: Option<Vec<WhereField>>) -> anyhow::Result<Option<String>> {
        Ok(None)
    }
}
