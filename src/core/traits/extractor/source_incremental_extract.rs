use ::anyhow::Result;
use uuid::Uuid;

use crate::core::traits::load::LoadEnum;

#[async_trait::async_trait]
pub trait SourceIncrementalExtract {
    async fn extract(
        &self,
        cursor: Option<Uuid>,
        limit: i64,
        strategy_enum: &LoadEnum,
    ) -> Result<(Vec<serde_json::Value>, Option<Uuid>)>;
}
