use ::anyhow::Result;
use chrono::NaiveDateTime;
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

    async fn extract_updated_after(
        &self,
        _updated_after: Option<NaiveDateTime>,
        _limit: i64,
    ) -> Result<(Vec<serde_json::Value>, Option<Uuid>)> {
        Err(anyhow::anyhow!(
            "source does not support updated_at extraction"
        ))
    }

    async fn count_updated_after(&self, _updated_after: Option<NaiveDateTime>) -> Result<i64> {
        Err(anyhow::anyhow!(
            "source does not support updated_at counting"
        ))
    }
}
