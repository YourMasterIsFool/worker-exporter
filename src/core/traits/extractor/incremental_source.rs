use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::core::traits::load::LoadEnum;

//buat incremental extract
//example id>watermark atau pakai date juga bisa > cursor_date
//
//

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WhereField {
    pub field: String,
    pub operator: String,
    pub value: String,
}

#[async_trait::async_trait]
pub trait IncrementalSource {
    type Cursor: Clone;
    type Output;

    async fn fetch(
        &self,
        strategy_enum: &LoadEnum,
        limit: i64,
        cursor: Option<Self::Cursor>,
    ) -> Result<Vec<Self::Output>>;
}
