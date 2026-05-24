use std::sync::Arc;

use anyhow::Result;

use crate::core::traits::extractor::incremental_source::WhereField;

pub enum LoadEnum {
    Upsert,
    Insert,
    Update(Vec<String>),
    Select(Option<Vec<WhereField>>),
}
#[async_trait::async_trait]
pub trait IncrementalLoad: Send + Sync {
    async fn load(&self, rows: &[serde_json::Value]) -> Result<i64>;
}

pub trait LoaderWarehouse: Send + Sync {
    fn table(&self) -> &'static str;
    fn columns(&self) -> &'static [&'static str];
    fn conflicted_columns(&self) -> &'static [&'static str];
}

#[async_trait::async_trait]
impl<T: IncrementalLoad + ?Sized + Send + Sync> IncrementalLoad for Arc<T> {
    async fn load(&self, rows: &[serde_json::Value]) -> Result<i64> {
        (**self).load(rows).await
    }
}
