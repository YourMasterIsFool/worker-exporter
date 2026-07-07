use std::{collections::HashMap, sync::Arc};

use crate::core::types::{
    ImportDatasetConfig, ImportRow, ImportRowBulk, ImportTableResult, SycnTableResult,
    SyncPayloadDataset, WhereField,
};
use anyhow::Result;
use sqlx::{Postgres, Transaction};

pub enum LoadEnum {
    Upsert,
    Insert,
    Update(Vec<String>),
    Select(Option<Vec<WhereField>>, Option<Vec<String>>),
}

impl LoadEnum {
    pub fn is_select(&self) -> bool {
        matches!(self, LoadEnum::Select(_, _))
    }
}
#[async_trait::async_trait]
pub trait IncrementalLoadPersitence: Send + Sync {
    async fn load(&self, rows: &[serde_json::Value]) -> Result<i64>;
}

#[async_trait::async_trait]
pub trait ImportLoadPersitence: Send + Sync {
    async fn load(
        &self,
        // 🔥 Perbaikan: Diubah dari ImportRowBulk menjadi ImportTableResult
        rows: &[HashMap<String, ImportTableResult>],
        logging_id: &str, // Sekaligus perbaikan typo nama variable 'loggin_id' -> 'logging_id'
        config: &ImportDatasetConfig,
    ) -> Result<i64>;
}

pub trait LoaderPersitence: Send + Sync {
    fn table(&self) -> &'static str;
    fn columns(&self) -> &'static [&'static str];
    fn conflicted_columns(&self) -> &'static [&'static str];
}

#[async_trait::async_trait]
impl<T: IncrementalLoadPersitence + ?Sized + Send + Sync> IncrementalLoadPersitence for Arc<T> {
    async fn load(&self, rows: &[serde_json::Value]) -> Result<i64> {
        (**self).load(rows).await
    }
}

#[async_trait::async_trait]
pub trait BulkLoaderTrait: Send + Sync {
    async fn load(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        result: &SycnTableResult,
    ) -> Result<i64>;
}

#[async_trait::async_trait]
pub trait SyncAggregator: Send + Sync {
    async fn run(&self, payload: &SyncPayloadDataset, stream_id: &str) -> Result<i64>;
}
