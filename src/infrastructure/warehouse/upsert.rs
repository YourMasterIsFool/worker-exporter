use std::marker::PhantomData;

use anyhow::{Ok, Result};
use sqlx::QueryBuilder;

pub struct UpsertConfig {
    pub table: &'static str,
    pub columns: &'static [&'static str],
    pub conflict_columns: &'static [&'static str],
    pub updated_columns: &'static [&'static str],
}

pub struct Upsert<'a, T> {
    config: &'a UpsertConfig,
    _marker: PhantomData<T>,
}

#[async_trait::async_trait]
pub trait UpsertTrait<T> {
    async fn bulk(&mut self, rows: &[T]) -> Result<()>;
}
