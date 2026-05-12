use std::result;

use anyhow::{Ok, Result};
use sqlx::{postgres::PgPoolOptions, PgPool, QueryBuilder};

use crate::core::traits::extractor::build_query_trait::BuildQueryTrait;

pub struct TenantPoolQuerySnapshot {
    pool: PgPool,
}
