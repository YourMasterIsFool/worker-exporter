use std::fmt::Debug;

use serde::Serialize;
use sqlx::{postgres::PgRow, FromRow, PgPool, Postgres, QueryBuilder};
use tracing::debug;
use uuid::Uuid;
use worker_exporter::core::types::WhereField;
use crate::core::types::{CastingEnum, Identifierable};

pub async fn resolve_query_builder_incremental_cursor<T>(
    qb: &mut QueryBuilder<'_, Postgres>,
    cursor: Option<String>,
    limit: i64,
    pool: &PgPool,
) -> anyhow::Result<(Vec<serde_json::Value>, Option<Uuid>)>
where
    T: for<'q> FromRow<'q, PgRow> + Identifierable + Serialize + Send + Unpin + Debug,
{
    if let Some(c) = cursor {
        qb.push("  AND id > ").push_bind(c);
    }
    qb.push(" LIMIT ").push_bind(limit);

    let result = qb.build_query_as::<T>().fetch_all(pool).await;

    match result {
        Ok(result) => {
            debug!("result candidate ingestion : {:?}", result.iter().clone());
            let last_cursor: Option<Uuid> = result.last().map(|last| last.get_id());

            tracing::debug!("result ingestion: {:?}", result.iter().clone());
            let parse_result = result
                .iter()
                .map(serde_json::to_value)
                .collect::<std::result::Result<Vec<serde_json::Value>, _>>()?;

            tracing::debug!(" candidate source {:?}", parse_result.clone());
            Ok((parse_result, last_cursor))
        }
        Err(e) => {
            debug!("errr {:?}", e);
            Err(e.into())
        }
    }
}


pub struct IncrementalCursorResolver<'q, T> {
    qb: QueryBuilder<'q, Postgres>,
    pool: &'q PgPool,
    limit: i64,
    source_debug: &'q str,
    _marker: std::marker::PhantomData<T>,

}

// -------------------------------------------------------------------------
// 3. IMPLEMENTATION BLOCK (Logika Utama)
// -------------------------------------------------------------------------

impl<'q, T> IncrementalCursorResolver<'q, T>
where
    T: for<'r> FromRow<'r, PgRow> + Identifierable + serde::Serialize + Send + Unpin + 'static,
{
    pub fn new(qb: QueryBuilder<'q, Postgres>, pool: &'q PgPool, default_limit: i64, source_debug: &'q str) -> Self {
        let mut query = qb;
        query.push(" WHERE 1=1 ");
        Self {
            qb: query,
            pool,
            limit: default_limit,
            source_debug,
            _marker: std::marker::PhantomData,
        }
    }

    // Mengintegrasikan WhereField langsung di dalam method struct
    pub fn with_filters(mut self, where_fields: &Option<Vec<WhereField>>) -> Self {

        if let Some(where_fields) = where_fields {
            for filter in where_fields {
                let casting = format!("::{}", filter.casting.unwrap_or(CastingEnum::Varchar).as_str());

                // Single value handling
                if let Some(value) = filter.value {
                    self.qb.push(format!(" AND {} {} ", filter.field, filter.operator));
                    self.qb.push_bind(value);
                    self.qb.push(&casting);
                }

                // Multiple values handling (IN)
                if let Some(values) = filter.values {
                    if !values.is_empty() {
                        self.qb.push(format!(" AND {} {} (", filter.field, filter.operator));
                        let mut separated = self.qb.separated(", ");
                        for val in values {
                            separated.push_bind(val);
                            separated.push_unseparated(&casting);
                        }
                        self.qb.push(")");
                    }
                }
            }
        }

        self
    }

    pub fn with_cursor(mut self, cursor: Option<Uuid>) -> Self {
        if let Some(c) = cursor {
            self.qb.push(" AND id > ").push_bind(c);
        }
        self
    }

    pub async fn resolve(mut self) -> anyhow::Result<(Vec<serde_json::Value>, Option<Uuid>)> {
        self.qb.push(" LIMIT ").push_bind(self.limit);
        tracing::info!("running resolve {}", self.source_debug);
        let result = self.qb
            .build_query_as::<T>()
            .fetch_all(self.pool)
            .await.map_err(|err|{
            tracing::error!("error {} resolve {} ", err, self.source_debug);
            err
        });

        let last_cursor = result.last().map(|last| last.get_id());

        let parse_result = result
            .iter()
            .map(serde_json::to_value)
            .collect::<std::result::Result<Vec<serde_json::Value>, _>>()?;

        tracing::info!("sucessfully resolved {:?}", self.source_debug);

        Ok((parse_result, last_cursor))
    }
}
