use serde::Serialize;
use serde_json::Value;
use sqlx::{postgres::PgRow, FromRow, PgPool, Postgres, QueryBuilder};
use uuid::Uuid;

use crate::{core::traits::etl::fetch_data_json, core::types::WhereField};

pub async fn fetch_temporary_rows<T>(
    pool: &PgPool,
    table: &str,
    columns: &[&str],
    filters: Option<Vec<WhereField>>,
) -> anyhow::Result<Vec<Value>>
where
    T: for<'row> FromRow<'row, PgRow> + Serialize + Unpin + Send,
{
    let mut query = QueryBuilder::new(format!("SELECT {} FROM {table}", columns.join(", ")));
    fetch_data_json::<T>(&mut query, filters, pool, None).await
}

pub async fn extract_temporary_rows<T, IdOf>(
    pool: &PgPool,
    table: &str,
    columns: &[&str],
    cursor: Option<Uuid>,
    limit: i64,
    id_of: IdOf,
) -> anyhow::Result<(Vec<Value>, Option<Uuid>)>
where
    T: for<'row> FromRow<'row, PgRow> + Serialize + Unpin + Send,
    IdOf: Fn(&T) -> Uuid,
{
    let mut query: QueryBuilder<Postgres> =
        QueryBuilder::new(format!("SELECT {} FROM {table}", columns.join(", ")));
    if let Some(cursor) = cursor {
        query.push(" WHERE id > ").push_bind(cursor);
    }
    query.push(" ORDER BY id ASC LIMIT ").push_bind(limit);

    let records = query.build_query_as::<T>().fetch_all(pool).await?;
    let next_cursor = records.last().map(&id_of);
    let rows = records
        .iter()
        .map(serde_json::to_value)
        .collect::<Result<Vec<_>, _>>()?;

    Ok((rows, next_cursor))
}
