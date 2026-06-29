use std::{any::Any, collections::HashSet};

use sqlx::{prelude::FromRow, PgPool, Pool, Postgres, QueryBuilder};

use crate::core::types::WhereField;

// `TypedExtractor` adalah sebuah trait yang digunakan untuk pengambilan data fetching
// bersifat unplugable  untuk OLAP/INGESTION
// pengambilan data secara cursor Paginated
//
// *Implementasi
//  setiap yang menggunakan trait ini harus mendfinisikan `Send + Sync` dan pastikan safety ketika
//  dikirim dan dikonsumsi antar thread

#[async_trait::async_trait]
pub trait TypedExtractor: Send + Sync {
    // `type Output` sebuah generic yang akan digunakan untuk hasil data dari fetching pastikan memakai 'static agar dapat
    // lifetime bersifat 'longlive'
    type Output: 'static + Send + Sync;

    // `type Cursor` sebuah generic yang digunakan sebagai type data yang dimiliki cursor
    type Cursor: 'static + Send + Sync;

    // pool: sebuah parameter untuk menerima database connection
    // curor: paremeter untuk menerima paginated cursor
    // limi: parameer untuk mengambil data secara limit/paginated maximum jumlah datay
    async fn fetch(
        &self,
        pool: &PgPool,
        cursor: Option<Self::Cursor>,
        limit: i64,
    ) -> anyhow::Result<Vec<Self::Output>>;
}

// `DataExtractor` adalah sebuah trait digunakan untuk mengambil langsung kedalam db OLTP,
// bersifat unplugable
// pengambilan data secara Cursor Paaginated

#[async_trait::async_trait]
pub trait DataExtractor: Send + Sync {
    // pool: sebuah parameter untuk menerima database connection
    // curor: paremeter untuk menerima paginated cursor
    // limi: parameer untuk mengambil data secara limit/paginated maximum jumlah datay

    async fn extract(
        &self,
        pool: &PgPool,
        cursor: Box<dyn Any + Send + Sync>,
        limit: i64,
    ) -> anyhow::Result<Box<dyn Any + Send + Sync>>;
}

#[async_trait::async_trait]
impl<T: TypedExtractor> DataExtractor for T {
    async fn extract(
        &self,
        pool: &PgPool,
        cursor: Box<dyn Any + Send + Sync>,
        limit: i64,
    ) -> anyhow::Result<Box<dyn Any + Send + Sync>> {
        let new_cursor = *cursor
            .downcast::<T::Cursor>()
            .map_err(|_| anyhow::anyhow!("failed to downcast cursor: "))?;

        let data = self.fetch(pool, Some(new_cursor), limit).await?;
        Ok(Box::new(data))
    }
}

pub trait DataTransformer {
    fn transform(
        &self,
        data: Vec<serde_json::Value>,
    ) -> Result<Vec<serde_json::Value>, anyhow::Error>;
}

#[async_trait::async_trait]
pub trait DataFetcher {
    type Cursor;
    type Output;

    async fn find_by_ids(
        &self,
        ids: &[Self::Cursor],
        limit: i64,
    ) -> anyhow::Result<Vec<Self::Output>>;
}

#[async_trait::async_trait]
pub trait DataFetcherJson: Send + Sync {
    async fn fetch(
        &self,
        filters: Option<Vec<WhereField>>,
    ) -> anyhow::Result<Vec<serde_json::Value>>;
}

pub async fn fetch_data_json<T>(
    query: &mut QueryBuilder<'_, Postgres>,
    filters: Option<Vec<WhereField>>,
    pool: &Pool<Postgres>,
    order_by: Option<&str>,
) -> anyhow::Result<Vec<serde_json::Value>>
where
    T: for<'r> FromRow<'r, sqlx::postgres::PgRow> + serde::Serialize + Unpin + Send,
{
    if let Some(params) = filters {
        if !params.is_empty() {
            query.push(" WHERE 1=1 ");
        }
        for field in params {
            // Hapus hardcode $1, gunakan string kosong agar QueryBuilder yang mengisi otomatis
            if let Some(values) = field.values {
                // Gunakan format yang benar: tambahkan operator dan target tipe array
                // Hasil akhirnya akan menjadi: AND id = ANY($1::uuid[])
                let clean_values: Vec<String> = values
                    .into_iter()
                    .filter(|s| s != "null" && !s.is_empty())
                    .collect::<HashSet<String>>()
                    .into_iter()
                    .collect();

                // 2. Jika setelah difilter array kosong, kita tidak perlu memanggil query
                // atau biarkan database mengembalikan hasil kosong
                //
                if clean_values.is_empty() {
                    continue;
                }

                query.push(format!(" AND {} {} ANY(", field.field, field.operator));
                query.push_bind(clean_values);
                query.push("::uuid[]) ");
            } else if let Some(value) = field.value {
                // Untuk kondisi biasa: AND id = $1
                query.push(format!(" AND {} {} ", field.field, field.operator));
                query.push_bind(value);
            }
        }
    }
    if let Some(order_by) = order_by {
        query.push(format!(" ORDER BY {} ", order_by));
    }
    let result = query.build_query_as::<T>().fetch_all(pool).await?;

    let parse_result = result
        .iter()
        .map(serde_json::to_value)
        .collect::<Result<Vec<serde_json::Value>, _>>()?;

    Ok(parse_result)
}
