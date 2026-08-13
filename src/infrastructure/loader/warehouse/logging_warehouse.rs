use std::sync::Arc;

use anyhow::Ok;

use chrono::NaiveDateTime;
use serde_json::json;
use sqlx::{Postgres, QueryBuilder};
use tracing::{debug, instrument};

use crate::infrastructure::db::warehouse_client::WarehouseClient;
use crate::{
    core::pagination::Paginated,
    core::traits::load::{LoadEnum, LoaderPersitence},
    domain::logging::{
        entity::{LoggingEntity, LoggingFilter, LoggingSort, LoggingStatus},
        repo::{ListLoggingRequest, LoggingRepo},
    },
    errors::DomainError,
    utils::load_query::build_query,
};

#[derive(sqlx::FromRow, Debug)]
pub struct LoggingWarehouseModel {
    pub id: Option<String>,
    pub created_at: Option<NaiveDateTime>,
    pub actor_id: String,
    pub stream_id: String,
    pub logging_type: String,
    pub completed_at: Option<NaiveDateTime>,
    pub download_url: Option<String>,
    pub s3_key: Option<String>,
    pub status: String,
    pub error: Option<serde_json::Value>,
    pub additional_info: Option<serde_json::Value>,
    pub processed_count: i32,
    pub total_count: i64,
    pub current_chunk: i32,
    pub tenant_id: String,
    pub title: Option<String>,
    pub email: String,
    pub total_chunk: i32,
    pub cursor: Option<String>,
    pub config: Option<serde_json::Value>,
}

impl From<LoggingWarehouseModel> for LoggingEntity {
    fn from(value: LoggingWarehouseModel) -> Self {
        Self {
            created_at: value.created_at,
            stream_id: value.stream_id,
            email: value.email,
            tenant_id: value.tenant_id,
            title: value.title,
            additional_info: value.additional_info,
            completed_at: value.completed_at,
            s3_key: value.s3_key,
            cursor: value.cursor,
            current_chunk: value.current_chunk,
            download_url: value.download_url,
            error: value.error,
            logging_type: value.logging_type,
            actor_id: value.actor_id,
            processed_count: value.processed_count,
            total_count: value.total_count,
            status: value.status,
            id: value.id,
            total_chunk: value.total_chunk,
            config: value.config,
        }
    }
}
impl From<LoggingEntity> for LoggingWarehouseModel {
    fn from(value: LoggingEntity) -> Self {
        Self {
            created_at: value.created_at,
            stream_id: value.stream_id,
            actor_id: value.actor_id,
            email: value.email,
            tenant_id: value.tenant_id,
            title: value.title,
            additional_info: value.additional_info,
            completed_at: value.completed_at,
            s3_key: value.s3_key,
            cursor: value.cursor,
            current_chunk: 0,
            download_url: value.download_url,
            error: value.error,
            logging_type: LoggingStatus::Pending.to_string(),
            processed_count: value.processed_count,
            total_count: value.total_count,
            status: value.logging_type,
            id: None,
            total_chunk: value.total_chunk,
            config: value.config,
        }
    }
}

pub struct LoggingWarehouse {
    pub client: Arc<WarehouseClient>,
}

impl LoggingWarehouse {
    fn lookup_predicate(bind_index: usize) -> String {
        format!("(id::text = ${bind_index} OR stream_id = ${bind_index})")
    }

    async fn update_commit<T: Send + Sync>(
        &self,
        stream_id: &str,
        key: &str,
        value: T,
    ) -> anyhow::Result<()>
    where
        T: Send + Sync + sqlx::Type<sqlx::Postgres>,
        for<'q> T: sqlx::Encode<'q, sqlx::Postgres>,
    {
        let qb = format!(
            "UPDATE {} SET {} = $1 WHERE {}",
            self.table(),
            key,
            Self::lookup_predicate(2)
        );

        sqlx::query(&qb)
            .bind(&value)
            .bind(stream_id)
            .execute(&self.client.pool)
            .await?;

        Ok(())
    }

    fn append_filters(query_builder: &mut QueryBuilder<'_, Postgres>, filter: &LoggingFilter) {
        let mut has_filter = false;

        for (column, value) in [
            ("id", &filter.id),
            ("actor_id", &filter.actor_id),
            ("email", &filter.email),
            ("tenant_id", &filter.tenant_id),
            ("logging_type", &filter.logging_type),
            ("status", &filter.status),
            ("stream_id", &filter.stream_id),
        ] {
            if let Some(value) = value {
                query_builder.push(if has_filter { " AND " } else { " WHERE " });
                query_builder
                    .push(column)
                    .push(" = ")
                    .push_bind(value.clone());
                has_filter = true;
            };
        }
    }

    fn append_order_by(query_builder: &mut QueryBuilder<'_, Postgres>, sort: &[LoggingSort]) {
        if sort.is_empty() {
            query_builder.push(" ORDER BY created_at DESC, id DESC");
            return;
        }

        query_builder.push(" ORDER BY ");
        for (index, sort) in sort.iter().enumerate() {
            if index > 0 {
                query_builder.push(", ");
            }
            query_builder
                .push(sort.field.column())
                .push(" ")
                .push(sort.direction.as_sql());
        }
        query_builder.push(", id DESC");
    }
}
impl LoaderPersitence for LoggingWarehouse {
    fn table(&self) -> &'static str {
        "loggings"
    }
    fn columns(&self) -> &'static [&'static str] {
        &[
            "logging_type",
            "completed_at",
            "download_url",
            "s3_key",
            "actor_id",
            "email",
            "tenant_id",
            "title",
            "status",
            "error",
            "additional_info",
            "processed_count",
            "total_count",
            "current_chunk",
            "total_chunk",
            "cursor",
            "stream_id",
            "config",
        ]
    }

    fn conflicted_columns(&self) -> &'static [&'static str] {
        &["id"]
    }
}
#[async_trait::async_trait]
impl LoggingRepo for LoggingWarehouse {
    async fn find_by_id(&self, id: &str) -> anyhow::Result<Option<LoggingEntity>> {
        sqlx::query_as::<_, LoggingWarehouseModel>(
            "SELECT * FROM loggings WHERE id::text = $1 LIMIT 1",
        )
        .bind(id)
        .fetch_optional(&self.client.pool)
        .await
        .map(|logging| logging.map(Into::into))
        .map_err(DomainError::DatabaseError)
        .map_err(Into::into)
    }

    #[instrument(skip(self, request))]
    async fn list_logs(
        &self,
        request: &ListLoggingRequest,
    ) -> anyhow::Result<Paginated<LoggingEntity>> {
        let mut count_query = QueryBuilder::<Postgres>::new("SELECT COUNT(*) FROM loggings");
        Self::append_filters(&mut count_query, &request.filter);
        let total_record: i64 = count_query
            .build_query_scalar()
            .fetch_one(&self.client.pool)
            .await
            .map_err(DomainError::DatabaseError)?;

        let query = format!("SELECT * FROM {}", self.table());
        let mut query_builder = QueryBuilder::<Postgres>::new(query);
        Self::append_filters(&mut query_builder, &request.filter);
        Self::append_order_by(&mut query_builder, &request.sort);
        query_builder
            .push(" LIMIT ")
            .push_bind(i64::from(request.pagination.limit()))
            .push(" OFFSET ")
            .push_bind(request.pagination.offset());

        let content = query_builder
            .build_query_as::<LoggingWarehouseModel>()
            .fetch_all(&self.client.pool)
            .await
            .map_err(DomainError::DatabaseError)?
            .into_iter()
            .map(|f| f.into())
            .collect::<Vec<LoggingEntity>>();

        Ok(Paginated::new(content, total_record, request.pagination))
    }
    #[instrument(skip(self, entity), fields(logging_type = %entity.logging_type, stream_id = %entity.stream_id))]
    async fn store(&self, entity: LoggingEntity) -> anyhow::Result<String> {
        let insert_enum = LoadEnum::Insert;
        let qb = build_query(self, &insert_enum);
        let sql_returning = format!("{} RETURNING id ", qb);
        debug!(" logging entity {:?}", entity);
        let result: String = sqlx::query_scalar(&sql_returning)
            .bind(entity.logging_type.to_string())
            .bind(entity.completed_at)
            .bind(entity.download_url)
            .bind(entity.s3_key)
            .bind(entity.actor_id)
            .bind(entity.email)
            .bind(entity.tenant_id)
            .bind(entity.title)
            .bind(entity.status)
            .bind(entity.error)
            .bind(entity.additional_info)
            .bind(entity.processed_count)
            .bind(entity.total_count)
            .bind(entity.current_chunk)
            .bind(entity.total_chunk)
            .bind(entity.cursor)
            .bind(entity.stream_id)
            .bind(entity.config)
            .fetch_one(&self.client.pool)
            .await
            .map_err(DomainError::DatabaseError)?;
        let id = result;
        Ok(id)
    }

    #[instrument(skip(self), fields(stream_id = %stream_id, current_chunk = %current_chunk))]
    async fn update_current_chunk(
        &self,
        stream_id: &str,
        current_chunk: i64,
    ) -> anyhow::Result<()> {
        self.update_commit(stream_id, "current_chunk", current_chunk)
            .await?;
        Ok(())
    }
    #[instrument(skip(self), fields(stream_id = %stream_id, total_chunk = %total_chunk))]
    async fn update_total_chunk(&self, stream_id: &str, total_chunk: i64) -> anyhow::Result<()> {
        self.update_commit(stream_id, "total_chunk", total_chunk)
            .await
    }
    #[instrument(skip(self), fields(stream_id = %stream_id, processed_count = %processed_count))]
    async fn update_processed_count(
        &self,
        stream_id: &str,
        processed_count: i64,
    ) -> anyhow::Result<()> {
        let qb = format!(
            "UPDATE {} SET processed_count = $1 WHERE {}",
            self.table(),
            Self::lookup_predicate(2)
        );

        sqlx::query(&qb)
            .bind(processed_count)
            .bind(stream_id)
            .execute(&self.client.pool)
            .await?;

        Ok(())
    }

    async fn update_cursor(&self, stream_id: &str, cursor: Option<String>) -> anyhow::Result<()> {
        self.update_commit(stream_id, "cursor", cursor).await?;

        Ok(())
    }

    #[instrument(skip(self), fields(stream_id = %stream_id))]
    async fn mark_as_completed(&self, stream_id: &str) -> anyhow::Result<()> {
        let qb = format!(
            "UPDATE {} SET status = $1, completed_at = NOW() WHERE {}",
            self.table(),
            Self::lookup_predicate(2)
        );

        sqlx::query(&qb)
            .bind(LoggingStatus::Completed.to_string())
            .bind(stream_id)
            .execute(&self.client.pool)
            .await?;

        Ok(())
    }

    async fn update_status(&self, stream_id: &str, status: LoggingStatus) -> anyhow::Result<()> {
        self.update_commit(stream_id, "status", status.to_string())
            .await?;
        Ok(())
    }

    async fn update_error(&self, stream_id: &str, error: Option<String>) -> anyhow::Result<()> {
        let json_error = json!({"error": error});
        self.update_commit(stream_id, "error", json_error).await?;
        Ok(())
    }

    async fn update_download_url_and_s3_key(
        &self,
        stream_id: &str,
        download_url: String,
        s3_key: String,
    ) -> anyhow::Result<()> {
        let query = format!(
            "UPDATE {} SET download_url = $1, s3_key = $2 WHERE {}",
            self.table(),
            Self::lookup_predicate(3)
        );
        sqlx::query(&query)
            .bind(download_url)
            .bind(s3_key)
            .bind(stream_id)
            .execute(&self.client.pool)
            .await?;
        Ok(())
    }

    async fn update_cursor_and_processed_count(
        &self,
        stream_id: &str,
        cursor: Option<String>,
        processed_count: i64,
    ) -> anyhow::Result<()> {
        let query = format!(
            "UPDATE {} SET cursor = $1, processed_count = $2 WHERE {}",
            self.table(),
            Self::lookup_predicate(3)
        );
        sqlx::query(&query)
            .bind(cursor)
            .bind(processed_count)
            .bind(stream_id)
            .execute(&self.client.pool)
            .await?;
        Ok(())
    }
}
