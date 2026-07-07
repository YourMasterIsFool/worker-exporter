use std::sync::Arc;

use anyhow::Ok;

use chrono::NaiveDateTime;
use serde_json::json;
use sqlx::QueryBuilder;
use tracing::{debug, instrument};

use crate::infrastructure::db::warehouse_client::WarehouseClient;
use crate::{
    core::traits::load::{LoadEnum, LoaderPersitence},
    domain::logging::{
        entity::{LoggingEntity, LoggingStatus},
        repo::LoggingRepo,
    },
    errors::DomainError,
    utils::build_query,
};

#[derive(sqlx::FromRow, Debug)]
pub struct LoggingWarehouseModel {
    pub id: Option<String>,
    pub actor_id: String,
    pub stream_id: String,
    pub logging_type: String,
    pub completed_at: Option<NaiveDateTime>,
    pub download_url: Option<String>,
    pub status: String,
    pub error: Option<serde_json::Value>,
    pub additional_info: Option<serde_json::Value>,
    pub processed_count: i32,
    pub current_chunk: i32,
    pub tenant_id: String,
    pub email: String,
    pub total_chunk: i32,
    pub cursor: Option<String>,
    pub config: Option<serde_json::Value>,
}

impl From<LoggingWarehouseModel> for LoggingEntity {
    fn from(value: LoggingWarehouseModel) -> Self {
        Self {
            stream_id: value.stream_id,
            email: value.email,
            tenant_id: value.tenant_id,
            additional_info: value.additional_info,
            completed_at: value.completed_at,
            cursor: value.cursor,
            current_chunk: value.current_chunk,
            download_url: value.download_url,
            error: value.error,
            logging_type: value.logging_type,
            actor_id: value.actor_id,
            processed_count: value.processed_count,
            status: value.status,
            id: value.id,
            total_chunk: value.total_chunk,
            config: None,
        }
    }
}
impl From<LoggingEntity> for LoggingWarehouseModel {
    fn from(value: LoggingEntity) -> Self {
        Self {
            stream_id: value.stream_id,
            actor_id: value.actor_id,
            email: value.email,
            tenant_id: value.tenant_id,
            additional_info: value.additional_info,
            completed_at: value.completed_at,
            cursor: value.cursor,
            current_chunk: 0,
            download_url: value.download_url,
            error: value.error,
            logging_type: LoggingStatus::Pending.to_string(),
            processed_count: value.processed_count,
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
            "
            UPDATE {} SET {} = $1 where 1=1 AND stream_id = $2
        ",
            self.table(),
            key
        );

        sqlx::query(&qb)
            .bind(&value)
            .bind(stream_id)
            .execute(&self.client.pool)
            .await?;

        Ok(())
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
            "actor_id",
            "email",
            "tenant_id",
            "status",
            "error",
            "additional_info",
            "processed_count",
            "current_chunk",
            "cursor",
            "stream_id",
        ]
    }

    fn conflicted_columns(&self) -> &'static [&'static str] {
        &["id"]
    }
}
#[async_trait::async_trait]
impl LoggingRepo for LoggingWarehouse {
    #[instrument(skip(self, tenant_id))]
    async fn list_log_by_tenant_id(&self, tenant_id: &str) -> anyhow::Result<Vec<LoggingEntity>> {
        let query = format!("SELECT * FROM {}", self.table());
        let mut query_builder = QueryBuilder::new(&query);
        query_builder.push(" WHERE tenant_id = ");
        query_builder.push_bind(tenant_id);
        query_builder.push(" ORDER BY created_at DESC");

        let result = query_builder
            .build_query_as::<LoggingWarehouseModel>()
            .fetch_all(&self.client.pool)
            .await
            .map_err(DomainError::DatabaseError)?;

        let mapper_result = result
            .into_iter()
            .map(|f| f.into())
            .collect::<Vec<LoggingEntity>>();
        Ok(mapper_result)
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
            .bind(entity.actor_id)
            .bind(entity.email)
            .bind(entity.tenant_id)
            .bind(entity.status)
            .bind(entity.error)
            .bind(entity.additional_info)
            .bind(entity.processed_count)
            .bind(entity.current_chunk)
            .bind(entity.cursor)
            .bind(entity.stream_id)
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
    #[instrument(skip(self), fields(stream_id = %stream_id, processed_count = %processed_count))]
    async fn update_processed_count(
        &self,
        stream_id: &str,
        processed_count: i64,
    ) -> anyhow::Result<()> {
        let qb = format!(
            "
            UPDATE {} SET processed_count = $1 where stream_id = $2
        ",
            self.table()
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
            "
            UPDATE {} SET status = $1, completed_at = NOW() where  stream_id = $2
        ",
            self.table()
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

    async fn update_download_url(&self, stream_id: &str, url: String) -> anyhow::Result<()> {
        self.update_commit(stream_id, "download_url", url).await?;
        Ok(())
    }

    async fn update_cursor_and_processed_count(
        &self,
        stream_id: &str,
        cursor: Option<String>,
        processed_count: i64,
    ) -> anyhow::Result<()> {
        let query = format!(
            "UPDATE {} set cursor = $1, processed_count = $2 where stream_id = $3",
            self.table()
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
