use std::sync::Arc;

use anyhow::Ok;
use sqlx::QueryBuilder;
use tracing::{info, instrument};
use uuid::Uuid;

use crate::{
    core::traits::load::{LoadEnum, LoaderWarehouse},
    domain::logging::{
        entity::{self, LoggingEntity, LoggingStatus},
        repo::LoggingRepo,
    },
    errors::DomainError,
    infrastructure::warehouse::warehouse_client::WarehouseClient,
    utils::build_query,
};

#[derive(sqlx::FromRow)]
pub struct LoggingWarehouseModel {
    pub id: Option<String>,
    pub logging_type: String,
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub download_url: Option<String>,
    pub status: String,
    pub error: Option<serde_json::Value>,
    pub additional_info: Option<serde_json::Value>,
    pub processed_count: i64,
    pub current_chunk: i64,
    pub total_chunk: i64,
    pub cursor: Option<String>,
}

impl From<LoggingEntity> for LoggingWarehouseModel {
    fn from(value: LoggingEntity) -> Self {
        Self {
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
        }
    }
}

pub struct LoggingWarehouse {
    pub client: Arc<WarehouseClient>,
}

impl LoggingWarehouse {
    async fn updateCommit<T: Send + Sync>(
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
impl LoaderWarehouse for LoggingWarehouse {
    fn table(&self) -> &'static str {
        "loggings"
    }
    fn columns(&self) -> &'static [&'static str] {
        &[
            "logging_type",
            "completed_at",
            "download_url",
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
    #[instrument(skip(self, entity), fields(logging_type = %entity.logging_type, stream_id = %entity.stream_id))]
    async fn store(&self, entity: LoggingEntity) -> anyhow::Result<String> {
        let qb = build_query(self, crate::core::traits::load::LoadEnum::Insert);
        let sql_returning = format!("{} RETURNING id ", qb);
        info!(" logging entity {:?}", entity);
        let result: String = sqlx::query_scalar(&sql_returning)
            .bind(entity.logging_type.to_string())
            .bind(entity.completed_at)
            .bind(entity.download_url)
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
    async fn updateCurrentChunk(&self, stream_id: &str, current_chunk: i64) -> anyhow::Result<()> {
        self.updateCommit(stream_id, "current_chunk", current_chunk)
            .await?;
        Ok(())
    }
    #[instrument(skip(self), fields(stream_id = %stream_id, processed_count = %processed_count))]
    async fn updateProcessedCount(
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

    async fn updateCursor(&self, stream_id: &str, cursor: Option<String>) -> anyhow::Result<()> {
        self.updateCommit(stream_id, "cursor", cursor).await?;

        Ok(())
    }

    #[instrument(skip(self), fields(stream_id = %stream_id))]
    async fn markAsCompleted(&self, stream_id: &str) -> anyhow::Result<()> {
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

    async fn updateStatus(&self, stream_id: &str, status: LoggingStatus) -> anyhow::Result<()> {
        self.updateCommit(stream_id, "status", status.to_string())
            .await?;
        Ok(())
    }

    async fn updateError(&self, stream_id: &str, error: Option<String>) -> anyhow::Result<()> {
        self.updateCommit(stream_id, "error", error).await?;
        Ok(())
    }

    async fn updateDownloadUrl(&self, stream_id: &str, url: String) -> anyhow::Result<()> {
        self.updateCommit(stream_id, "download_url", url).await?;
        Ok(())
    }

    async fn updateCursorAndProcessedCount(
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
