use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default)]
pub struct LoggingFilter {
    pub id: Option<String>,
    pub actor_id: Option<String>,
    pub email: Option<String>,
    pub tenant_id: Option<String>,
    pub logging_type: Option<String>,
    pub status: Option<String>,
    pub stream_id: Option<String>,
}

#[derive(Clone, Debug)]
pub struct LoggingSort {
    pub field: LoggingSortField,
    pub direction: SortDirection,
}

#[derive(Clone, Copy, Debug)]
pub enum LoggingSortField {
    Id,
    ActorId,
    Email,
    TenantId,
    LoggingType,
    Status,
    StreamId,
    CreatedAt,
    CompletedAt,
}

impl LoggingSortField {
    pub fn column(self) -> &'static str {
        match self {
            Self::Id => "id",
            Self::ActorId => "actor_id",
            Self::Email => "email",
            Self::TenantId => "tenant_id",
            Self::LoggingType => "logging_type",
            Self::Status => "status",
            Self::StreamId => "stream_id",
            Self::CreatedAt => "created_at",
            Self::CompletedAt => "completed_at",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum SortDirection {
    Asc,
    Desc,
}

impl SortDirection {
    pub fn as_sql(self) -> &'static str {
        match self {
            Self::Asc => "ASC",
            Self::Desc => "DESC",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LoggingEntity {
    pub id: Option<String>,
    pub created_at: Option<NaiveDateTime>,
    pub actor_id: String,
    pub email: String,
    pub tenant_id: String,
    pub title: Option<String>,
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
    pub total_chunk: i32,
    pub cursor: Option<String>,
    pub stream_id: String,
    pub config: Option<serde_json::Value>,
}

pub enum LoggingStatus {
    Pending,
    Processing,
    Completed,
    Failed,
    PartialSuccess,
    PartialSync,
    SyncProgress,
    SyncFailed,
    SyncSuccess,
}

pub enum LoggingType {
    Ingestion,
    Export,
    Import,
    Sync,
}

impl LoggingType {
    pub fn to_string(&self) -> String {
        match self {
            LoggingType::Ingestion => "INGESTION".into(),
            LoggingType::Export => "EXPORT".into(),
            LoggingType::Import => "IMPORT".into(),
            LoggingType::Sync => "SYNC".into(),
        }
    }
}

impl LoggingStatus {
    pub fn to_string(&self) -> String {
        match self {
            LoggingStatus::Pending => "PENDING".into(),
            LoggingStatus::Processing => "PROCESSING".into(),
            LoggingStatus::Completed => "COMPLETED".into(),
            LoggingStatus::Failed => "FAILED".into(),
            LoggingStatus::PartialSuccess => "PARTIAL_SUCCESS".into(),
            LoggingStatus::PartialSync => "PARTIAL_SYNC".into(),
            LoggingStatus::SyncProgress => "SYNC_PROGRESS".into(),
            LoggingStatus::SyncFailed => "SYNC_FAILED".into(),
            LoggingStatus::SyncSuccess => "SYNC_SUCCESS".into(),
        }
    }
}
