use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LoggingEntity {
    pub id: Option<String>,
    pub actor_id: String,
    pub email: String,
    pub tenant_id: String,
    pub logging_type: String,
    pub completed_at: Option<NaiveDateTime>,
    pub download_url: Option<String>,
    pub status: String,
    pub error: Option<serde_json::Value>,
    pub additional_info: Option<serde_json::Value>,
    pub processed_count: i32,
    pub current_chunk: i32,
    pub total_chunk: i32,
    pub cursor: Option<String>,
    pub stream_id: String,
}

pub enum LoggingStatus {
    Pending,
    Processing,
    Completed,
    Failed,
}

pub enum LoggingType {
    Ingestion,
    Export,
    Import,
}

impl LoggingType {
    pub fn to_string(&self) -> String {
        match self {
            LoggingType::Ingestion => "INGESTION".into(),
            LoggingType::Export => "EXPORT".into(),
            LoggingType::Import => "IMPORT".into(),
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
        }
    }
}
