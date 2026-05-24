use chrono::DateTime;
use chrono::Utc;

use crate::infrastructure::ingestion::organization::enums;

#[derive(Clone, Debug)]
pub struct LoggingEntity {
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
}

impl LoggingType {
    pub fn to_string(&self) -> String {
        match self {
            LoggingType::Ingestion => "INGESTION".into(),
            LoggingType::Export => "EXPORT".into(),
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
