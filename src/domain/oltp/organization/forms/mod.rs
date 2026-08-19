use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct FormIngestionEntity {
    pub id: Uuid,
    pub updated_at: Option<NaiveDateTime>,
    pub created_at: NaiveDateTime,
    pub deleted_at: Option<NaiveDateTime>,
    pub tenant_id: Option<Uuid>,
    pub title: String,
    pub description: Option<String>,
    pub scoring_enabled: bool,
    pub minimum_score: Option<i32>,
    pub timer_enabled: bool,
    pub duration_minutes: Option<i32>,
    pub permission_keys: Vec<String>,
    pub status: String,
    pub group_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct FormQuestionIngestionEntity {
    pub id: Uuid,
    pub updated_at: Option<NaiveDateTime>,
    pub created_at: NaiveDateTime,
    pub deleted_at: Option<NaiveDateTime>,
    pub form_id: Uuid,
    pub sequence: i32,
    pub question_text: String,
    pub is_required: bool,
    pub points: i32,
    pub question_type: Option<String>,
    pub settings: Option<Value>,
    pub valid_from: Option<DateTime<Utc>>,
    pub valid_end: Option<DateTime<Utc>>,
    pub is_current: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct FormQuestionOptionIngestionEntity {
    pub id: Uuid,
    pub form_question_id: Uuid,
    pub sequence: i32,
    pub option_text: String,
    pub is_correct: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct FormResponseIngestionEntity {
    pub id: Uuid,
    pub form_id: Uuid,
    pub respondent_id: Uuid,
    pub status: String,
    pub started_at: NaiveDateTime,
    pub submitted_at: Option<NaiveDateTime>,
    pub total_score: Option<i32>,
    pub total_point: Option<i32>,
    pub notify_after_submit: bool,
    pub respondent_name: Option<String>,
    pub respondent_image: Option<String>,
    pub respondent_email: Option<String>,
    pub identifier_id: Option<String>,
    pub extension: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct FormResponseAnswerIngestionEntity {
    pub id: Uuid,
    pub form_response_id: Uuid,
    pub form_question_id: Uuid,
    pub answer_value: Option<Value>,
    pub score_earned: i32,
    pub updated_at: Option<NaiveDateTime>,
    pub created_at: NaiveDateTime,
    pub deleted_at: Option<NaiveDateTime>,
}
