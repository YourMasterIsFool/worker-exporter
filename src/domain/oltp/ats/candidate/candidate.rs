use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
pub struct CandidateIngestionEntity {
    pub id: Uuid,
    pub user_id: Option<Uuid>,
    pub name: String,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub email: String,

    // Keamanan
    pub identity_number_encrypt: Option<String>,
    pub identity_number_hashing: Option<String>,
    pub is_identity_number_verified: Option<bool>,
    pub identity_number_verified_at: Option<NaiveDateTime>,

    // Media IDs
    pub profile_photo_media_id: Option<Uuid>,
    pub video_resume_media_id: Option<Uuid>,
    pub cv_media_id: Option<Uuid>,

    // Lokasi & Metadata
    pub location_meta: Option<serde_json::Value>, // Menggunakan serde_json::Value untuk JSONB

    // Arrays
    pub keywords: Option<Vec<String>>,
    pub interests: Option<Vec<Uuid>>,
    // ... field lainnya mengikuti pola ini
}
