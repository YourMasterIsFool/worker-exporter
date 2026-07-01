use serde::Serialize;
use sqlx::prelude::FromRow;

#[derive(Debug, Clone, FromRow, Serialize)]
pub struct CandidatePoolSnapshotModel {
    pub name: String,
    pub nik: String,
    pub id: String,
}
