use axum::{
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use thiserror::Error;

#[derive(Serialize)]
pub struct AppResponse {
    detail: String,
}
#[derive(Error, Debug)]
pub enum AppError {
    #[error("internal server error {0}")]
    InternalServerError(String),
    #[error("Validation error: {0}")]
    ValidationError(String),
    #[error("Error Parssing: {0}")]
    ParsingError(String),
    #[error("Not Found {0}")]
    NotFound(String),
}

#[derive(Error, Debug)]
pub enum DomainError {
    #[error("Database error: {0}")]
    DatabaseError(#[from] sqlx::Error),

    #[error("Warehouse Error: {0}")]
    WarehouseError(sqlx::Error),

    #[error("Ingestion Error: {0}")]
    IngestionError(sqlx::Error),
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        let (code, message) = match self {
            AppError::InternalServerError(err) => {
                (axum::http::StatusCode::INTERNAL_SERVER_ERROR, err)
            }
            AppError::ValidationError(err) => (axum::http::StatusCode::BAD_REQUEST, err),
            AppError::ParsingError(err) => (axum::http::StatusCode::BAD_REQUEST, err),
            AppError::NotFound(err) => (axum::http::StatusCode::NOT_FOUND, err),
        };
        let json = Json(AppResponse {
            detail: message.to_string(),
        });

        (code, json).into_response()
    }
}
