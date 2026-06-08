use thiserror::Error;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("internal server error")]
    InternalServerError,
    #[error("Validation error: {0}")]
    ValidationError(#[from] serde_json::Error),
    #[error("Error Parssing: {0}")]
    ParsingError(#[from] anyhow::Error),
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
