use thiserror::Error;

#[derive(Debug, Error)]
pub enum WarehouseError {
    #[error("Warehoue Database Error {0}")]
    DatabaseError(String),

    #[error("Warehouse Application Error {0}")]
    ApplicationError(String),
}
