use std::sync::Arc;

use crate::{
    core::pagination::Paginated,
    domain::logging::{
        entity::LoggingEntity,
        repo::{ListLoggingRequest, LoggingRepo},
    },
    errors::AppError,
};

pub struct ListLogByTenantIdUsecase {
    pub logging_repo: Arc<dyn LoggingRepo + Send + Sync>,
}

impl ListLogByTenantIdUsecase {
    pub fn new(logging_repo: Arc<dyn LoggingRepo + Send + Sync>) -> Self {
        Self { logging_repo }
    }

    pub async fn execute(
        &self,
        request: &ListLoggingRequest,
    ) -> anyhow::Result<Paginated<LoggingEntity>> {
        let logs = self
            .logging_repo
            .list_logs(request)
            .await
            .map_err(|err| AppError::InternalServerError(err.to_string()))?;
        Ok(logs)
    }
}
