use std::sync::Arc;

use crate::{domain::logging::repo::LoggingRepo, errors::AppError};

pub struct ListLogByTenantIdUsecase {
    pub logging_repo: Arc<dyn LoggingRepo + Send + Sync>,
}

impl ListLogByTenantIdUsecase {
    pub fn new(logging_repo: Arc<dyn LoggingRepo + Send + Sync>) -> Self {
        Self { logging_repo }
    }

    pub async fn execute(
        &self,
        tenant_id: &str,
    ) -> anyhow::Result<Vec<crate::domain::logging::entity::LoggingEntity>> {
        let logs = self
            .logging_repo
            .list_log_by_tenant_id(tenant_id)
            .await
            .map_err(|err| AppError::InternalServerError(err.to_string()))?;
        Ok(logs)
    }
}
