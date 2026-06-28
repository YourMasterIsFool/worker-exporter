use std::sync::Arc;

use crate::{
    core::config::database_manager::DatabaseManager,
    domain::logging::repo::LoggingRepo,
    infrastructure::{
        db::db_manager::DatabaseInfraManager, warehouse::logging_warehouse::LoggingWarehouse,
    },
};

pub struct Repositories {
    pub logging_repo: Arc<dyn LoggingRepo + Send + Sync>,
}

impl Repositories {
    pub fn new(database_manager: Arc<DatabaseInfraManager>) -> Self {
        let warehouse_client = database_manager.warehouse.clone();
        let logging_repo = Arc::new(LoggingWarehouse {
            client: warehouse_client,
        });
        Self { logging_repo }
    }
}
