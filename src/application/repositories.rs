use std::sync::Arc;

use crate::infrastructure::loader::warehouse::{
    logging_warehouse::LoggingWarehouse, warehouse_logging_import::WarehouseLoggingImport,
};
use crate::{
    domain::logging::repo::LoggingRepo, infrastructure::db::db_manager::DatabaseInfraManager,
};

pub struct Repositories {
    pub logging_repo: Arc<dyn LoggingRepo + Send + Sync>,
    pub import_logging_repo: Arc<WarehouseLoggingImport>,
}

impl Repositories {
    pub fn new(database_manager: Arc<DatabaseInfraManager>) -> Self {
        let warehouse_client = database_manager.warehouse.clone();
        let logging_repo = Arc::new(LoggingWarehouse {
            client: warehouse_client.clone(),
        });
        let import_logging_repo = Arc::new(WarehouseLoggingImport::new(warehouse_client));
        Self {
            logging_repo,
            import_logging_repo,
        }
    }
}
