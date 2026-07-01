use std::sync::Arc;

use crate::domain::logging::repo::LoggingRepo;
use crate::infrastructure::db::warehouse_client::WarehouseClient;
use crate::infrastructure::loader::warehouse::logging_warehouse::LoggingWarehouse;

pub struct WarehouseRepo {
    pub logging_repo: Arc<dyn LoggingRepo>,
}

impl WarehouseRepo {
    pub fn new(warehouse_client: Arc<WarehouseClient>) -> Self {
        let logging = LoggingWarehouse {
            client: warehouse_client,
        };
        Self {
            logging_repo: Arc::new(logging),
        }
    }
}
