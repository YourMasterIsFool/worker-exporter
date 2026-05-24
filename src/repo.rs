use std::sync::Arc;

use crate::{
    domain::logging::repo::LoggingRepo,
    infrastructure::warehouse::{
        logging_warehouse::LoggingWarehouse, warehouse_client::WarehouseClient,
    },
};

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
