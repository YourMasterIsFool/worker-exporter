use std::sync::Arc;

use crate::infrastructure::db::warehouse_client::WarehouseClient;
use crate::{
    core::config::database_manager::DatabaseManager,
    infrastructure::db::{
        ats_client::AtsClient, hris_client::HrisClient, master_client::MasterClient,
    },
};

pub struct DatabaseInfraManager {
    pub warehouse: Arc<WarehouseClient>,
    pub ats: Arc<AtsClient>,
    pub hris: Arc<HrisClient>,
    pub master: Arc<MasterClient>,
}

impl DatabaseInfraManager {
    pub fn new(database_manager: Arc<DatabaseManager>) -> Self {
        let warehouse = Arc::new(WarehouseClient::new(database_manager.warehouse.clone()).unwrap());
        let ats = Arc::new(AtsClient::new(database_manager.ats.clone()));
        let hris = Arc::new(HrisClient::new(database_manager.hris.clone()));
        let master = Arc::new(MasterClient::new(database_manager.master.clone()));

        Self {
            warehouse,
            ats,
            hris,
            master,
        }
    }
}
