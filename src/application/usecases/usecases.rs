use std::sync::Arc;

use crate::application::{
    repositories::Repositories, usecases::list_log_by_tenant_id_usecase::ListLogByTenantIdUsecase,
};

pub struct Usecases {
    pub list_log_by_tenant_id_usceases: Arc<ListLogByTenantIdUsecase>,
}
impl Usecases {
    pub fn new(repositories: Arc<Repositories>) -> Self {
        let loging_repo = repositories.logging_repo.clone();
        let list_log_by_tenant_id_usceases = Arc::new(ListLogByTenantIdUsecase::new(loging_repo));
        Self {
            list_log_by_tenant_id_usceases,
        }
    }
}
