use std::sync::Arc;

use crate::application::{
    fetch_source_regitry::DataFetcherJsonRegistry,
    repositories::Repositories,
    usecases::{
        generated_email_template_usecase::GeneratedEmailTemplateUsecase,
        list_log_by_tenant_id_usecase::ListLogByTenantIdUsecase,
    },
};

pub struct Usecases {
    pub list_log_by_tenant_id_usceases: Arc<ListLogByTenantIdUsecase>,
    pub generated_email_template_usecase: Arc<GeneratedEmailTemplateUsecase>,
}
impl Usecases {
    pub fn new(repositories: Arc<Repositories>, fetcher: Arc<DataFetcherJsonRegistry>) -> Self {
        let loging_repo = repositories.logging_repo.clone();
        let list_log_by_tenant_id_usceases = Arc::new(ListLogByTenantIdUsecase::new(loging_repo));
        let generated_email_template_usecase = Arc::new(GeneratedEmailTemplateUsecase { fetcher });
        Self {
            list_log_by_tenant_id_usceases,
            generated_email_template_usecase,
        }
    }
}
