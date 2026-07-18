use std::sync::Arc;

use crate::application::{
    fetch_source_regitry::DataFetcherJsonRegistry,
    repositories::Repositories,
    usecases::{
        generated_email_template_usecase::GeneratedEmailTemplateUsecase,
        get_import_logging_detail_usecase::GetImportLoggingDetailUsecase,
        list_log_by_tenant_id_usecase::ListLogByTenantIdUsecase,
    },
};

pub struct Usecases {
    pub list_log_by_tenant_id_usceases: Arc<ListLogByTenantIdUsecase>,
    pub get_import_logging_detail_usecase: Arc<GetImportLoggingDetailUsecase>,
    pub generated_email_template_usecase: Arc<GeneratedEmailTemplateUsecase>,
}
impl Usecases {
    pub fn new(repositories: Arc<Repositories>, fetcher: Arc<DataFetcherJsonRegistry>) -> Self {
        let loging_repo = repositories.logging_repo.clone();
        let list_log_by_tenant_id_usceases = Arc::new(ListLogByTenantIdUsecase::new(loging_repo));
        let get_import_logging_detail_usecase = Arc::new(GetImportLoggingDetailUsecase::new(
            repositories.logging_repo.clone(),
            repositories.import_logging_repo.clone(),
        ));
        let generated_email_template_usecase =
            Arc::new(GeneratedEmailTemplateUsecase::new(fetcher));
        Self {
            list_log_by_tenant_id_usceases,
            get_import_logging_detail_usecase,
            generated_email_template_usecase,
        }
    }
}
