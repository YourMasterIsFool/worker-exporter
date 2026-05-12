use crate::core::datasets::dataset_runner::DatasetRunner;
use crate::core::datasets::dataset_trait::DatasetTrait;
use crate::core::traits::exporter::export_writer::ExportWriter;
use crate::core::traits::strategy::registry::Registry;
use crate::core::traits::tenants::tenant_strategy::TenantStrategy;

pub struct ExportRequest {
    pub dataset: String,
    pub format: String,
    pub tenant: String,
}

pub type DatasetRegistry = Registry<dyn DatasetTrait>;
pub type WriterRegsitry = Registry<dyn ExportWriter>;
pub type TenantRegistry = Registry<dyn TenantStrategy>;
