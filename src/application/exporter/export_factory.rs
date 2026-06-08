use crate::{
    application::exporter::types::ExportRequest,
    core::traits::pipeline::export_pipeline::ExportPipeline,
};

#[async_trait::async_trait]
pub trait ExportFactory: Send + Sync {
    fn name(&self) -> &'static str;

    async fn build(&self, export: &ExportRequest) -> Box<dyn ExportPipeline + Send + Sync>;
}
