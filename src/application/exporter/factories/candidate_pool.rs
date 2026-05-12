// use crate::application::exporter::export_factory::ExportFactory;
// use crate::application::exporter::pipeline::candidate_pool_pipeline::CandidatePoolPipeline;
// use crate::application::exporter::types::{
//     DatasetRegistry, ExportRequest, TenantRegistry, WriterRegsitry,
// };
//
// use crate::core::traits::pipeline::export_pipeline::ExportPipeline;
// use crate::domain::datasets;
// use std::sync::Arc;
//
// pub struct CandidatePoolFactory {
//     writer_registry: Arc<WriterRegsitry>,
// }
//
// #[async_trait::async_trait]
// impl ExportFactory for CandidatePoolFactory {
//     fn name(&self) -> &'static str {
//         "candidate_pool_factory"
//     }
//
//     async fn build(&self, export: &ExportRequest) -> Box<dyn ExportPipeline + Send + Sync> {}
// }
