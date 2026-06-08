// // pub type datasetregistry = registry<dyn datasettrait>;
// pub type writerregsitry = registry<dyn exportwriter>;
// pub type tenantregistry = registry<dyn tenantstrategy>;

use std::sync::Arc;

use crate::{
    application::{
        pipelines::ingestion_pipeline::IngestionPipeline, source::source_registry::SourceRegistry,
    },
    core::traits::{pipeline::pipeline_trait::PipelineTrait, strategy::registry::Registry},
};

pub type PipelineRegistry = Registry<dyn PipelineTrait>;
pub fn build_pipeline_registry(
    // loader: Arc<LoadRegistry>,
    source: Arc<SourceRegistry>,
) -> PipelineRegistry {
    let mut registry = PipelineRegistry::new();
    registry.register(
        "ingestion",
        Arc::new(IngestionPipeline {
            source: source.clone(),
        }),
    );
    registry
}
