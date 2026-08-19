use std::sync::Arc;

use crate::{
    application::{
        pipelines::rollback_pipeline::ConfigRollbackPipeline,
        rollback_loader_registry::RollbackLoaderRegistry,
    },
    core::traits::{pipeline::pipeline_trait::RollbackPipeline, strategy::registry::Registry},
    infrastructure::db::ats_client::AtsClient,
};

pub type RollbackPipelineRegistry = Registry<dyn RollbackPipeline + Send + Sync>;

pub fn build_rollback_pipeline_registry(
    loaders: Arc<RollbackLoaderRegistry>,
    ats_client: Arc<AtsClient>,
) -> RollbackPipelineRegistry {
    let mut registry = RollbackPipelineRegistry::new();
    registry
        .register(
            "CONFIG_ROLLBACK_PIPELINE",
            Arc::new(ConfigRollbackPipeline {
                loaders,
                ats_client,
            }),
        )
        .expect("register config rollback pipeline");
    registry
}
