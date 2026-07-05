use std::sync::Arc;

use crate::{
    application::loader_registry::LoaderRegistry,
    core::traits::{pipeline::pipeline_trait::PipelineImport, strategy::registry::Registry},
    infrastructure::pipeline::ats::vacancy_import_pipeline::VacancyImportPipeline,
};

pub type PipelineImportRegistry = Registry<dyn PipelineImport>;
pub fn build_pipeline_import_registry(loader: Arc<LoaderRegistry>) -> PipelineImportRegistry {
    let mut registry = PipelineImportRegistry::new();
    registry.register(
        "vacancy_import_pipeline",
        Arc::new(VacancyImportPipeline {
            loader: loader.clone(),
        }),
    );
    registry
}
