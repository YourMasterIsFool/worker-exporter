use std::sync::Arc;

use crate::{
    application::import::import_registry::ImportLoaderRegistry,
    core::traits::{pipeline::pipeline_trait::PipelineImport, strategy::registry::Registry},
    infrastructure::pipeline::ats::vacancy_import_pipeline::VacancyImportPipeline,
};

pub type PipelineImportRegistry = Registry<dyn PipelineImport>;
pub fn build_pipeline_import_registry(loader: Arc<ImportLoaderRegistry>) -> PipelineImportRegistry {
    let mut registry = PipelineImportRegistry::new();
    registry.register(
        "CONFIG_IMPORT_PIPELINE",
        Arc::new(VacancyImportPipeline {
            loader: loader.clone(),
        }),
    );
    registry.register(
        "vacancy_import_pipeline",
        Arc::new(VacancyImportPipeline { loader }),
    );
    registry
}
