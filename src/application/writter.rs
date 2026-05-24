use std::sync::Arc;

use crate::{
    app_state::{self, AppState},
    core::traits::{strategy::registry::Registry, writter::writter_trait::WritterTrait},
    infrastructure::exporter::csv_exporter::CsvExporter,
};

pub type WritterRegistry = Registry<dyn WritterTrait>;
pub fn build_writter_registry() -> WritterRegistry {
    let mut registry = WritterRegistry::new();
    registry.register("csv", Arc::new(CsvExporter::new()));
    registry
}
