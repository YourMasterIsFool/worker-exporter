use crate::core::traits::{exporter::export_writer::ExportWriter, strategy::registry::Registry};

pub type ExportRegistry = Registry<dyn ExportWriter>;
