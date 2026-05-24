use crate::core::traits::{strategy::registry::Registry, writter::writter_trait::WritterTrait};

pub type ExportRegistry = Registry<dyn WritterTrait>;
