use std::collections::HashMap;

use anyhow::Ok;
use tracing::warn;

use crate::{
    core::traits::writter::writter_trait::WritterTrait,
    infrastructure::exporter::{csv_exporter::CsvExporter, excel_exporter::ExcelExporter},
};

// pub type WritterRegistry = Registry<dyn WritterTrait>;
// pub fn build_writter_registry() -> WritterRegistry {
//     let mut registry = WritterRegistry::new();
//     registry.register("csv", || Box::new(CsvExporter::new()));
//     registry
// }
//

pub type WritterFactory = Box<dyn Fn() -> Box<dyn WritterTrait> + Send + Sync>;
pub struct WritterRegistry {
    pub maps: HashMap<String, WritterFactory>,
}

impl Default for WritterRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl WritterRegistry {
    pub fn new() -> Self {
        let maps = HashMap::new();
        Self { maps }
    }

    pub fn get(&self, key: &str) -> anyhow::Result<Box<dyn WritterTrait>> {
        if let Some(writter) = self.maps.get(key) {
            Ok(writter())
        } else {
            warn!("registry not found");

            Err(anyhow::anyhow!("writter not found"))
        }
    }
    pub fn register<F>(&mut self, name: &str, writter: F) -> anyhow::Result<()>
    where
        F: Fn() -> Box<dyn WritterTrait> + Send + Sync + 'static,
    {
        if self.maps.contains_key(name) {
            return Err(anyhow::anyhow!("writter already registered"));
        }
        self.maps.insert(name.to_string(), Box::new(writter));
        Ok(())
    }
}
pub fn build_writter_registry() -> WritterRegistry {
    let mut registry = WritterRegistry::new();
    registry.register("csv", || Box::new(CsvExporter::new()));
    registry.register("excel", || Box::new(ExcelExporter::new()));
    registry
}
