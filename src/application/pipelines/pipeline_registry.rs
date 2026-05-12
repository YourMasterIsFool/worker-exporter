// // pub type datasetregistry = registry<dyn datasettrait>;
// pub type writerregsitry = registry<dyn exportwriter>;
// pub type tenantregistry = registry<dyn tenantstrategy>;

use crate::core::traits::{
    extractor::incremental_extract::IncrementalExtract, pipeline::pipeline_trait::PipelineTrait,
    strategy::registry::Registry,
};

pub type ExtractorRegistry = Registry<dyn IncrementalExtract>;

pub type PipelineRegistry = Registry<dyn PipelineTrait>;
