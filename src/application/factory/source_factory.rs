use std::sync::Arc;

use crate::{
    app_state::AppState,
    core::traits::{extractor::incremental_extract::IncrementalExtract, handler::JobStruct},
};

pub trait SourceFactory: Send + Sync {
    fn build(
        &self,
        job: &JobStruct,
        state: Arc<AppState>,
    ) -> Box<dyn IncrementalExtract + Send + Sync>;
}
