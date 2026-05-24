use std::sync::Arc;

use crate::{
    app_state::AppState,
    core::traits::{
        extractor::source_incremental_extract::SourceIncrementalExtract, handler::JobStruct,
    },
};

pub trait SourceFactory: Send + Sync {
    fn build(
        &self,
        job: &JobStruct,
        state: Arc<AppState>,
    ) -> Box<dyn SourceIncrementalExtract + Send + Sync>;
}
