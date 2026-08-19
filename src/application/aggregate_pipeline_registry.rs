use std::sync::Arc;

use crate::{
    application::aggregate::candidate_aggregate_pipeline::{
        CandidateAggregatePipeline, VacancyAggregatePipeline,
    },
    core::traits::{pipeline::pipeline_trait::AggregatePipeline, strategy::registry::Registry},
    infrastructure::db::ats_client::AtsClient,
};

pub type AggregatePipelineRegistry = Registry<dyn AggregatePipeline + Send + Sync>;

pub fn build_aggregate_pipeline_registry(ats_client: Arc<AtsClient>) -> AggregatePipelineRegistry {
    let mut registry = AggregatePipelineRegistry::new();
    registry
        .register(
            "CANDIDATE_AGGREGATE",
            Arc::new(CandidateAggregatePipeline {
                ats_client: ats_client.clone(),
            }),
        )
        .expect("aggregate pipeline key must be unique");
    registry
        .register(
            "VACANCY_AGGREGATE",
            Arc::new(VacancyAggregatePipeline { ats_client }),
        )
        .expect("aggregate pipeline key must be unique");
    registry
}
