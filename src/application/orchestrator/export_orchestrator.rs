use std::sync::Arc;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::{
    app_state::{self, AppState},
    application::{
        pipelines::{export_pipeline::ExportPipeline, ingestion_pipeline::IngestionPipeline},
        runner::ingestion_runner::IngestionRunner,
    },
};

#[derive(Serialize, Debug, Deserialize)]
pub struct ExportFields {
    label: String,
    source: String,
}

#[derive(Serialize, Debug, Deserialize)]

pub struct ExportJob {
    pub(crate) tenant_id: String,
    pub(crate) limit: i64,
    pub(crate) source: String,
    pub(crate) fields: Vec<ExportFields>,
}
pub struct ExportOrchestrator {
    ingestion_pipeline: Arc<IngestionPipeline>,
    export_pipeline: Arc<ExportPipeline>,
    ingestion_runner: Arc<IngestionRunner>,
    app_state: Arc<AppState>,
}

impl ExportOrchestrator {
    fn new(
        ingestion_pipeline: Arc<IngestionPipeline>,
        export_pipeline: Arc<ExportPipeline>,
        ingestion_runner: Arc<IngestionRunner>,
        app_state: Arc<AppState>,
    ) -> Self {
        Self {
            ingestion_pipeline,
            export_pipeline,
            app_state,
            ingestion_runner,
        }
    }

    async fn run(&self, export_job: &ExportJob) -> Result<()> {
        let Ok(source) = self.app_state.source_registry.get(&export_job.source) else {
            warn!(
                "Failed to get source for '{}', skipping job...",
                export_job.source
            );
            return Ok(());
        };
        let Ok(loader) = self
            .app_state
            .incremental_load_registry
            .get(&export_job.source)
        else {
            warn!(
                "Failed to get loader for '{}', skipping job...",
                export_job.source
            );
            return Ok(());
        };

        // run ingestion

        let run_ingestion = IngestionPipeline {
            source: source.clone(),
        };

        match IngestionRunner::run(&run_ingestion, &loader).await {
            std::result::Result::Ok((total_affected)) => {
                info!(
                    "running ingestion runner {} total affected {}",
                    export_job.tenant_id, total_affected
                )
            }
            Err(err) => warn!("failed ingestion runner"),
        }

        Ok(())
    }
}
