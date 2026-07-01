use anyhow::{Ok, Result};
use uuid::Uuid;

use crate::{
    core::traits::{load::IncrementalLoadPersitence, pipeline::pipeline_trait::PipelineTrait},
    domain::logging::repo::LoggingRepo,
};
pub struct LoaderRunner;

impl LoaderRunner {
    pub async fn run<P, L, W>(ingestion: &P, load: &L, logging: &W, stream_id: &str) -> Result<i64>
    where
        P: PipelineTrait + Send + Sync + ?Sized,
        L: IncrementalLoadPersitence + Send + Sync + ?Sized,
        W: LoggingRepo + Send + Sync + ?Sized,
    {
        let mut cursor: Option<Uuid> = None;
        let mut total_affected_rows = 0;

        loop {
            let result = ingestion.run(cursor, 10, "tenants").await?;
            if result.0.is_none() {
                break;
            }
            println!("affeced rows {:?}", result.1);

            load.load(&result.1).await?;
            cursor = result.0;

            total_affected_rows += result.1.len() as i64;
            // parsing into option string
            let parse_cursor: Option<String> = cursor.map(|f| f.to_string());

            // update cursor and processed count
            logging
                .update_cursor_and_processed_count(stream_id, parse_cursor, total_affected_rows)
                .await?;
        }

        Ok(total_affected_rows)
    }
}
