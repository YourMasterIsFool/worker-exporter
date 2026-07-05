use std::sync::Arc;

use crate::core::traits::dataset_trait::ImporterDataset;
use crate::core::traits::orchestrator::ImporterColumn;
use crate::{core::types::WhereField, domain::logging::repo::LoggingRepo};

pub struct ImportRunner<'a> {
    dataset: Box<dyn ImporterDataset>,
    logging: Box<dyn LoggingRepo>,
    fields: &'a [ImporterColumn],
    id: &'a str,
    filters: Option<Vec<WhereField>>,
    limit: i64,
}

impl<'a> ImportRunner<'a> {
    pub async fn run<D, L, W>(&self, cursor: &mut Option<String>) -> anyhow::Result<()>
where {
        Ok(())
    }
}
