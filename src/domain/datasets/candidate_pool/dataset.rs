use crate::core::datasets::dataset_runner::DatasetRunner;
use crate::core::datasets::dataset_trait::DatasetTrait;
use crate::core::traits::exporter::export_writer::ExportRow;
use crate::core::traits::transformer::transform_data::TransformData;
use anyhow::Result;

// use super::row::CandidatePoolDatasetRow;
use crate::infrastructure::warehouse::snapshots::candidate_pool::model::CandidatePoolSnapshotModel;
use crate::infrastructure::warehouse::snapshots::candidate_pool::query::CandidatePoolQuerySnapshot;

pub struct CandidatePoolDataset {
    query: CandidatePoolQuerySnapshot,
}

impl CandidatePoolDataset {
    pub fn new(query: CandidatePoolQuerySnapshot) -> Self {
        Self { query }
    }
}

impl TransformData for CandidatePoolDataset {
    type Output = ExportRow;
    type Input = CandidatePoolSnapshotModel;

    fn transform(&self, inputs: Vec<Self::Input>) -> Vec<Self::Output> {
        let mut result = Vec::with_capacity(inputs.len());

        for (i, row) in inputs.into_iter().enumerate() {
            let export_row = ExportRow {
                fields: vec![(i + 1).to_string(), row.id.to_string(), row.nik, row.name],
            };
            result.push(export_row);
        }
        result

        /* `std::vec::Vec<core::exporter::export_writer::ExportRow>` value */
    }
}
