use excelstream::{streaming_reader::RowIterator, CellValue};

use crate::core::traits::dataset_trait::{ImportRow, ImporterDataset};
use std::collections::HashMap;

pub struct AtsImportLsiVacancyDataset {}

#[async_trait::async_trait]
impl ImporterDataset for AtsImportLsiVacancyDataset {
    async fn import(
        &self,
        rows: &[RowIterator],
        mapper_columns: &HashMap<i32, String>,
    ) -> anyhow::Result<HashMap<String, Vec<ImportRow>>> {
        let mut final_result: HashMap<String, Vec<ImportRow>> = HashMap::new();

        // for row_iterator in rows {
        //     for cell_result in row_iterator {
        //         let row_data: Vec<CellValue> = cell_result?;

        //         for (index, val) in row_data.into_iter().enumerate() {
        //             let key = match mapper_columns.get(&(index as i32)) {
        //                 Some(k) if !k.is_empty() => k,
        //                 _ => continue,
        //             };

        //             // Tentukan kategori dan field tanpa banyak alokasi string
        //             let (entry_key, field_name) =
        //                 if let Some(stripped) = key.strip_prefix("customFields.") {
        //                     ("customFields", stripped.to_string())
        //                 } else {
        //                     ("main", key.clone())
        //                 };

        //             final_result
        //                 .entry(entry_key.to_string())
        //                 .or_default()
        //                 .push(ImportRow {
        //                     field: field_name,
        //                     value: val.into(),
        //                 });
        //         }
        //     }
        // }
        Ok(final_result)
    }
}
