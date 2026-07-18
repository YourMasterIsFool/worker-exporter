use calamine::Data;
use excelstream::streaming_reader::RowIterator;

use crate::core::{
    traits::{dataset_trait::ImporterDataset, orchestrator::ImporterColumn},
    types::{ImportDatasetConfig, ImportRow},
};
use std::collections::HashMap;

pub struct AtsImportLsiVacancyDataset {}

#[async_trait::async_trait]
impl ImporterDataset for AtsImportLsiVacancyDataset {
    async fn import(
        &self,
        rows: &[Data],
        config: &ImportDatasetConfig,
    ) -> anyhow::Result<HashMap<String, Vec<ImportRow>>> {
        let mut final_result: HashMap<String, Vec<ImportRow>> = HashMap::new();

        // for (index, importer_col) in fields.iter().enumerate() {
        //     let row_value = rows.get(index);

        //     // 1. Konversi data Calamine ke serde_json::Value
        //     let value = match row_value {
        //         Some(Data::String(s)) => serde_json::Value::String(s.clone()),
        //         Some(Data::Float(f)) => {
        //             serde_json::Value::Number(serde_json::Number::from_f64(*f).unwrap_or(0.into()))
        //         }
        //         Some(Data::Int(i)) => serde_json::Value::Number((*i).into()),
        //         Some(Data::Bool(b)) => serde_json::Value::Bool(*b),
        //         _ => serde_json::Value::Null,
        //     };

        //     // 2. Split berdasarkan pipe '|' untuk mendukung multi-table
        //     let paths: Vec<&str> = importer_col.column.split('|').collect();

        //     for path in paths {
        //         // 3. Split berdasarkan titik '.' untuk memisahkan table dan field
        //         let (group_name, field_key) = if let Some((group, key)) = path.split_once('.') {
        //             (group, key)
        //         } else {
        //             ("others", path)
        //         };

        //         // 4. Masukkan ke HashMap untuk grup terkait
        //         final_result
        //             .entry(group_name.to_string())
        //             .or_default()
        //             .push(ImportRow {
        //                 field: field_key.to_string(),
        //                 value: value.clone(), // Clone value karena bisa dipakai di beberapa grup
        //                 fetcher: importer_col.fetcher.clone(),
        //                 type_data: importer_col.type_data.clone(),
        //             });
        //     }
        // }

        Ok(final_result)
    }
}
