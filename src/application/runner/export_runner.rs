use crate::core::datasets::dataset_trait::DatasetTrait;
pub struct ExportRunner;

// impl ExportRunner {
//     pub async fn run<D, F>(&self, dataset: &D, mut process: F) -> anyhow::Result<()>
//     where
//         D: DatasetTrait,
//         F: FnMut(Vec<D::Output>) -> anyhow::Result<()>,
//     {
//         let mut cursor: Option<D::Cursor> = None;
//         loop {
//             let (data, last_cursor) = dataset.fetch_batch(cursor.clone(), 100).await?;

//             if data.is_empty() {
//                 break;
//             }

//             // process the data

//             process(data)?;

//             // set last cursor in to cursor
//             cursor = last_cursor;
//         }

//         Ok(())
//     }
// }
