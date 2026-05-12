use anyhow::{Ok, Result};
use uuid::Uuid;

use crate::core::traits::{
    load::{IncrementalLoad, UpsertLoader},
    pipeline::pipeline_trait::PipelineTrait,
};
pub struct IngestionRunner;

// impl IngestionRunner {
//     pub async fn run<D, F>(&self, dataset: &D, mut process: F) -> anyhow::Result<()>
//     where
//         D: PipelineTra,
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

impl IngestionRunner {
    pub async fn run<P, L>(p: &P, l: &L) -> Result<i64>
    where
        P: PipelineTrait + Send + Sync,
        L: IncrementalLoad + Send + Sync,
    {
        let mut cursor: Option<Uuid> = None;
        let mut total_affected_rows = 0;

        loop {
            let result = p.run(cursor, 10).await?;
            if result.0.is_none() {
                break;
            }
            println!("affeced rows {:?}", result.1);

            l.load(&result.1).await?;
            cursor = result.0;

            total_affected_rows += result.1.len() as i64;
        }

        Ok(total_affected_rows)
    }
}
