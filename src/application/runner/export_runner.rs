use anyhow::bail;

use crate::{
    core::{datasets::dataset_trait::Dataset, traits::writter::writter_trait::WritterTrait},
    domain::logging::repo::LoggingRepo,
};
pub struct ExportRunner;

impl ExportRunner {
    pub async fn run<D, L, W>(
        dataset: &D,
        logging: &L,
        writter: &mut W,
        id: &str,
    ) -> anyhow::Result<()>
    where
        D: Dataset + Send + Sync + ?Sized,
        L: LoggingRepo + Send + Sync + ?Sized,
        W: WritterTrait + Send + Sync + ?Sized,
    {
        let mut cursor: Option<String> = None;
        let headers: Vec<String> = vec![
            "id".into(),
            "tenant_name".into(),
            "tenant_code".into(),
            "tenant_id".into(),
        ];
        // writing_headers
        writter.write_headers(&headers)?;
        let mut processed_count: i64 = 0;
        loop {
            let (result, new_cursor) = dataset.fetch_batch(cursor.clone(), 100).await?;
            if result.is_empty() {
                break;
            }
            writter.write_rows(&result)?;
            processed_count += result.len() as i64;

            if new_cursor.is_none() {
                // last page — rows already written, exit cleanly
                break;
            }

            if new_cursor == cursor {
                bail!("dataset cursor did not advance");
            }

            cursor = new_cursor;
            logging
                .updateCursorAndProcessedCount(id, cursor.clone(), processed_count)
                .await?;
        }

        writter.finish()?;
        Ok(())
    }
}
