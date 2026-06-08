use anyhow::bail;
use tokio::task;

use crate::{
    core::{
        datasets::dataset_trait::Dataset,
        traits::{
            extractor::incremental_source::WhereField,
            load::LoadEnum,
            orchestrator::ExporterColumn,
            writter::writter_trait::{ExportRow, WritterTrait},
        },
    },
    domain::logging::repo::LoggingRepo,
};
pub struct ExportRunner;

impl ExportRunner {
    pub async fn run<D, L, W>(
        dataset: &D,
        logging: &L,
        writter: &mut W,
        fields: &[ExporterColumn],
        id: &str,
        filters: Option<Vec<WhereField>>,
        cursor: &mut Option<String>,
    ) -> anyhow::Result<()>
    where
        D: Dataset + Send + Sync + ?Sized,
        L: LoggingRepo + Send + Sync + ?Sized,
        W: WritterTrait + Send + Sync + ?Sized,
    {
        let mut headers: Vec<String> = vec!["number".into()];
        let mut counter = 1;

        let custom_headers = fields.iter().map(|f| f.name.clone()).collect::<Vec<_>>();

        // custom adding for numbering/counter
        headers.extend(custom_headers);

        writter.write_headers(&headers)?;

        let mut columns = fields.iter().map(|f| f.column.clone()).collect::<Vec<_>>(); // writing_headers

        let strategy_enum = LoadEnum::Select(filters, Some(columns));
        let mut processed_count: i64 = 0;

        // loop {
        //     let (result, new_cursor) = dataset
        //         .fetch_batch(&strategy_enum, cursor.clone(), 100)
        //         .await?;
        //     if result.is_empty() {
        //         break;
        //     }

        //     let result_with_counter: Vec<ExportRow> = result
        //         .into_iter()
        //         .enumerate()
        //         .map(|(index, mut f)| {
        //             let current_counter = index + counter;
        //             // let mut export_row = ExportRow {
        //             //     fields: vec![current_counter.to_string()],
        //             // };

        //             // export_row.fields.extend(f.fields);
        //             // export_row
        //             //
        //             f.fields.insert(0, current_counter.to_string().to_string());
        //             f
        //         })
        //         .collect::<Vec<_>>();

        //     writter.write_rows(&result_with_counter)?;
        //     processed_count += result_with_counter.len() as i64;

        //     counter += result_with_counter.len();

        //     if new_cursor.is_none() {
        //         // last page — rows already written, exit cleanly
        //         break;
        //     }

        //     *cursor = new_cursor;
        //     logging
        //         .update_cursor_and_processed_count(id, cursor.clone(), processed_count)
        //         .await?;
        // }
        let keystone_cursor = cursor.clone();

        for i in 0..8000 {
            // 1. Kueri nyata ke database! Ini akan menguji koneksi DB di setiap putaran
            let (result, new_cursor) = dataset
                .fetch_batch(&strategy_enum, cursor.clone(), 1000)
                .await?;

            // Jika data habis (pasti habis di putaran selanjutnya jika cursor tidak direset)
            if result.is_empty() {
                // Trik: Paksa reset cursor ke posisi awal agar putaran berikutnya database mengembalikan data lagi
                *cursor = keystone_cursor.clone();
                continue;
            }

            // 2. Proses penomoran urut (Counter)
            let result_with_counter: Vec<ExportRow> = result
                .into_iter()
                .enumerate()
                .map(|(index, mut f)| {
                    let current_counter = index + counter;

                    let mut optimized_fields = Vec::with_capacity(f.fields.len() + 1);
                    optimized_fields.push(current_counter.to_string());
                    optimized_fields.extend(f.fields);

                    f.fields = optimized_fields;
                    f
                })
                .collect::<Vec<_>>();

            // 3. Tulis ke Excel
            writter.write_rows(&result_with_counter)?;
            processed_count += result_with_counter.len() as i64;
            counter += result_with_counter.len();

            // 4. Update status logging ke DB secara nyata
            logging
                .update_cursor_and_processed_count(id, new_cursor.clone(), processed_count)
                .await?;

            // Log progress ke terminal setiap 100 kueri database
            if i % 100 == 0 {
                println!("Database Stress-Test: Sukses kueri banyolan ke-{}...", i);
            }

            // 5. Geser cursor hasil DB asli, tapi jika menyentuh halaman terakhir (None),
            // kita paksa reset ke awal untuk putaran loop selanjutnya
            if new_cursor.is_none() {
                *cursor = keystone_cursor.clone();
            } else {
                *cursor = new_cursor;
            }

            task::yield_now().await;

            //bikin nafas server

            tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        }
        Ok(())
    }
}
