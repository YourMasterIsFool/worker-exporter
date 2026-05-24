use std::fs::File;

use crate::core::traits::writter::writter_trait::{ExportRow, WritterTrait};
use anyhow::{Ok, Result};
use csv::Writer;

pub struct CsvExporter {
    pub writter: Option<Writer<File>>,
    pub written_headers: bool,
}

impl CsvExporter {
    pub fn new() -> Self {
        Self {
            writter: None,
            written_headers: false,
        }
    }
}

impl WritterTrait for CsvExporter {
    fn write_headers(&mut self, headers: Vec<String>) -> Result<()> {
        // writte header
        if let Some(writter) = self.writter.as_mut() {
            writter
                .write_record(headers.iter())
                .map_err(|e| anyhow::anyhow!(e))?;
            self.written_headers = true;
        }
        Ok(())
    }

    fn write_rows(&mut self, rows: &[ExportRow]) -> Result<()> {
        if let Some(writter) = &mut self.writter {
            for row in rows {
                writter
                    .write_record(row.fields.iter())
                    .map_err(|e| anyhow::anyhow!(e))?;
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        if let Some(writter) = self.writter.as_mut() {
            writter.flush().map_err(|e| anyhow::anyhow!(e))?;
        }
        Ok(())
    }

    fn init(&mut self, file_path: &str) -> Result<()> {
        let defined_writter = csv::Writer::from_path(file_path).map_err(|e| anyhow::anyhow!(e))?;
        self.writter = Some(defined_writter);
        Ok(())
    }
}
