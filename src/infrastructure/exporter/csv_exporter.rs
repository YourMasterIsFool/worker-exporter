use std::{fs::File, path::Path};

use crate::core::traits::writter::writter_trait::{ExportRow, WritterTrait};
use anyhow::{Result, anyhow};
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
    fn write_headers(&mut self, headers: &[String]) -> Result<()> {
        let writter = self
            .writter
            .as_mut()
            .ok_or_else(|| anyhow!("csv writer is not initialized"))?;
        writter.write_record(headers.iter())?;
        writter.flush()?;
        self.written_headers = true;
        Ok(())
    }

    fn write_rows(&mut self, rows: &[ExportRow]) -> Result<()> {
        let writter = self
            .writter
            .as_mut()
            .ok_or_else(|| anyhow!("csv writer is not initialized"))?;
        for row in rows {
            writter.write_record(row.fields.iter())?;
        }
        writter.flush()?;
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        let writter = self
            .writter
            .as_mut()
            .ok_or_else(|| anyhow!("csv writer is not initialized"))?;
        writter.flush()?;
        Ok(())
    }

    fn init(&mut self, file_path: &str) -> Result<()> {
        let path = Path::new(file_path);
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }

        let defined_writter = csv::Writer::from_path(path)?;
        self.writter = Some(defined_writter);
        Ok(())
    }
}
