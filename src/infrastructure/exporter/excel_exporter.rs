use std::path::Path;

use anyhow::{anyhow, Result};
use excelstream::ExcelWriter;

use crate::core::traits::writter::writter_trait::{ExportRow, WritterTrait};

pub struct ExcelExporter {
    pub file_path: Option<String>,
    pub client: Option<ExcelWriter>,
}

unsafe impl Send for ExcelExporter {}
unsafe impl Sync for ExcelExporter {}
impl Default for ExcelExporter {
    fn default() -> Self {
        Self::new()
    }
}

impl ExcelExporter {
    pub fn new() -> Self {
        Self {
            file_path: None,

            client: None,
        }
    }
}

impl WritterTrait for ExcelExporter {
    fn write_headers(&mut self, headers: &[String]) -> Result<()> {
        if let Some(client) = self.client.as_mut() {
            client.write_header(headers.iter().map(|h| h.as_str()).collect::<Vec<_>>());
        }
        Ok(())
    }

    fn write_rows(&mut self, rows: &[ExportRow]) -> Result<()> {
        if let Some(client) = self.client.as_mut() {
            for row in rows {
                client.write_row(row.fields.iter().map(|f| f.as_str()).collect::<Vec<_>>())?;
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        if let Some(client) = self.client.take() {
            // Panggil fungsi save jika memang mengonsumsi objek,
            // atau biarkan objek ini keluar dari scope agar ter-drop otomatis
            client.save().map_err(|e| anyhow!(e))?;
        }

        Ok(())
    }

    fn init(&mut self, file_path: &str) -> Result<()> {
        let path = Path::new(file_path);
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }

        self.file_path = Some(file_path.to_string());

        let client = ExcelWriter::new(path)?;

        self.client = Some(client);
        Ok(())
    }
}
