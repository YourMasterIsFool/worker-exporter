use std::path::Path;

use anyhow::{anyhow, Result};
use rust_xlsxwriter::Workbook;

use crate::core::{traits::writter::writter_trait::WritterTrait, types::ExportRow};

pub struct ExcelExporterRs {
    pub file_path: Option<String>,
    pub workbook: Option<Workbook>,
    pub current_row: u32,
}

unsafe impl Send for ExcelExporterRs {}
unsafe impl Sync for ExcelExporterRs {}

impl Default for ExcelExporterRs {
    fn default() -> Self {
        Self::new()
    }
}

impl ExcelExporterRs {
    pub fn new() -> Self {
        Self {
            file_path: None,
            workbook: None,
            current_row: 0,
        }
    }
}

impl WritterTrait for ExcelExporterRs {
    fn write_headers(&mut self, headers: &[String]) -> Result<()> {
        let workbook = self
            .workbook
            .as_mut()
            .ok_or_else(|| anyhow!("excel_rs writer is not initialized"))?;
        let worksheet = workbook
            .worksheets_mut()
            .first_mut()
            .ok_or_else(|| anyhow!("excel_rs worksheet is missing"))?;

        worksheet.write_row(
            self.current_row,
            0,
            headers.iter().map(|header| header.as_str()),
        )?;
        self.current_row += 1;
        Ok(())
    }

    fn write_rows(&mut self, rows: &[ExportRow]) -> Result<()> {
        let workbook = self
            .workbook
            .as_mut()
            .ok_or_else(|| anyhow!("excel_rs writer is not initialized"))?;
        let worksheet = workbook
            .worksheets_mut()
            .first_mut()
            .ok_or_else(|| anyhow!("excel_rs worksheet is missing"))?;

        for row in rows {
            worksheet.write_row(
                self.current_row,
                0,
                row.fields.iter().map(|field| field.as_str()),
            )?;
            self.current_row += 1;
        }

        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        let mut workbook = self
            .workbook
            .take()
            .ok_or_else(|| anyhow!("excel_rs writer is not initialized"))?;
        let file_path = self
            .file_path
            .as_deref()
            .ok_or_else(|| anyhow!("excel_rs output path is missing"))?;

        workbook.save(file_path).map_err(|error| anyhow!(error))
    }

    fn init(&mut self, file_path: &str) -> Result<()> {
        let path = Path::new(file_path);
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }

        self.file_path = Some(file_path.to_string());

        let mut workbook = Workbook::new();
        workbook.add_worksheet_with_constant_memory();
        self.workbook = Some(workbook);
        self.current_row = 0;
        Ok(())
    }
}
