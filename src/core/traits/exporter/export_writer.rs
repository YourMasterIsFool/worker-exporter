use anyhow::Result;

pub struct ExportRow {
    pub fields: Vec<String>,
}
pub trait ExportWriter: Send + Sync {
    fn write_headers(&mut self, headers: Vec<String>) -> Result<()>;
    fn write_rows(&mut self, rows: &[ExportRow]) -> Result<()>;
    fn finish(&mut self) -> Result<()>;
}
