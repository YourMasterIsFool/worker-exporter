use anyhow::Result;

pub struct ExportRow {
    pub fields: Vec<String>,
}
pub trait WritterTrait: Send + Sync {
    fn write_headers(&mut self, headers: &[String]) -> Result<()>;
    fn write_rows(&mut self, rows: &[ExportRow]) -> Result<()>;
    fn finish(&mut self) -> Result<()>;
    fn init(&mut self, file_path: &str) -> Result<()>;
}
