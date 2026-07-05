use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::core::types::ExportRow;

pub trait WritterTrait: Send + Sync {
    fn write_headers(&mut self, headers: &[String]) -> Result<()>;
    fn write_rows(&mut self, rows: &[ExportRow]) -> Result<()>;
    fn finish(&mut self) -> Result<()>;
    fn init(&mut self, file_path: &str) -> Result<()>;
}
