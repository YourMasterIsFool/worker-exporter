use anyhow::Ok;
use excelstream::ExcelWriter;

use crate::core::traits::writter::writter_trait::WritterTrait;

pub struct ExcelExporter {
    pub client: Option<ExcelWriter>,
}

impl ExcelExporter {
    pub fn new() -> Self {
        Self { client: None }
    }
}

impl WritterTrait for ExcelExporter {
    fn write_headers(&mut self, headers: &[String]) -> anyhow::Result<()> {
        if let Some(client) = &mut self.client {
            client.write_header(headers)?;
        }
        Ok(())
    }

    fn write_rows(
        &mut self,
        rows: &[crate::core::traits::writter::writter_trait::ExportRow],
    ) -> anyhow::Result<()> {
        todo!()
    }

    fn finish(&mut self) -> anyhow::Result<()> {
        todo!()
    }

    fn init(&mut self, file_path: &str) -> anyhow::Result<()> {
        let client = ExcelWriter::new(file_path)?;
        self.client = Some(client);
        Ok(())
    }
}
