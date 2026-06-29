use crate::core::traits::writter::writter_trait::{ExportRow, WritterTrait};
use anyhow::{anyhow, Result};
use csv::Writer;
use std::{fs::File, io::BufWriter, path::Path}; // <-- Tambahkan import BufWriter

pub struct CsvExporter {
    // Ganti File menjadi BufWriter<File> di dalam generic Writer
    pub writter: Option<Writer<BufWriter<File>>>,
    pub written_headers: bool,
}

impl Default for CsvExporter {
    fn default() -> Self {
        Self::new()
    }
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
    // Fungsi write_headers, write_rows, dan finish kodingannya TETAP SAMA PERSIS!
    // Tidak ada yang perlu diubah di sana karena BufWriter otomatis bekerja di latar belakang.

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
        writter.flush()?; // Ini akan menguras isi ember RAM langsung ke SSD setiap akhir batch kueri
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

    // ============================================================
    // DI SINI KUNCI PERUBAHANNYA:
    // ============================================================
    fn init(&mut self, file_path: &str) -> Result<()> {
        let path = Path::new(file_path);
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }

        // 1. Buat handle file aslinya di SSD
        let file = File::create(path)?;

        // 2. Bungkus file pakai BufWriter, kita kasih kapasitas "ember" sebesar 128 KB
        // Artinya, data baru bener-bener dikirim lewat kabel USB ke SSD eksternal setiap kali teks CSV terkumpul 128 KB di RAM
        let buffered_file = BufWriter::with_capacity(128 * 1024, file);

        // 3. Masukkan BufWriter tersebut ke fungsi constructor csv
        let defined_writter = csv::Writer::from_writer(buffered_file);

        self.writter = Some(defined_writter);
        Ok(())
    }
}
