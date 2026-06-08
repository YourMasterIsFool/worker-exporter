use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExportRow {
    pub fields: Vec<String>,
}

impl ExportRow {
    pub fn validate_json<T: serde::Serialize>(
        value: T,
        columns: &[String],
    ) -> anyhow::Result<Self> {
        let parsed_json = serde_json::to_value(value)?;
        let mut result: Vec<String> = vec![];

        if let Some(object) = parsed_json.as_object() {
            for field in columns {
                if let Some(value) = object.get(field) {
                    // 💡 FIX: Ekstrak string tanpa membawa tanda petik dua JSON
                    let formatted_value = match value {
                        serde_json::Value::String(s) => s.clone(),
                        serde_json::Value::Number(n) => n.to_string(),
                        serde_json::Value::Bool(b) => b.to_string(),
                        serde_json::Value::Null => String::from("-"),
                        _ => value.to_string(), // Untuk array atau objek bersarang
                    };
                    result.push(formatted_value);
                } else {
                    result.push(String::from("-"));
                }
            }
        }

        Ok(Self { fields: result })
    }
}
pub trait WritterTrait: Send + Sync {
    fn write_headers(&mut self, headers: &[String]) -> Result<()>;
    fn write_rows(&mut self, rows: &[ExportRow]) -> Result<()>;
    fn finish(&mut self) -> Result<()>;
    fn init(&mut self, file_path: &str) -> Result<()>;
}
