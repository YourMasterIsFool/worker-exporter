use std::collections::HashMap;

use serde_json::Value;

pub type ExportTransform = fn(&Value, &str) -> String;

pub struct TransformRegistry {
    transforms: HashMap<String, ExportTransform>,
}

impl TransformRegistry {
    pub fn new() -> Self {
        Self {
            transforms: HashMap::new(),
        }
    }

    pub fn register(&mut self, name: &str, transform: ExportTransform) -> anyhow::Result<()> {
        if self
            .transforms
            .insert(name.to_ascii_lowercase(), transform)
            .is_some()
        {
            return Err(anyhow::anyhow!("transform already registered: {name}"));
        }
        Ok(())
    }

    pub fn resolve(&self, spec: Option<&str>, value: &Value) -> String {
        let Some(spec) = spec else {
            return value_to_string(value);
        };
        let (name, argument) = spec.split_once(':').unwrap_or((spec, ""));
        self.transforms
            .get(&name.to_ascii_lowercase())
            .map(|transform| transform(value, argument))
            .unwrap_or_else(|| value_to_string(value))
    }
}

pub fn build_transform_registry() -> TransformRegistry {
    let mut registry = TransformRegistry::new();
    registry
        .register("join", join)
        .expect("register join transform");
    registry
        .register("uppercase", uppercase)
        .expect("register uppercase transform");
    registry
        .register("lowercase", lowercase)
        .expect("register lowercase transform");
    registry
        .register("date", date)
        .expect("register date transform");
    registry
}

pub fn value_to_string(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Array(values) => values
            .iter()
            .map(value_to_string)
            .collect::<Vec<_>>()
            .join(", "),
        value => value.to_string(),
    }
}

fn join(value: &Value, separator: &str) -> String {
    let separator = if separator.is_empty() {
        ", "
    } else {
        separator
    };
    match value {
        Value::Array(values) => values
            .iter()
            .map(value_to_string)
            .collect::<Vec<_>>()
            .join(separator),
        value => value_to_string(value),
    }
}

fn uppercase(value: &Value, _: &str) -> String {
    value_to_string(value).to_uppercase()
}

fn lowercase(value: &Value, _: &str) -> String {
    value_to_string(value).to_lowercase()
}

fn date(value: &Value, format: &str) -> String {
    use chrono::{DateTime, NaiveDate, NaiveDateTime};

    let value = value_to_string(value);
    let format = if format.is_empty() {
        "%Y-%m-%d"
    } else {
        format
    };
    DateTime::parse_from_rfc3339(&value)
        .map(|date| date.format(format).to_string())
        .or_else(|_| {
            NaiveDateTime::parse_from_str(&value, "%Y-%m-%d %H:%M:%S")
                .map(|date| date.format(format).to_string())
        })
        .or_else(|_| {
            NaiveDate::parse_from_str(&value, "%Y-%m-%d")
                .map(|date| date.format(format).to_string())
        })
        .unwrap_or(value)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::build_transform_registry;

    #[test]
    fn joins_many_values_with_configured_separator() {
        assert_eq!(
            build_transform_registry().resolve(Some("join: | "), &json!(["A", "B"])),
            "A | B"
        );
    }
}
