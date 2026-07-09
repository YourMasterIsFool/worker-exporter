use anyhow::{Context, Result};
use calamine::Data;
use serde_json::Value;

pub fn reformated_value(key: &str, value: &serde_json::Value) -> Result<String> {
    let val = value[key]
        .as_str()
        .context(format!("missing {key}"))?
        .to_string();

    Ok(val)
}

pub fn calamine_to_json(dt: &Data) -> Value {
    match dt {
        Data::Int(i) => Value::from(*i),
        Data::Float(f) => Value::from(*f),
        Data::String(s) => Value::String(s.clone()),
        Data::Bool(b) => Value::Bool(*b),
        Data::DateTime(d) => Value::String(d.to_string()),
        _ => Value::Null,
    }
}
