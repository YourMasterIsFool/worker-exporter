use std::collections::{HashMap, HashSet};

use serde_json::Value;

pub const CUSTOM_FIELD_VALUE_COLUMNS: &[&str] = &[
    "value_text",
    "value_number",
    "value_id",
    "value_bool",
    "value_date",
];

pub fn first_object_value(source: Option<&Value>, keys: &[&str]) -> Option<Value> {
    let Value::Object(map) = source? else {
        return None;
    };

    keys.iter().find_map(|key| map.get(*key).cloned())
}

pub fn first_non_null_object_value(value: &Value, keys: &[&str]) -> Option<Value> {
    let Value::Object(map) = value else {
        return None;
    };

    keys.iter()
        .find_map(|key| map.get(*key).filter(|value| !value.is_null()).cloned())
}

pub fn values_at_path(value: Value, path: Option<&str>) -> Vec<Value> {
    let Some(path) = path.map(str::trim).filter(|path| !path.is_empty()) else {
        return vec![value];
    };

    path.split('.').fold(vec![value], |values, segment| {
        values
            .into_iter()
            .flat_map(|value| match value {
                Value::Object(object) => object.get(segment).cloned().into_iter().collect(),
                Value::Array(values) => values
                    .into_iter()
                    .filter_map(|value| match value {
                        Value::Object(object) => object.get(segment).cloned(),
                        _ => None,
                    })
                    .collect(),
                _ => vec![],
            })
            .collect()
    })
}

pub fn flatten_values(values: Vec<Value>) -> Vec<Value> {
    values
        .into_iter()
        .flat_map(|value| match value {
            Value::Array(values) => flatten_values(values),
            value => vec![value],
        })
        .collect()
}

pub fn value_as_keyword_list(value: &Value) -> Vec<String> {
    match value {
        Value::Null => vec![],
        Value::String(value) if !value.trim().is_empty() => vec![value.trim().to_string()],
        Value::String(_) => vec![],
        Value::Array(values) => values.iter().flat_map(value_as_keyword_list).collect(),
        Value::Bool(value) => vec![value.to_string()],
        Value::Number(value) => vec![value.to_string()],
        value => vec![value.to_string()],
    }
}

/// Returns the distinct string values stored under one field in JSON rows.
/// This is useful when constructing batched fetcher filters.
pub fn unique_string_field_values(rows: &[Value], field: &str) -> Vec<String> {
    rows.iter()
        .filter_map(|row| row.get(field).and_then(Value::as_str))
        .map(ToOwned::to_owned)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect()
}

/// Indexes JSON rows by a string field without cloning the row payloads.
pub fn index_rows_by_string<'a>(rows: &'a [Value], field: &str) -> HashMap<&'a str, &'a Value> {
    rows.iter()
        .filter_map(|row| row.get(field).and_then(Value::as_str).map(|key| (key, row)))
        .collect()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{index_rows_by_string, unique_string_field_values};

    #[test]
    fn returns_distinct_string_values_from_json_rows() {
        let rows = vec![
            json!({"id": "candidate-1"}),
            json!({"id": "candidate-1"}),
            json!({"id": "candidate-2"}),
            json!({"id": null}),
        ];

        let ids = unique_string_field_values(&rows, "id");

        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&"candidate-1".to_string()));
        assert!(ids.contains(&"candidate-2".to_string()));
    }

    #[test]
    fn indexes_rows_without_cloning_payloads() {
        let rows = vec![json!({"id": "candidate-1", "name": "A"})];

        let indexed = index_rows_by_string(&rows, "id");

        assert_eq!(indexed["candidate-1"]["name"], json!("A"));
    }
}
