use serde_json::Value;

/// Resolves an export column from a list of named JSON sources.
///
/// A column may be explicitly qualified, for example `vacancy.name` or
/// `candidate_data.email`. Unqualified columns are resolved in source order.
pub fn resolve_export_value(column: &str, sources: &[(&str, &Value)]) -> Value {
    let column = column.trim();
    if column.is_empty() {
        return Value::Null;
    }

    if let Some((source_name, path)) = column.split_once('.') {
        if let Some((_, source)) = sources.iter().find(|(name, _)| *name == source_name) {
            return resolve_json_path(source, path)
                .cloned()
                .unwrap_or(Value::Null);
        }
    }

    for (_, source) in sources {
        if let Some(value) = resolve_json_path(source, column) {
            return value.clone();
        }
    }

    Value::Null
}

pub fn export_value_to_string(value: Value) -> String {
    match value {
        Value::Null => "-".to_string(),
        Value::String(value) => value,
        Value::Number(value) => value.to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Array(_) => value.to_string(),
        Value::Object(object) => object
            .get("name")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| Value::Object(object).to_string()),
    }
}

fn resolve_json_path<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    if path.is_empty() {
        return Some(value);
    }

    path.split('.').try_fold(value, |current, key| {
        if key.is_empty() {
            return None;
        }

        if let Some(object) = current.as_object() {
            return object_value(object, key);
        }

        current.as_array()?.get(key.parse::<usize>().ok()?)
    })
}

fn object_value<'a>(object: &'a serde_json::Map<String, Value>, key: &str) -> Option<&'a Value> {
    object
        .get(key)
        .or_else(|| object.get(&camel_to_snake(key)))
        .or_else(|| object.get(&snake_to_camel(key)))
}

fn camel_to_snake(value: &str) -> String {
    let mut result = String::with_capacity(value.len());
    for character in value.chars() {
        if character.is_ascii_uppercase() {
            if !result.is_empty() {
                result.push('_');
            }
            result.push(character.to_ascii_lowercase());
        } else {
            result.push(character);
        }
    }
    result
}

fn snake_to_camel(value: &str) -> String {
    let mut result = String::with_capacity(value.len());
    let mut uppercase_next = false;
    for character in value.chars() {
        if character == '_' {
            uppercase_next = true;
        } else if uppercase_next {
            result.push(character.to_ascii_uppercase());
            uppercase_next = false;
        } else {
            result.push(character);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{export_value_to_string, resolve_export_value};

    #[test]
    fn resolves_qualified_and_unqualified_values() {
        let bucket = json!({"id": "bucket-1"});
        let candidate_data = json!({"name": "Applicant One"});
        let vacancy = json!({"name": "Rust Engineer"});
        let sources = [
            ("applicant_bucket", &bucket),
            ("candidate_data", &candidate_data),
            ("vacancy", &vacancy),
        ];

        assert_eq!(
            resolve_export_value("applicant_bucket.id", &sources),
            json!("bucket-1")
        );
        assert_eq!(
            resolve_export_value("candidate_data.name", &sources),
            json!("Applicant One")
        );
        assert_eq!(
            resolve_export_value("name", &sources),
            json!("Applicant One")
        );
    }

    #[test]
    fn formats_missing_values_as_dash() {
        assert_eq!(export_value_to_string(json!(null)), "-");
        assert_eq!(export_value_to_string(json!(true)), "true");
    }

    #[test]
    fn formats_named_objects_using_name() {
        assert_eq!(
            export_value_to_string(json!({"id": "gender-id", "name": "Laki-laki"})),
            "Laki-laki"
        );
    }

    #[test]
    fn resolves_array_indexes_and_snake_case_aliases() {
        let source = json!({
            "contacts": [{"contact_name": "Family One"}],
            "created_at": "2026-01-01"
        });
        let sources = [("candidate_data", &source)];

        assert_eq!(
            resolve_export_value("contacts.0.contactName", &sources),
            json!("Family One")
        );
        assert_eq!(
            resolve_export_value("createdAt", &sources),
            json!("2026-01-01")
        );
    }
}
