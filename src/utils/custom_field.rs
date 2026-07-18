use bigdecimal::BigDecimal;
use chrono::NaiveDate;
use serde_json::Value;
use uuid::Uuid;

use crate::core::traits::orchestrator::{ImporterColumn, TypeDataEnum};

#[derive(Debug, Default)]
pub struct CustomFieldTypedValue {
    pub value_text: Option<String>,
    pub value_number: Option<BigDecimal>,
    pub value_id: Option<Uuid>,
    pub value_bool: Option<bool>,
    pub value_date: Option<NaiveDate>,
}

pub fn normalize_custom_field_key(value: &str) -> String {
    value
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

pub fn custom_field_keys_match(left: &str, right: &str) -> bool {
    normalize_custom_field_key(left) == normalize_custom_field_key(right)
}

/// The optional `key` in a custom-field unique rule is its business key.
/// Use the column path only when the configuration does not supply one.
pub fn resolve_custom_field_key(field: &ImporterColumn, fallback_column: &str) -> String {
    field
        .validation_rules
        .as_ref()
        .and_then(|rules| {
            rules.iter().find_map(|rule| {
                rule.validator
                    .eq_ignore_ascii_case("unique")
                    .then(|| rule.options.get("key").and_then(Value::as_str))
                    .flatten()
                    .map(str::trim)
                    .filter(|key| !key.is_empty())
                    .map(ToString::to_string)
            })
        })
        .unwrap_or_else(|| fallback_column.to_string())
}

pub fn custom_field_value(row: &Value) -> Option<&Value> {
    let field_type = row.get("field_type")?.as_str()?.trim().to_uppercase();
    let value_column = match field_type.as_str() {
        "TEXT" => "value_text",
        "NUMBER" => "value_number",
        "UUID" | "ID" => "value_id",
        "BOOLEAN" | "BOOL" => "value_bool",
        "DATE" => "value_date",
        _ => return None,
    };

    row.get(value_column).filter(|value| !value.is_null())
}

fn value_to_uuid(value: &Value) -> Option<Uuid> {
    match value {
        Value::String(value) => Uuid::parse_str(value.trim()).ok(),
        Value::Array(values) => values.iter().find_map(value_to_uuid),
        _ => None,
    }
}

fn resolved_fetcher_uuid(value: &Value, meta: &Value, fetcher_key: Option<&str>) -> Option<Uuid> {
    value_to_uuid(value).or_else(|| {
        let keys = fetcher_key
            .filter(|key| !key.trim().is_empty())
            .into_iter()
            .chain(std::iter::once("id"));

        match meta {
            Value::Object(object) => keys
                .clone()
                .find_map(|key| object.get(key).and_then(value_to_uuid)),
            Value::Array(items) => items.iter().find_map(|item| match item {
                Value::Object(object) => keys
                    .clone()
                    .find_map(|key| object.get(key).and_then(value_to_uuid)),
                _ => value_to_uuid(item),
            }),
            _ => None,
        }
    })
}

fn value_to_text(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        value => Some(value.to_string()),
    }
}

/// Maps one resolved import value to the single EAV value column allowed by
/// the custom-field entity. Fetcher metadata is used only to recover an UUID
/// when the fetcher result is nested.
pub fn resolve_custom_field_typed_value(
    field_type: Option<&TypeDataEnum>,
    value: &Value,
    meta: &Value,
    fetcher_key: Option<&str>,
) -> CustomFieldTypedValue {
    let mut resolved = CustomFieldTypedValue::default();

    match field_type {
        Some(TypeDataEnum::Uuid) => {
            resolved.value_id = resolved_fetcher_uuid(value, meta, fetcher_key)
        }
        Some(TypeDataEnum::Number) => {
            let number = value.as_f64().or_else(|| {
                value
                    .as_str()
                    .and_then(|value| value.trim().parse::<f64>().ok())
            });
            resolved.value_number = number.and_then(|value| BigDecimal::try_from(value).ok());
        }
        Some(TypeDataEnum::Boolean) => resolved.value_bool = value.as_bool(),
        Some(TypeDataEnum::Date) => {
            resolved.value_date = value
                .as_str()
                .and_then(|value| NaiveDate::parse_from_str(value, "%Y-%m-%d").ok());
        }
        Some(TypeDataEnum::Text) | None => resolved.value_text = value_to_text(value),
    }

    resolved
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{custom_field_keys_match, custom_field_value, resolve_custom_field_typed_value};
    use crate::core::traits::orchestrator::TypeDataEnum;

    #[test]
    fn resolves_value_using_field_type() {
        let row = json!({"field_type": "TEXT", "value_text": "RR-001"});
        assert_eq!(custom_field_value(&row), Some(&json!("RR-001")));
    }

    #[test]
    fn resolves_uuid_from_fetcher_metadata() {
        let value = json!(null);
        let meta = json!({"id": "019c8d92-25fa-7c5b-8c9a-0e01077568d4"});
        let typed =
            resolve_custom_field_typed_value(Some(&TypeDataEnum::Uuid), &value, &meta, Some("id"));
        assert!(typed.value_id.is_some());
    }

    #[test]
    fn treats_camel_and_snake_case_keys_as_equivalent() {
        assert!(custom_field_keys_match("noRR", "no_rr"));
    }
}
