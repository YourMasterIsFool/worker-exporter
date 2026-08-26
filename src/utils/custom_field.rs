use std::collections::HashMap;

use bigdecimal::BigDecimal;
use chrono::NaiveDate;
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::{
    core::traits::orchestrator::{ImporterColumn, TypeDataEnum},
    utils::json_value::{first_non_null_object_value, CUSTOM_FIELD_VALUE_COLUMNS},
};

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

pub fn parse_import_date(value: &str) -> Option<NaiveDate> {
    let value = value.trim();
    NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .ok()
        .or_else(|| NaiveDate::parse_from_str(value, "%Y/%m/%d").ok())
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

    row.get(value_column)
        .or_else(|| {
            let camel_case = match value_column {
                "value_text" => "valueText",
                "value_number" => "valueNumber",
                "value_id" => "valueId",
                "value_bool" => "valueBool",
                "value_date" => "valueDate",
                _ => return None,
            };
            row.get(camel_case)
        })
        .filter(|value| !value.is_null())
}

/// Resolves a custom-field value regardless of whether the source includes a
/// usable `field_type`. This supports both EAV rows and snapshot JSON payloads.
pub fn resolved_custom_field_value(row: &Value) -> Option<&Value> {
    custom_field_value(row).or_else(|| {
        let Value::Object(map) = row else {
            return None;
        };

        CUSTOM_FIELD_VALUE_COLUMNS
            .iter()
            .find_map(|key| map.get(*key).filter(|value| !value.is_null()))
    })
}

/// Groups EAV custom-field rows into `{ parent_id: { field_key: value } }`.
/// `meta` remains a fallback for legacy values that do not use an EAV column.
pub fn group_custom_field_values_by_parent(rows: &[Value]) -> HashMap<String, Value> {
    let mut grouped = HashMap::<String, Map<String, Value>>::new();

    for row in rows {
        let Some(parent_id) = row.get("parent_id").and_then(Value::as_str) else {
            continue;
        };
        let Some(field_key) = row.get("field_key").and_then(Value::as_str) else {
            continue;
        };

        let value = resolved_custom_field_value(row)
            .cloned()
            .or_else(|| first_non_null_object_value(row, &["meta"]))
            .unwrap_or(Value::Null);

        grouped
            .entry(parent_id.to_string())
            .or_default()
            .insert(field_key.to_string(), value);
    }

    grouped
        .into_iter()
        .map(|(parent_id, fields)| (parent_id, Value::Object(fields)))
        .collect()
}

/// Indexes raw EAV custom-field rows by their parent and business field key.
pub fn index_custom_field_rows_by_parent_and_key(rows: &[Value]) -> HashMap<String, Value> {
    rows.iter()
        .filter_map(|row| {
            let parent_id = row.get("parent_id").and_then(Value::as_str)?;
            let field_key = row.get("field_key").and_then(Value::as_str)?;
            Some((format!("{parent_id}_{field_key}"), row.clone()))
        })
        .collect()
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
            resolved.value_date = value.as_str().and_then(parse_import_date);
        }
        Some(TypeDataEnum::Text) | None => resolved.value_text = value_to_text(value),
    }

    resolved
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use serde_json::json;

    use super::{
        custom_field_keys_match, custom_field_value, group_custom_field_values_by_parent,
        index_custom_field_rows_by_parent_and_key, parse_import_date,
        resolve_custom_field_typed_value,
    };
    use crate::core::traits::orchestrator::TypeDataEnum;

    #[test]
    fn resolves_value_using_field_type() {
        let row = json!({"field_type": "TEXT", "value_text": "RR-001"});
        assert_eq!(custom_field_value(&row), Some(&json!("RR-001")));
    }

    #[test]
    fn resolves_camel_case_value_column_using_field_type() {
        let row = json!({"field_type": "TEXT", "valueText": "RR-001"});
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
    fn parses_excel_slash_separated_date() {
        assert_eq!(
            parse_import_date("2026/01/10"),
            NaiveDate::from_ymd_opt(2026, 1, 10)
        );
    }

    #[test]
    fn treats_camel_and_snake_case_keys_as_equivalent() {
        assert!(custom_field_keys_match("noRR", "no_rr"));
    }

    #[test]
    fn groups_values_by_parent_and_field_key() {
        let rows = vec![
            json!({
                "parent_id": "vacancy-1",
                "field_key": "jobGradeId",
                "field_type": "UUID",
                "value_id": "grade-1"
            }),
            json!({
                "parent_id": "vacancy-1",
                "field_key": "legacyField",
                "meta": {"name": "legacy"}
            }),
        ];

        let grouped = group_custom_field_values_by_parent(&rows);

        assert_eq!(grouped["vacancy-1"]["jobGradeId"], json!("grade-1"));
        assert_eq!(
            grouped["vacancy-1"]["legacyField"],
            json!({"name": "legacy"})
        );
    }

    #[test]
    fn indexes_raw_rows_by_parent_and_field_key() {
        let rows = vec![json!({
            "parent_id": "candidate-1",
            "field_key": "height",
            "value_number": "175"
        })];

        let indexed = index_custom_field_rows_by_parent_and_key(&rows);

        assert_eq!(indexed["candidate-1_height"]["value_number"], json!("175"));
    }
}
