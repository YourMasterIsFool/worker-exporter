use std::{collections::HashMap, sync::Arc};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::{
    domain::logging::{entity::LoggingEntity, repo::LoggingRepo},
    errors::AppError,
    infrastructure::loader::warehouse::warehouse_logging_import::WarehouseLoggingImport,
    utils::{custom_field_keys_match, custom_field_value},
};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ImportLoggingDetail {
    /// Counts are calculated from rows in `import_config.main_table` / `master_table`.
    #[serde(rename = "total_processed")]
    pub total_processed: usize,
    #[serde(rename = "total_success")]
    pub total_success: usize,
    #[serde(rename = "total_failed")]
    pub total_failed: usize,
    /// Display labels from `import_config.fields[].header`.
    pub headers: Vec<ImportLoggingHeader>,
    /// Imported rows keyed by the corresponding `headers[].key`.
    pub content: Vec<Map<String, Value>>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ImportLoggingHeader {
    pub label: String,
    pub key: String,
}

#[derive(Debug, Clone, Default)]
struct ImportLoggingCounts {
    total_processed: usize,
    total_success: usize,
    total_failed: usize,
}

#[derive(Debug, Deserialize)]
struct StoredImportConfig {
    fields: Vec<StoredImportField>,
    #[serde(alias = "main_table")]
    master_table: String,
    #[serde(default)]
    sequential_table: Option<Vec<StoredSequentialTable>>,
}

#[derive(Debug, Deserialize)]
struct StoredImportField {
    header: String,
    column: String,
    #[serde(default)]
    select: Option<String>,
    #[serde(default)]
    is_child: bool,
    #[serde(default)]
    fetcher_field_key: Option<String>,
    #[serde(default)]
    is_custom: bool,
}

#[derive(Debug, Deserialize)]
struct StoredSequentialTable {
    table: String,
    fk_column: String,
    is_custom: bool,
}

pub struct GetImportLoggingDetailUsecase {
    logging_repo: Arc<dyn LoggingRepo + Send + Sync>,
    import_logging_repo: Arc<WarehouseLoggingImport>,
}

impl GetImportLoggingDetailUsecase {
    pub fn new(
        logging_repo: Arc<dyn LoggingRepo + Send + Sync>,
        import_logging_repo: Arc<WarehouseLoggingImport>,
    ) -> Self {
        Self {
            logging_repo,
            import_logging_repo,
        }
    }

    pub async fn execute(&self, logging_id: &str) -> Result<ImportLoggingDetail, AppError> {
        let logging = self
            .logging_repo
            .find_by_id(logging_id)
            .await
            .map_err(|error| AppError::InternalServerError(error.to_string()))?
            .ok_or_else(|| {
                AppError::NotFound(format!("logging '{}' tidak ditemukan", logging_id))
            })?;

        if !logging.logging_type.eq_ignore_ascii_case("IMPORT") {
            return Err(AppError::ValidationError(
                "detail ini hanya tersedia untuk logging_type IMPORT".to_string(),
            ));
        }

        let config = Self::import_config(&logging)?;
        let logging_id = Uuid::parse_str(logging_id).map_err(|error| {
            AppError::ValidationError(format!("logging_id '{}' bukan UUID: {error}", logging_id))
        })?;
        let entities = self
            .import_logging_repo
            .list_master_entity_details(logging_id, &config.master_table)
            .await
            .map_err(|error| AppError::InternalServerError(error.to_string()))?;
        let summary = Self::summarize_entities(&entities);

        let mut headers = config
            .fields
            .iter()
            .map(|field| ImportLoggingHeader {
                label: field.display_label(),
                key: field.key(),
            })
            .collect::<Vec<_>>();
        headers.extend(Self::state_headers());
        let sequential_table = config.sequential_table.as_deref().unwrap_or(&[]);
        let mut body = Vec::with_capacity(entities.len());

        for entity in entities {
            let mut sequential_rows = HashMap::new();

            for sequential in sequential_table {
                let rows = self
                    .import_logging_repo
                    .fetch_rows_by_uuid(&sequential.table, &sequential.fk_column, entity.entity_id)
                    .await
                    .map_err(|error| AppError::InternalServerError(error.to_string()))?;
                sequential_rows.insert(sequential.table.as_str(), rows);
            }

            let mut row = config
                .fields
                .iter()
                .map(|field| {
                    (
                        field.key(),
                        Self::resolve_field_value(
                            field,
                            &config.master_table,
                            &entity.entity,
                            sequential_table,
                            &sequential_rows,
                        ),
                    )
                })
                .collect::<Map<_, _>>();
            row.extend(Self::state_values(&entity));
            row.insert(
                "id".to_string(),
                entity
                    .entity
                    .get("id")
                    .cloned()
                    .unwrap_or_else(|| Value::String(entity.entity_id.to_string())),
            );

            body.push(row);
        }

        Ok(ImportLoggingDetail {
            total_processed: summary.total_processed,
            total_success: summary.total_success,
            total_failed: summary.total_failed,
            headers,
            content: body,
        })
    }

    fn summarize_entities(
        entities: &[crate::infrastructure::loader::warehouse::warehouse_logging_import::ImportMasterEntityDetail],
    ) -> ImportLoggingCounts {
        ImportLoggingCounts {
            total_processed: entities.len(),
            total_success: entities
                .iter()
                .filter(|entity| entity.sync_status == Some(crate::infrastructure::loader::warehouse::warehouse_logging_import::ImportSyncStatus::Sync))
                .count(),
            total_failed: entities
                .iter()
                .filter(|entity| {
                    entity.sync_status
                        == Some(crate::infrastructure::loader::warehouse::warehouse_logging_import::ImportSyncStatus::SyncFailed)
                        || entity.validation_status
                            == Some(crate::infrastructure::loader::warehouse::warehouse_logging_import::ImportValidationStatus::Invalid)
                })
                .count(),
        }
    }

    fn import_config(logging: &LoggingEntity) -> Result<StoredImportConfig, AppError> {
        let config = logging.config.as_ref().ok_or_else(|| {
            AppError::NotFound("konfigurasi import tidak tersedia pada logging".to_string())
        })?;
        let config = config.get("import_config").unwrap_or(config);

        serde_json::from_value(config.clone())
            .map_err(|error| AppError::ParsingError(format!("config import tidak valid: {error}")))
    }

    fn state_headers() -> [ImportLoggingHeader; 7] {
        [
            ImportLoggingHeader {
                label: "Validation Status".to_string(),
                key: "validation_status".to_string(),
            },
            ImportLoggingHeader {
                label: "Sync Status".to_string(),
                key: "sync_status".to_string(),
            },
            ImportLoggingHeader {
                label: "Error".to_string(),
                key: "error".to_string(),
            },
            ImportLoggingHeader {
                label: "Sync Error".to_string(),
                key: "sync_error".to_string(),
            },
            ImportLoggingHeader {
                label: "Sync At".to_string(),
                key: "sync_at".to_string(),
            },
            ImportLoggingHeader {
                label: "Validated At".to_string(),
                key: "validated_at".to_string(),
            },
            ImportLoggingHeader {
                label: "Metadata".to_string(),
                key: "metadata".to_string(),
            },
        ]
    }

    fn state_values(
        entity: &crate::infrastructure::loader::warehouse::warehouse_logging_import::ImportMasterEntityDetail,
    ) -> [(String, Value); 7] {
        [
            (
                "validation_status".to_string(),
                Self::json_or_null(entity.validation_status),
            ),
            (
                "sync_status".to_string(),
                Self::json_or_null(entity.sync_status),
            ),
            (
                "error".to_string(),
                entity.errors.clone().unwrap_or(Value::Null),
            ),
            (
                "sync_error".to_string(),
                entity
                    .sync_error
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Null),
            ),
            ("sync_at".to_string(), Self::json_or_null(entity.synced_at)),
            (
                "validated_at".to_string(),
                Self::json_or_null(entity.validated_at),
            ),
            (
                "metadata".to_string(),
                entity.metadata.clone().unwrap_or(Value::Null),
            ),
        ]
    }

    fn json_or_null<T: Serialize>(value: Option<T>) -> Value {
        value
            .and_then(|value| serde_json::to_value(value).ok())
            .unwrap_or(Value::Null)
    }

    fn resolve_field_value(
        field: &StoredImportField,
        entity_table: &str,
        main: &Value,
        sequential_config: &[StoredSequentialTable],
        sequential_rows: &HashMap<&str, Vec<Value>>,
    ) -> Value {
        if let Some((select, is_child)) = field.selected_path(entity_table) {
            return Self::resolve_selected_value(
                &select,
                is_child,
                field.is_custom,
                entity_table,
                main,
                sequential_rows,
            );
        }

        let paths = field
            .column
            .split('|')
            .filter_map(|path| path.trim().split_once(':'))
            .collect::<Vec<_>>();

        if paths.len() == 1 {
            let (table, column) = paths[0];
            return Self::resolve_path_value(
                table,
                column,
                field.is_custom,
                entity_table,
                main,
                sequential_config,
                sequential_rows,
            );
        }

        let mut values = Map::new();
        for (table, column) in paths {
            values.insert(
                format!("{table}:{column}"),
                Self::resolve_path_value(
                    table,
                    column,
                    field.is_custom,
                    entity_table,
                    main,
                    sequential_config,
                    sequential_rows,
                ),
            );
        }

        Value::Object(values)
    }

    fn resolve_selected_value(
        select: &str,
        is_child: bool,
        is_custom: bool,
        entity_table: &str,
        main: &Value,
        sequential_rows: &HashMap<&str, Vec<Value>>,
    ) -> Value {
        let Some((table, path)) = select.trim().split_once(':') else {
            return Value::Null;
        };

        if is_custom && table.ends_with("custom_field") {
            return Self::resolve_custom_selected_value(table, path, is_child, sequential_rows);
        }

        if !is_child && table == entity_table {
            return Self::resolve_json_path(main, path).unwrap_or(Value::Null);
        }

        let rows = sequential_rows
            .get(table)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let values = rows
            .iter()
            .filter_map(|row| Self::resolve_json_path(row, path))
            .collect::<Vec<_>>();

        if is_child {
            return Self::join_values(values);
        }

        Self::collapse_values(values)
    }

    fn resolve_custom_selected_value(
        table: &str,
        select_path: &str,
        is_child: bool,
        sequential_rows: &HashMap<&str, Vec<Value>>,
    ) -> Value {
        let mut parts = select_path.split(':').filter(|part| !part.is_empty());
        let Some(field_key) = parts.next() else {
            return Value::Null;
        };
        let selected_path = parts.collect::<Vec<_>>();
        let rows = sequential_rows
            .get(table)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let values = rows
            .iter()
            .filter(|row| {
                row.get("field_key")
                    .and_then(Value::as_str)
                    .is_some_and(|key| custom_field_keys_match(key, field_key))
            })
            .filter_map(|row| Self::resolve_custom_selected_path(row, &selected_path))
            .collect::<Vec<_>>();

        if is_child {
            Self::join_values(values)
        } else {
            Self::collapse_values(values)
        }
    }

    fn resolve_custom_selected_path(row: &Value, selected_path: &[&str]) -> Option<Value> {
        match selected_path {
            [] => custom_field_value(row).cloned(),
            ["value_text"] | ["value_number"] | ["value_id"] | ["value_bool"] | ["value_date"] => {
                Self::custom_value_column(row, selected_path[0])
            }
            ["meta", path @ ..] => {
                let meta = row.get("meta")?;
                if path.is_empty() {
                    Some(meta.clone())
                } else {
                    Self::resolve_json_path(meta, &path.join("."))
                }
            }
            [column, path @ ..] => {
                let value = row.get(*column)?;
                if path.is_empty() {
                    Some(value.clone())
                } else {
                    Self::resolve_json_path(value, &path.join("."))
                }
            }
        }
    }

    fn custom_value_column(row: &Value, column: &str) -> Option<Value> {
        let camel_case = match column {
            "value_text" => "valueText",
            "value_number" => "valueNumber",
            "value_id" => "valueId",
            "value_bool" => "valueBool",
            "value_date" => "valueDate",
            _ => return row.get(column).cloned(),
        };

        row.get(column)
            .or_else(|| row.get(camel_case))
            .filter(|value| !value.is_null())
            .cloned()
    }

    fn resolve_json_path(value: &Value, path: &str) -> Option<Value> {
        path.split('.')
            .filter(|key| !key.is_empty())
            .try_fold(value, |current, key| current.get(key))
            .cloned()
    }

    fn join_values(values: Vec<Value>) -> Value {
        let values = values
            .into_iter()
            .filter_map(|value| match value {
                Value::Null => None,
                Value::String(value) => Some(value),
                value => Some(value.to_string()),
            })
            .collect::<Vec<_>>();

        if values.is_empty() {
            Value::Null
        } else {
            Value::String(values.join(", "))
        }
    }

    fn resolve_path_value(
        table: &str,
        column: &str,
        field_is_custom: bool,
        entity_table: &str,
        main: &Value,
        sequential_config: &[StoredSequentialTable],
        sequential_rows: &HashMap<&str, Vec<Value>>,
    ) -> Value {
        if table == entity_table {
            return main.get(column).cloned().unwrap_or(Value::Null);
        }

        let Some(table_config) = sequential_config.iter().find(|item| item.table == table) else {
            return Value::Null;
        };
        let rows = sequential_rows
            .get(table)
            .map(Vec::as_slice)
            .unwrap_or_default();

        if field_is_custom || table_config.is_custom {
            let values = rows
                .iter()
                .filter(|row| {
                    row.get("field_key")
                        .and_then(Value::as_str)
                        .is_some_and(|field_key| custom_field_keys_match(field_key, column))
                })
                .filter_map(custom_field_value)
                .cloned()
                .collect::<Vec<_>>();
            return Self::collapse_values(values);
        }

        Self::collapse_values(
            rows.iter()
                .filter_map(|row| row.get(column).cloned())
                .collect(),
        )
    }

    fn collapse_values(mut values: Vec<Value>) -> Value {
        match values.len() {
            0 => Value::Null,
            1 => values.pop().unwrap_or(Value::Null),
            _ => Value::Array(values),
        }
    }
}

impl StoredImportField {
    fn selected_path(&self, master_table: &str) -> Option<(String, bool)> {
        if let Some(select) = self.select.as_ref() {
            return Some((select.clone(), self.is_child));
        }

        let field_key = self.fetcher_field_key.as_deref()?;
        let (table, column) = self
            .column
            .split('|')
            .filter_map(|path| path.trim().split_once(':'))
            .rev()
            .find(|(_, column)| column.ends_with("_meta"))?;

        Some((
            format!("{table}:{column}.{field_key}"),
            table != master_table,
        ))
    }

    fn display_label(&self) -> String {
        let header = self.header.trim().trim_end_matches('*').trim().to_string();

        if !header.contains('_') {
            return header;
        }

        header
            .split('_')
            .filter(|part| !part.is_empty())
            .map(|part| {
                let mut characters = part.chars();
                let Some(first) = characters.next() else {
                    return String::new();
                };
                format!("{}{}", first.to_ascii_uppercase(), characters.as_str())
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn key(&self) -> String {
        let mut key = String::new();
        let mut previous_is_lowercase = false;
        let mut previous_is_separator = true;

        for character in self.display_label().chars() {
            if character.is_ascii_alphanumeric() {
                if character.is_ascii_uppercase() && previous_is_lowercase && !previous_is_separator
                {
                    key.push('_');
                }
                key.push(character.to_ascii_lowercase());
                previous_is_lowercase = character.is_ascii_lowercase();
                previous_is_separator = false;
            } else if !previous_is_separator {
                key.push('_');
                previous_is_lowercase = false;
                previous_is_separator = true;
            }
        }

        key.trim_matches('_').to_string()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::json;

    use super::{GetImportLoggingDetailUsecase, StoredImportField, StoredSequentialTable};

    #[test]
    fn uses_normalized_header_as_body_key() {
        let field = StoredImportField {
            header: "No RR*".to_string(),
            column: "warehouse.ats_vacancy_custom_field:noRR".to_string(),
            select: None,
            is_child: false,
            fetcher_field_key: None,
            is_custom: true,
        };

        assert_eq!(field.display_label(), "No RR");
        assert_eq!(field.key(), "no_rr");
    }

    #[test]
    fn resolves_custom_field_to_its_configured_header() {
        let field = StoredImportField {
            header: "No RR*".to_string(),
            column: "warehouse.ats_vacancy_custom_field:noRR".to_string(),
            select: None,
            is_child: false,
            fetcher_field_key: None,
            is_custom: true,
        };
        let sequential = vec![StoredSequentialTable {
            table: "warehouse.ats_vacancy_custom_field".to_string(),
            fk_column: "parent_id".to_string(),
            is_custom: true,
        }];
        let mut rows = HashMap::new();
        rows.insert(
            "warehouse.ats_vacancy_custom_field",
            vec![json!({
                "field_key": "noRR",
                "field_type": "TEXT",
                "value_text": "RR-001"
            })],
        );

        let value = GetImportLoggingDetailUsecase::resolve_field_value(
            &field,
            "warehouse.ats_vacancy_temporary",
            &json!({"id": "vacancy-id"}),
            &sequential,
            &rows,
        );

        assert_eq!(value, json!("RR-001"));
    }

    #[test]
    fn joins_selected_child_values() {
        let mut rows = HashMap::new();
        rows.insert(
            "warehouse.ats_vacancy_pic",
            vec![
                json!({"employee_meta": {"name": "verrandy"}}),
                json!({"employee_meta": {"name": "ridho"}}),
            ],
        );

        let value = GetImportLoggingDetailUsecase::resolve_selected_value(
            "warehouse.ats_vacancy_pic:employee_meta.name",
            true,
            false,
            "warehouse.ats_vacancy_temporary",
            &json!({}),
            &rows,
        );

        assert_eq!(value, json!("verrandy, ridho"));
    }

    #[test]
    fn resolves_selected_master_value() {
        let value = GetImportLoggingDetailUsecase::resolve_selected_value(
            "warehouse.ats_vacancy_temporary:position_meta.name",
            false,
            false,
            "warehouse.ats_vacancy_temporary",
            &json!({"position_meta": {"name": "bagus"}}),
            &HashMap::new(),
        );

        assert_eq!(value, json!("bagus"));
    }

    #[test]
    fn derives_select_and_child_flag_from_metadata_column() {
        let field = StoredImportField {
            header: "PIC*".to_string(),
            column: "warehouse.ats_vacancy_pic:employee_id|warehouse.ats_vacancy_pic:employee_meta"
                .to_string(),
            select: None,
            is_child: false,
            fetcher_field_key: Some("name".to_string()),
            is_custom: false,
        };

        assert_eq!(
            field.selected_path("warehouse.ats_vacancy_temporary"),
            Some((
                "warehouse.ats_vacancy_pic:employee_meta.name".to_string(),
                true
            ))
        );
    }

    #[test]
    fn resolves_selected_custom_meta_or_value_column() {
        let mut rows = HashMap::new();
        rows.insert(
            "warehouse.ats_vacancy_custom_field",
            vec![json!({
                "field_key": "workLocationGroupId",
                "field_type": "UUID",
                "value_text": "fallback",
                "meta": {"name": "bagus"}
            })],
        );

        let meta = GetImportLoggingDetailUsecase::resolve_selected_value(
            "warehouse.ats_vacancy_custom_field:workLocationGroupId:meta:name",
            true,
            true,
            "warehouse.ats_vacancy_temporary",
            &json!({}),
            &rows,
        );
        let value_text = GetImportLoggingDetailUsecase::resolve_selected_value(
            "warehouse.ats_vacancy_custom_field:workLocationGroupId:value_text",
            true,
            true,
            "warehouse.ats_vacancy_temporary",
            &json!({}),
            &rows,
        );

        assert_eq!(meta, json!("bagus"));
        assert_eq!(value_text, json!("fallback"));
    }
}
