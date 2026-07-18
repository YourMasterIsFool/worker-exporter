use std::{collections::HashMap, sync::Arc};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::{
    domain::logging::{entity::LoggingEntity, repo::LoggingRepo},
    errors::AppError,
    infrastructure::loader::warehouse::warehouse_logging_import::WarehouseLoggingImport,
    utils::custom_field_value,
};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ImportLoggingDetail {
    /// Display labels from `import_config.fields[].header`.
    pub headers: Vec<ImportLoggingHeader>,
    /// Imported rows keyed by the corresponding `headers[].key`.
    pub body: Vec<Map<String, Value>>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ImportLoggingHeader {
    pub label: String,
    pub key: String,
}

#[derive(Debug, Deserialize)]
struct StoredImportConfig {
    fields: Vec<StoredImportField>,
    master_table: String,
    #[serde(default)]
    sequential_table: Option<Vec<StoredSequentialTable>>,
}

#[derive(Debug, Deserialize)]
struct StoredImportField {
    header: String,
    column: String,
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

            body.push(row);
        }

        Ok(ImportLoggingDetail { headers, body })
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
                    entity_table,
                    main,
                    sequential_config,
                    sequential_rows,
                ),
            );
        }

        Value::Object(values)
    }

    fn resolve_path_value(
        table: &str,
        column: &str,
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

        if table_config.is_custom {
            let values = rows
                .iter()
                .filter(|row| row.get("field_key").and_then(Value::as_str) == Some(column))
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
        };

        assert_eq!(field.display_label(), "No RR");
        assert_eq!(field.key(), "no_rr");
    }

    #[test]
    fn resolves_custom_field_to_its_configured_header() {
        let field = StoredImportField {
            header: "No RR*".to_string(),
            column: "warehouse.ats_vacancy_custom_field:noRR".to_string(),
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
}
