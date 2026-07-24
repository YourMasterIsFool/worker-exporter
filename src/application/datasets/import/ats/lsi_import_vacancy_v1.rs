use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use calamine::Data;
use moka::future::Cache;
use serde_json::{Map, Value};
use tracing::info;
use uuid::Uuid;

use crate::application::datasets::import::dynamci_dataset_v1::DynamicDatasetV1;
use crate::{
    application::{
        fetch_source_regitry::DataFetcherJsonRegistry,
        validations::{
            prepare_validations, validate_value, PreparedValidation, ValidationRegistry,
        },
    },
    core::{
        traits::orchestrator::SequentialTable,
        traits::{dataset_trait::ImporterDatasetV1, orchestrator::Filterable},
        types::{
            CastingEnum, ImportDatasetConfig, ImportTableResult, TableStrategy, ValidationConfig,
            IMPORT_ENTITY_STATE_RESULT_KEY, IMPORT_FETCHER_ERROR_RESULT_KEY,
        },
    },
    utils::{
        custom_field_keys_match, first_object_value, flatten_values, resolved_custom_field_value,
        uuid_values_filter, value::calamine_to_json, value_as_keyword_list, values_at_path,
    },
};

pub struct LsiImportVacancyV1 {
    fetcher: Arc<DataFetcherJsonRegistry>,
    dynamic_dataset: DynamicDatasetV1,
    validations: Arc<ValidationRegistry>,
}

struct PreparedFieldValidation {
    field_index: usize,
    header: String,
    validations: Vec<PreparedValidation>,
}

impl LsiImportVacancyV1 {
    fn parse_table_column_path(path: &str) -> Option<(&str, &str)> {
        path.trim().split_once(':')
    }

    fn stage_detail_value(detail: &Value, keys: &[&str]) -> Option<Value> {
        first_object_value(detail.get("recruitment_stage_meta"), keys)
            .or_else(|| first_object_value(detail.get("stage_snapshot"), keys))
    }

    fn is_stage_template_group(group: &str) -> bool {
        let Some((table, column)) = group.trim().split_once(':') else {
            return false;
        };

        table.ends_with("vacancy_stage") && column == "requirement_stage_template_id"
    }

    fn raw_row_string_value(row: &Data) -> Option<String> {
        match calamine_to_json(row) {
            Value::String(value) if !value.trim().is_empty() => Some(value.trim().to_string()),
            Value::Null => None,
            other => {
                let text = other.to_string();
                let trimmed = text.trim().trim_matches('"').to_string();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed)
                }
            }
        }
    }

    fn field_index_by_column_name(
        config: &ImportDatasetConfig,
        target_column: &str,
    ) -> Option<usize> {
        config
            .import_config
            .fields
            .iter()
            .enumerate()
            .find_map(|(field_index, field)| {
                field
                    .column
                    .split('|')
                    .filter_map(Self::parse_table_column_path)
                    .find(|(_, column)| *column == target_column)
                    .map(|_| field_index)
            })
    }

    fn resolve_sequential_table(
        config: &ImportDatasetConfig,
        requested_table: &str,
    ) -> Option<String> {
        if let Some(table) = config
            .import_config
            .sequential_table
            .as_ref()
            .and_then(|tables| tables.iter().find(|seq| seq.table == requested_table))
            .map(|seq| seq.table.clone())
        {
            return Some(table);
        }

        let requested_suffix = requested_table.rsplit('.').next()?;

        config
            .import_config
            .sequential_table
            .as_ref()
            .and_then(|tables| {
                tables
                    .iter()
                    .find(|seq| seq.table.rsplit('.').next() == Some(requested_suffix))
            })
            .map(|seq| seq.table.clone())
    }

    fn table_matches_suffix(table: &str, suffixes: &[&str]) -> bool {
        let trimmed = table.trim();
        let tail = trimmed.rsplit('.').next().unwrap_or(trimmed);

        suffixes
            .iter()
            .any(|suffix| trimmed == *suffix || tail == *suffix || trimmed.ends_with(suffix))
    }

    fn resolve_sequential_table_by_suffixes(
        config: &ImportDatasetConfig,
        suffixes: &[&str],
    ) -> Option<String> {
        config
            .import_config
            .sequential_table
            .as_ref()
            .and_then(|tables| {
                tables
                    .iter()
                    .find(|seq| Self::table_matches_suffix(&seq.table, suffixes))
            })
            .map(|seq| seq.table.clone())
    }

    fn resolve_or_default_table(
        config: &ImportDatasetConfig,
        suffixes: &[&str],
        default_table: &str,
    ) -> String {
        Self::resolve_sequential_table_by_suffixes(config, suffixes)
            .unwrap_or_else(|| default_table.to_string())
    }

    fn normalize_uuid_string(value: &str) -> Option<String> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return None;
        }

        Uuid::parse_str(trimmed).ok().map(|uuid| uuid.to_string())
    }

    pub fn new(
        fetcher: Arc<DataFetcherJsonRegistry>,
        validations: Arc<ValidationRegistry>,
    ) -> Self {
        Self {
            fetcher: fetcher.clone(),
            dynamic_dataset: DynamicDatasetV1::new(fetcher),
            validations,
        }
    }

    async fn prepare_field_validations(
        &self,
        rows: &[&[Data]],
        config: &ImportDatasetConfig,
    ) -> anyhow::Result<Vec<PreparedFieldValidation>> {
        let mut prepared = Vec::new();

        for (field_index, field) in config.import_config.fields.iter().enumerate() {
            let mut rules = field
                .validation_rules
                .as_ref()
                .filter(|rules| !rules.is_empty())
                .cloned()
                .unwrap_or_default();

            // The generated Excel template uses a trailing `*` as the
            // required-field marker.  Respect it even when the payload has
            // not explicitly included a `validations: [{ validator:
            // "required" }]` entry for that field.
            let required_by_header = field.header.trim_end().ends_with('*');
            let already_required = rules
                .iter()
                .any(|rule| rule.validator.eq_ignore_ascii_case("required"));
            if required_by_header && !already_required {
                rules.push(ValidationConfig {
                    validator: "required".to_string(),
                    options: Value::Object(Map::new()),
                });
            }

            if rules.is_empty() {
                continue;
            }

            let values = rows
                .iter()
                .map(|row| {
                    row.get(field_index)
                        .map(calamine_to_json)
                        .unwrap_or(Value::Null)
                })
                .collect::<Vec<_>>();

            prepared.push(PreparedFieldValidation {
                field_index,
                header: field.header.clone(),
                validations: prepare_validations(&self.validations, &values, &rules).await?,
            });
        }

        Ok(prepared)
    }

    fn validate_row(rows: &[Data], validations: &mut [PreparedFieldValidation]) -> Value {
        let mut errors = Map::new();

        for field in validations {
            let value = rows
                .get(field.field_index)
                .map(calamine_to_json)
                .unwrap_or(Value::Null);
            let field_errors = validate_value(&value, &mut field.validations)
                .into_iter()
                .map(|error| error.to_string())
                .collect::<Vec<_>>();

            if !field_errors.is_empty() {
                tracing::warn!(
                    header = %field.header,
                    value = ?value,
                    errors = ?field_errors,
                    "Import validation gagal"
                );
                errors.insert(
                    field.header.clone(),
                    Value::Array(field_errors.into_iter().map(Value::String).collect()),
                );
            }
        }

        Value::Object(errors)
    }

    fn field_is_required(field: &crate::core::traits::orchestrator::ImporterColumn) -> bool {
        field.header.trim_end().ends_with('*')
            || field.validation_rules.as_ref().is_some_and(|rules| {
                rules
                    .iter()
                    .any(|rule| rule.validator.eq_ignore_ascii_case("required"))
            })
    }

    fn is_missing_required_value(value: Option<Value>) -> bool {
        match value {
            None | Some(Value::Null) => true,
            Some(Value::String(value)) => value.trim().is_empty(),
            Some(Value::Array(values)) => values.is_empty(),
            Some(_) => false,
        }
    }

    fn append_field_error(errors: &mut Map<String, Value>, header: &str, error: &str) {
        let error = Value::String(error.to_string());
        let entry = errors
            .entry(header.to_string())
            .or_insert_with(|| Value::Array(Vec::new()));

        if let Value::Array(field_errors) = entry {
            if !field_errors.contains(&error) {
                field_errors.push(error);
            }
        }
    }

    /// Required input is checked independently from validator state. This is
    /// important for trailing empty Excel cells: spreadsheet readers may omit
    /// those cells entirely, but they must still produce one error per
    /// required configured header.
    fn validate_required_input_fields(
        rows: &[Data],
        config: &ImportDatasetConfig,
        errors: &mut Value,
    ) {
        let Value::Object(error_map) = errors else {
            return;
        };

        for (field_index, field) in config.import_config.fields.iter().enumerate() {
            if !Self::field_is_required(field) {
                continue;
            }

            let value = rows.get(field_index).map(calamine_to_json);
            if !Self::is_missing_required_value(value) {
                continue;
            }

            Self::append_field_error(error_map, &field.header, "Required");
            tracing::warn!(
                header = %field.header,
                field_index,
                "Import required validation gagal karena cell Excel kosong"
            );
        }
    }

    /// `required` initially validates the Excel cell.  For lookup fields that
    /// is not sufficient: a non-empty label can still fail to resolve through
    /// its fetcher.  Validate the resolved ID/value as well so the logging
    /// state reflects data that can actually be inserted and synchronized.
    fn validate_resolved_required_fields(
        &self,
        rows: &[Data],
        result: &HashMap<String, ImportTableResult>,
        config: &ImportDatasetConfig,
        errors: &mut Value,
    ) {
        let Value::Object(error_map) = errors else {
            return;
        };

        for (field_index, field) in config.import_config.fields.iter().enumerate() {
            if field.fetcher.is_none() || !Self::field_is_required(field) {
                continue;
            }

            let resolved_value = field
                .column
                .split('|')
                .filter_map(Self::parse_table_column_path)
                .filter(|(_, column)| !column.ends_with("_meta"))
                .find_map(|(table, column)| self.extract_value_by_path(result, table, column));

            if !Self::is_missing_required_value(resolved_value) {
                continue;
            }

            if let Some(raw_value) = rows.get(field_index).and_then(Self::raw_row_string_value) {
                Self::append_field_error(
                    error_map,
                    &field.header,
                    &format!(
                        "Data fetcher '{}' tidak menemukan nilai '{}'",
                        field.fetcher.as_deref().unwrap_or_default(),
                        raw_value
                    ),
                );
            }

            Self::append_field_error(
                error_map,
                &field.header,
                "Required: data fetcher tidak ditemukan",
            );

            tracing::warn!(
                header = %field.header,
                column = %field.column,
                fetcher = ?field.fetcher,
                "Import required validation gagal karena hasil fetcher NULL"
            );
        }
    }

    fn attach_validation_errors(result: &mut HashMap<String, ImportTableResult>, errors: Value) {
        result.insert(
            IMPORT_ENTITY_STATE_RESULT_KEY.to_string(),
            ImportTableResult {
                // The record is metadata for the loader, not a warehouse table.
                // EAV prevents the orchestrator from adding a `logging_id` column to it.
                strategy: TableStrategy::Eav,
                columns: vec!["errors".to_string()],
                values: vec![vec![errors]],
            },
        );
    }

    fn merge_fetcher_errors(result: &mut HashMap<String, ImportTableResult>, errors: &mut Value) {
        let Some(fetcher_errors) = result.remove(IMPORT_FETCHER_ERROR_RESULT_KEY) else {
            return;
        };
        let Some(error_index) = fetcher_errors
            .columns
            .iter()
            .position(|column| column == "errors")
        else {
            return;
        };
        let Some(Value::Object(fetcher_error_map)) = fetcher_errors
            .values
            .first()
            .and_then(|values| values.get(error_index))
        else {
            return;
        };
        let Value::Object(error_map) = errors else {
            return;
        };

        for (header, values) in fetcher_error_map {
            let Value::Array(values) = values else {
                continue;
            };
            for value in values {
                if let Some(message) = value.as_str() {
                    Self::append_field_error(error_map, header, message);
                }
            }
        }
    }

    fn build_base_config(&self, config: &ImportDatasetConfig) -> ImportDatasetConfig {
        let mut cloned = config.clone();

        // `ImporterColumn::is_custom` is the field-level source of truth.  The
        // dynamic dataset routes EAV data based on `sequential_table`, so make
        // the two representations consistent before processing the row.  This
        // also keeps an incomplete payload from silently treating custom
        // fields as a normal has-many table (or dropping them entirely).
        let custom_tables = cloned
            .import_config
            .fields
            .iter()
            .filter(|field| field.is_custom)
            .flat_map(|field| field.column.split('|'))
            .filter_map(Self::parse_table_column_path)
            .map(|(table, _)| table.trim().to_string())
            .collect::<HashSet<_>>();

        let sequential_tables = cloned
            .import_config
            .sequential_table
            .get_or_insert_with(Vec::new);
        for custom_table in custom_tables {
            if custom_table == cloned.import_config.master_table {
                continue;
            }

            if let Some(sequential_table) = sequential_tables
                .iter_mut()
                .find(|sequential_table| sequential_table.table == custom_table)
            {
                sequential_table.is_custom = true;
            } else {
                sequential_tables.push(SequentialTable {
                    table: custom_table,
                    is_custom: true,
                    fk_column: "parent_id".to_string(),
                });
            }
        }

        let master_table = cloned.import_config.master_table.clone();

        cloned.import_config.fields = cloned
            .import_config
            .fields
            .into_iter()
            .map(|mut field| {
                // A custom field unique validator has to look up the EAV
                // table by `field_key`.  This keeps legacy payloads concise:
                // field-level `is_custom` is enough to enable that behavior.
                if field.is_custom {
                    if let Some(validation_rules) = field.validation_rules.as_mut() {
                        for rule in validation_rules
                            .iter_mut()
                            .filter(|rule| rule.validator.eq_ignore_ascii_case("unique"))
                        {
                            if let Value::Object(options) = &mut rule.options {
                                options
                                    .entry("is_custom".to_string())
                                    .or_insert(Value::Bool(true));
                                if options
                                    .get("fetcher")
                                    .and_then(Value::as_str)
                                    .is_some_and(|fetcher| fetcher.eq_ignore_ascii_case("VACANCY"))
                                {
                                    options.insert(
                                        "fetcher".to_string(),
                                        Value::String("VACANCY_CUSTOM_FIELD".to_string()),
                                    );
                                }
                            }
                        }
                    }
                }

                if field.fetcher.is_some() {
                    let fetcher_field_key = field
                        .fetcher_field_key
                        .as_deref()
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .unwrap_or("id")
                        .to_string();

                    field.fetcher_field_key = Some(fetcher_field_key.clone());
                    field.fetcher_key = Some(
                        field
                            .fetcher_key
                            .as_deref()
                            .map(str::trim)
                            .filter(|value| !value.is_empty())
                            .unwrap_or("id")
                            .to_string(),
                    );
                    field.casting = field.casting.or_else(|| {
                        if fetcher_field_key.eq_ignore_ascii_case("id") {
                            Some(CastingEnum::Uuid)
                        } else {
                            Some(CastingEnum::Varchar)
                        }
                    });
                }

                let groups = field
                    .column
                    .split('|')
                    .filter_map(|group| {
                        let trimmed = group.trim();
                        if trimmed.ends_with(":*") {
                            return None;
                        }

                        // A stage template itself is a lookup value stored on
                        // the master temporary row. `append_stage_rows` then
                        // expands it into its stage-detail child rows.
                        if Self::is_stage_template_group(trimmed) {
                            return Some(format!("{}:requirement_stage_template_id", master_table));
                        }

                        Some(trimmed.to_string())
                    })
                    .collect::<Vec<_>>();

                field.column = groups.join("|");
                field
            })
            .collect();
        cloned
    }

    fn normalize_number_value(value: Value) -> anyhow::Result<Value> {
        match value {
            Value::String(text) => {
                let text = text.trim();
                if text.is_empty() {
                    return Ok(Value::Null);
                }

                if let Ok(integer) = text.parse::<i64>() {
                    return Ok(Value::from(integer));
                }

                if let Ok(decimal) = text.parse::<f64>() {
                    return serde_json::Number::from_f64(decimal)
                        .map(Value::Number)
                        .ok_or_else(|| anyhow::anyhow!("nilai NUMBER '{}' tidak valid", text));
                }

                Err(anyhow::anyhow!("nilai NUMBER '{}' tidak valid", text))
            }
            Value::Array(values) => values
                .into_iter()
                .map(Self::normalize_number_value)
                .collect::<anyhow::Result<Vec<_>>>()
                .map(Value::Array),
            value => Ok(value),
        }
    }

    fn normalize_numeric_columns(
        &self,
        config: &ImportDatasetConfig,
        result: &mut HashMap<String, ImportTableResult>,
    ) -> anyhow::Result<()> {
        for field in &config.import_config.fields {
            if !matches!(
                field.type_data,
                Some(crate::core::traits::orchestrator::TypeDataEnum::Number)
            ) {
                continue;
            }

            for group in field.column.split('|') {
                let Some((table, column)) = Self::parse_table_column_path(group) else {
                    continue;
                };

                let Some(table_result) = result.get_mut(table) else {
                    continue;
                };

                let Some(column_index) = table_result
                    .columns
                    .iter()
                    .position(|value| value == column)
                else {
                    continue;
                };

                for row in &mut table_result.values {
                    if let Some(value) = row.get_mut(column_index) {
                        *value = Self::normalize_number_value(std::mem::take(value)).map_err(
                            |error| {
                                anyhow::anyhow!(
                                    "field '{}' (column '{}'): {}",
                                    field.header,
                                    column,
                                    error
                                )
                            },
                        )?;
                    }
                }
            }
        }

        Ok(())
    }

    fn extract_master_id(
        &self,
        result: &HashMap<String, ImportTableResult>,
        master_table: &str,
        primary_column: &str,
    ) -> anyhow::Result<String> {
        let master = result
            .get(master_table)
            .ok_or_else(|| anyhow::anyhow!("master table '{}' tidak ditemukan", master_table))?;

        let row = master
            .values
            .first()
            .ok_or_else(|| anyhow::anyhow!("master row kosong"))?;

        let id_index = master
            .columns
            .iter()
            .position(|col| col == primary_column)
            .ok_or_else(|| {
                anyhow::anyhow!("primary column '{}' tidak ditemukan", primary_column)
            })?;

        row.get(id_index)
            .and_then(|value| value.as_str())
            .map(|value| value.to_string())
            .ok_or_else(|| anyhow::anyhow!("nilai primary column '{}' tidak valid", primary_column))
    }

    fn extract_master_string_value(
        &self,
        result: &HashMap<String, ImportTableResult>,
        master_table: &str,
        column: &str,
    ) -> anyhow::Result<String> {
        let master = result
            .get(master_table)
            .ok_or_else(|| anyhow::anyhow!("master table '{}' tidak ditemukan", master_table))?;

        let row = master
            .values
            .first()
            .ok_or_else(|| anyhow::anyhow!("master row kosong"))?;

        let column_index = master
            .columns
            .iter()
            .position(|col| col == column)
            .ok_or_else(|| anyhow::anyhow!("column '{}' tidak ditemukan", column))?;

        row.get(column_index)
            .and_then(|value| value.as_str())
            .map(|value| value.to_string())
            .ok_or_else(|| anyhow::anyhow!("nilai column '{}' tidak valid", column))
    }

    fn extract_template_id_from_rows(
        &self,
        rows: &[Data],
        config: &ImportDatasetConfig,
        target_column: &str,
    ) -> Option<String> {
        let field_index = Self::field_index_by_column_name(config, target_column)?;
        Self::raw_row_string_value(rows.get(field_index)?)
    }

    fn extract_master_value(
        &self,
        result: &HashMap<String, ImportTableResult>,
        master_table: &str,
        column: &str,
    ) -> Option<Value> {
        let master = result.get(master_table)?;
        let row = master.values.first()?;
        let column_index = master.columns.iter().position(|col| col == column)?;
        row.get(column_index).cloned()
    }

    fn extract_value_by_path(
        &self,
        result: &HashMap<String, ImportTableResult>,
        table: &str,
        column: &str,
    ) -> Option<Value> {
        let table_result = result.get(table)?;

        match table_result.strategy {
            TableStrategy::Main => {
                let row = table_result.values.first()?;
                let column_index = table_result.columns.iter().position(|col| col == column)?;
                row.get(column_index).cloned()
            }
            TableStrategy::HasMany => {
                let column_index = table_result.columns.iter().position(|col| col == column)?;
                let mut collected = table_result
                    .values
                    .iter()
                    .filter_map(|row| row.get(column_index).cloned())
                    .filter(|value| !value.is_null())
                    .collect::<Vec<_>>();

                match collected.len() {
                    0 => None,
                    1 => collected.pop(),
                    _ => Some(Value::Array(collected)),
                }
            }
            TableStrategy::Eav => {
                let value_index = table_result.columns.iter().position(|col| col == "value")?;

                for row in &table_result.values {
                    let custom_value = row.get(value_index)?;
                    let Value::Object(custom_map) = custom_value else {
                        continue;
                    };

                    let Some(field_key) =
                        custom_map.get("field_key").and_then(|value| value.as_str())
                    else {
                        continue;
                    };

                    if custom_field_keys_match(field_key, column) {
                        return resolved_custom_field_value(custom_value).cloned();
                    }
                }

                None
            }
        }
    }

    fn configured_custom_field_value(value: &Value, value_column: Option<&str>) -> Option<Value> {
        let value_column = value_column
            .map(str::trim)
            .filter(|column| !column.is_empty());

        if let Some(value_column) = value_column {
            return value
                .get(value_column)
                .filter(|value| !value.is_null())
                .cloned();
        }

        resolved_custom_field_value(value).cloned()
    }

    fn filterable_value(
        value: Value,
        source_column: Option<&str>,
        field_key: Option<&str>,
        value_column: Option<&str>,
        is_array: bool,
    ) -> Option<Value> {
        let values = values_at_path(value, source_column);
        let field_key = field_key.map(str::trim).filter(|key| !key.is_empty());

        let values = if let Some(field_key) = field_key {
            flatten_values(values)
                .into_iter()
                .filter_map(|value| {
                    value
                        .get("field_key")
                        .and_then(Value::as_str)
                        .filter(|candidate| custom_field_keys_match(candidate, field_key))
                        .and_then(|_| Self::configured_custom_field_value(&value, value_column))
                })
                .collect::<Vec<_>>()
        } else {
            values
        };

        let values = values
            .into_iter()
            .filter(|value| !value.is_null())
            .collect::<Vec<_>>();
        if values.is_empty() {
            return None;
        }

        if is_array {
            if let [Value::Array(items)] = values.as_slice() {
                return Some(Value::Array(items.clone()));
            }
            Some(Value::Array(values))
        } else if values.len() == 1 {
            values.into_iter().next()
        } else {
            Some(Value::Array(values))
        }
    }

    fn upsert_main_value(
        &self,
        result: &mut HashMap<String, ImportTableResult>,
        master_table: &str,
        column: &str,
        value: Value,
    ) -> anyhow::Result<()> {
        let master = result
            .get_mut(master_table)
            .ok_or_else(|| anyhow::anyhow!("master table '{}' tidak ditemukan", master_table))?;

        let row = master
            .values
            .first_mut()
            .ok_or_else(|| anyhow::anyhow!("master row kosong"))?;

        if let Some(column_index) = master.columns.iter().position(|col| col == column) {
            if let Some(slot) = row.get_mut(column_index) {
                *slot = value;
            }
        } else {
            master.columns.push(column.to_string());
            row.push(value);
        }

        Ok(())
    }

    fn insert_optional_value(row: &mut HashMap<String, Value>, column: &str, value: Option<Value>) {
        if let Some(value) = value.filter(|value| !value.is_null()) {
            row.insert(column.to_string(), value);
        }
    }

    fn append_config_scope_columns(
        &self,
        config: &ImportDatasetConfig,
        result: &mut HashMap<String, ImportTableResult>,
    ) -> anyhow::Result<()> {
        let master_table = config.import_config.master_table.as_str();

        self.upsert_main_value(
            result,
            master_table,
            "tenant_id",
            Value::String(config.import_config.tenant_id.clone()),
        )?;
        self.upsert_main_value(
            result,
            master_table,
            "group_id",
            Value::String(config.import_config.group_id.clone()),
        )?;

        Ok(())
    }

    fn insert_has_many_result(
        &self,
        result: &mut HashMap<String, ImportTableResult>,
        table: String,
        rows: Vec<HashMap<String, Value>>,
    ) {
        if rows.is_empty() {
            return;
        }

        let mut columns: Vec<String> = rows
            .iter()
            .flat_map(|row| row.keys().cloned())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        columns.sort();

        let values = rows
            .into_iter()
            .map(|row| {
                columns
                    .iter()
                    .map(|column| row.get(column).cloned().unwrap_or(Value::Null))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        result.insert(
            table,
            ImportTableResult {
                strategy: TableStrategy::HasMany,
                columns,
                values,
            },
        );
    }

    fn append_publish_on_rows(
        &self,
        config: &ImportDatasetConfig,
        result: &mut HashMap<String, ImportTableResult>,
    ) -> anyhow::Result<()> {
        let master_table = &config.import_config.master_table;
        let vacancy_id =
            self.extract_master_id(result, master_table, &config.import_config.primary_column)?;
        let Some(status) = self
            .extract_master_value(result, master_table, "status")
            .and_then(|value| value.as_str().map(str::trim).map(str::to_uppercase))
            .filter(|status| !status.is_empty())
        else {
            return Ok(());
        };

        let tenant_id = config.import_config.tenant_id.trim();
        if tenant_id.is_empty() {
            return Ok(());
        }

        // Vacancy selalu aktif pada careersite tenant yang menjalankan import.
        self.upsert_main_value(
            result,
            master_table,
            "active_publish_keys",
            Value::Array(vec![Value::String(format!("CAREERSITE:{tenant_id}"))]),
        )?;

        let target_table = Self::resolve_or_default_table(
            config,
            &["vacancy_publish_on", "ats_vacancy_publish_on"],
            "warehouse.vacancy_publish_on",
        );
        let row = HashMap::from([
            ("id".to_string(), Value::String(Uuid::now_v7().to_string())),
            ("vacancy_id".to_string(), Value::String(vacancy_id)),
            (
                "platform".to_string(),
                Value::String("CAREERSITE".to_string()),
            ),
            ("status".to_string(), Value::String(status)),
            (
                "careersite_tenant_id".to_string(),
                Value::String(tenant_id.to_string()),
            ),
        ]);

        self.insert_has_many_result(result, target_table, vec![row]);
        Ok(())
    }

    fn append_main_payload_columns(
        &self,
        config: &ImportDatasetConfig,
        result: &mut HashMap<String, ImportTableResult>,
    ) -> anyhow::Result<()> {
        let master_table = config.import_config.master_table.as_str();

        if let Some(main_filterable) = &config.import_config.main_filterable {
            let mut filterable_attribute = Map::new();

            for Filterable {
                main_column,
                import_column,
                is_array,
                source_column,
                field_key,
                value_column,
            } in main_filterable
            {
                let Some((table, column)) = Self::parse_table_column_path(main_column) else {
                    continue;
                };

                if let Some(value) = self.extract_value_by_path(result, table, column) {
                    if let Some(value) = Self::filterable_value(
                        value,
                        source_column.as_deref(),
                        field_key.as_deref(),
                        value_column.as_deref(),
                        *is_array,
                    ) {
                        filterable_attribute.insert(import_column.to_string(), value);
                    }
                }
            }

            if !filterable_attribute.is_empty() {
                self.upsert_main_value(
                    result,
                    master_table,
                    "filterable_attribute",
                    Value::Object(filterable_attribute),
                )?;
            }
        }

        if let Some(main_keywords) = &config.import_config.main_searchable {
            let mut keywords = vec![];

            for path in main_keywords {
                let Some((table, column)) = Self::parse_table_column_path(path) else {
                    continue;
                };

                if let Some(value) = self.extract_value_by_path(result, table, column) {
                    keywords.extend(value_as_keyword_list(&value));
                }
            }

            keywords.sort();
            keywords.dedup();

            if !keywords.is_empty() {
                // Cukup kirim sebagai Array JSON murni.
                // Jangan gunakan format string "ARRAY[...]".
                let keyword_value = Value::Array(keywords.into_iter().map(Value::String).collect());

                self.upsert_main_value(result, master_table, "keywords", keyword_value)?;
            }
        }

        if let Some(permission_key) = &config.import_config.permission_key {
            // mapping data to value array because data is serde_json::value

            let mapping_to_value_array = Value::Array(
                permission_key
                    .iter()
                    .map(|f| Value::String(f.to_string()))
                    .collect(),
            );

            // insert permission_key
            self.upsert_main_value(
                result,
                master_table,
                "permission_keys",
                mapping_to_value_array,
            );
        }

        Ok(())
    }

    async fn append_requirement_rows(
        &self,
        rows: &[Data],
        config: &ImportDatasetConfig,
        result: &mut HashMap<String, ImportTableResult>,
    ) -> anyhow::Result<()> {
        let target_table = Self::resolve_or_default_table(
            config,
            &["ats_vacancy_requirement", "vacancy_requirement"],
            "warehouse.ats_vacancy_requirement",
        );

        let template_id = self
            .extract_master_string_value(
                result,
                &config.import_config.master_table,
                "recruitment_form_template_id",
            )
            .ok()
            .and_then(|value| Self::normalize_uuid_string(&value))
            .or_else(|| {
                self.extract_template_id_from_rows(rows, config, "recruitment_form_template_id")
                    .and_then(|value| Self::normalize_uuid_string(&value))
            });

        let Some(template_id) = template_id else {
            return Ok(());
        };

        self.upsert_main_value(
            result,
            &config.import_config.master_table,
            "recruitment_form_template_id",
            Value::String(template_id.clone()),
        )?;

        let template_detail_fetcher = self.fetcher.get("FORM_TEMPLATE_DETAIL")?;
        let field_fetcher = self.fetcher.get("FORM_FIELD")?;

        let detail_rows = template_detail_fetcher
            .fetch(Some(vec![uuid_values_filter(
                "template_id",
                vec![template_id],
            )]))
            .await?;

        if detail_rows.is_empty() {
            return Ok(());
        }

        let field_ids = detail_rows
            .iter()
            .filter_map(|row| row.get("form_field_id").and_then(|value| value.as_str()))
            .map(|value| value.to_string())
            .collect::<Vec<_>>();

        if field_ids.is_empty() {
            return Ok(());
        }

        let field_rows = field_fetcher
            .fetch(Some(vec![uuid_values_filter("id", field_ids)]))
            .await?;

        let field_map = field_rows
            .into_iter()
            .filter_map(|row| {
                let id = row
                    .get("id")
                    .and_then(|value| value.as_str())
                    .map(|id| id.to_string());

                id.map(|id| (id, row))
            })
            .collect::<HashMap<String, Value>>();

        let vacancy_id = self.extract_master_id(
            result,
            &config.import_config.master_table,
            &config.import_config.primary_column,
        )?;

        let mut requirement_rows = vec![];

        for detail in detail_rows {
            let Some(form_field_id) = detail.get("form_field_id").and_then(|value| value.as_str())
            else {
                continue;
            };

            let Some(field_meta) = field_map.get(form_field_id) else {
                continue;
            };

            let mut row = HashMap::from([
                ("id".to_string(), Value::String(Uuid::now_v7().to_string())),
                ("vacancy_id".to_string(), Value::String(vacancy_id.clone())),
                (
                    "key_field".to_string(),
                    field_meta.get("key_field").cloned().unwrap_or(Value::Null),
                ),
                (
                    "field_type".to_string(),
                    field_meta.get("field_type").cloned().unwrap_or(Value::Null),
                ),
                (
                    "field_purpose".to_string(),
                    field_meta
                        .get("field_purpose")
                        .cloned()
                        .unwrap_or(Value::Null),
                ),
                (
                    "is_default_question".to_string(),
                    field_meta
                        .get("is_default_question")
                        .cloned()
                        .unwrap_or(Value::Bool(false)),
                ),
                (
                    "is_range".to_string(),
                    field_meta
                        .get("is_range")
                        .cloned()
                        .unwrap_or(Value::Bool(false)),
                ),
                (
                    "is_mandatory".to_string(),
                    detail
                        .get("is_mandatory")
                        .cloned()
                        .unwrap_or(Value::Bool(false)),
                ),
            ]);

            Self::insert_optional_value(&mut row, "tooltip", field_meta.get("tooltip").cloned());
            Self::insert_optional_value(
                &mut row,
                "multiple_choice_option",
                field_meta.get("multiple_choice_option").cloned(),
            );
            Self::insert_optional_value(
                &mut row,
                "allowed_file_type",
                field_meta.get("allowed_file_type").cloned(),
            );
            Self::insert_optional_value(
                &mut row,
                "maximum_file",
                field_meta.get("maximum_file").cloned(),
            );
            Self::insert_optional_value(
                &mut row,
                "maximum_file_size_in_kb",
                field_meta.get("maximum_file_size_in_kb").cloned(),
            );
            Self::insert_optional_value(
                &mut row,
                "compare_operator",
                field_meta.get("compare_operator").cloned(),
            );
            Self::insert_optional_value(&mut row, "pivot", field_meta.get("pivot").cloned());
            Self::insert_optional_value(
                &mut row,
                "min_pivot",
                field_meta.get("min_pivot").cloned(),
            );
            Self::insert_optional_value(
                &mut row,
                "max_pivot",
                field_meta.get("max_pivot").cloned(),
            );
            Self::insert_optional_value(&mut row, "criteria", detail.get("criteria").cloned());
            Self::insert_optional_value(
                &mut row,
                "master_data",
                field_meta.get("master_data").cloned(),
            );
            Self::insert_optional_value(
                &mut row,
                "data_mapping",
                field_meta.get("data_mapping").cloned(),
            );

            requirement_rows.push(row);
        }

        if requirement_rows.is_empty() {
            return Ok(());
        }

        self.insert_has_many_result(result, target_table, requirement_rows);

        Ok(())
    }

    async fn append_stage_rows(
        &self,
        rows: &[Data],
        config: &ImportDatasetConfig,
        result: &mut HashMap<String, ImportTableResult>,
    ) -> anyhow::Result<()> {
        let target_table = config
            .import_config
            .fields
            .iter()
            .enumerate()
            .find_map(|(_, field)| {
                field.column.split('|').find_map(|group| {
                    let trimmed = group.trim();

                    if Self::is_stage_template_group(trimmed) {
                        trimmed
                            .split_once(':')
                            .and_then(|(table, _)| Self::resolve_sequential_table(config, table))
                    } else {
                        None
                    }
                })
            })
            .or_else(|| {
                Self::resolve_sequential_table_by_suffixes(
                    config,
                    &["ats_vacancy_stage", "vacancy_stage"],
                )
            })
            .unwrap_or_else(|| "warehouse.ats_vacancy_stage".to_string());

        let template_id = self
            .extract_master_string_value(
                result,
                &config.import_config.master_table,
                "requirement_stage_template_id",
            )
            .ok()
            .and_then(|value| Self::normalize_uuid_string(&value))
            .or_else(|| {
                self.extract_template_id_from_rows(rows, config, "requirement_stage_template_id")
                    .and_then(|value| Self::normalize_uuid_string(&value))
            });

        let Some(template_id) = template_id else {
            return Ok(());
        };

        self.upsert_main_value(
            result,
            &config.import_config.master_table,
            "requirement_stage_template_id",
            Value::String(template_id.clone()),
        )?;

        let template_detail_fetcher = self.fetcher.get("STAGE_TEMPLATE_DETAIL")?;
        let detail_rows = template_detail_fetcher
            .fetch(Some(vec![uuid_values_filter(
                "recruitment_stage_template_id",
                vec![template_id],
            )]))
            .await?;

        if detail_rows.is_empty() {
            return Ok(());
        }

        let vacancy_id = self.extract_master_id(
            result,
            &config.import_config.master_table,
            &config.import_config.primary_column,
        )?;

        let stage_rows = detail_rows
            .into_iter()
            .filter_map(|detail| {
                let stage_name = Self::stage_detail_value(&detail, &["stage_name", "name"])?;
                let stage_type = Self::stage_detail_value(&detail, &["stage_type", "type"])?;

                let mut row = HashMap::from([
                    ("id".to_string(), Value::String(Uuid::now_v7().to_string())),
                    ("vacancy_id".to_string(), Value::String(vacancy_id.clone())),
                    ("stage_name".to_string(), stage_name),
                    ("stage_type".to_string(), stage_type),
                    (
                        "is_default".to_string(),
                        Self::stage_detail_value(&detail, &["is_default"])
                            .unwrap_or(Value::Bool(false)),
                    ),
                    (
                        "sequence".to_string(),
                        detail.get("order").cloned().unwrap_or(Value::from(0)),
                    ),
                    (
                        "is_locked".to_string(),
                        Self::stage_detail_value(&detail, &["is_locked"])
                            .unwrap_or(Value::Bool(false)),
                    ),
                    (
                        "share_access".to_string(),
                        Self::stage_detail_value(&detail, &["share_access"])
                            .unwrap_or_else(|| Value::Array(vec![])),
                    ),
                ]);

                Self::insert_optional_value(
                    &mut row,
                    "stage_email_template_id",
                    Self::stage_detail_value(&detail, &["stage_email_template_id"]).and_then(
                        |value| {
                            value
                                .as_str()
                                .and_then(Self::normalize_uuid_string)
                                .map(Value::String)
                        },
                    ),
                );
                Self::insert_optional_value(
                    &mut row,
                    "stage_email_template_reject",
                    Self::stage_detail_value(&detail, &["stage_email_template_reject"]).and_then(
                        |value| {
                            value
                                .as_str()
                                .and_then(Self::normalize_uuid_string)
                                .map(Value::String)
                        },
                    ),
                );
                Self::insert_optional_value(
                    &mut row,
                    "form_template_id",
                    Self::stage_detail_value(&detail, &["form_template_id"]).and_then(|value| {
                        value
                            .as_str()
                            .and_then(Self::normalize_uuid_string)
                            .map(Value::String)
                    }),
                );

                Some(row)
            })
            .collect::<Vec<_>>();

        if stage_rows.is_empty() {
            return Ok(());
        }

        self.insert_has_many_result(result, target_table, stage_rows);

        Ok(())
    }

    async fn import_mapped(
        &self,
        rows: &[Data],
        config: &ImportDatasetConfig,
        cache: &Cache<String, Value>,
    ) -> anyhow::Result<HashMap<String, ImportTableResult>> {
        let base_config = self.build_base_config(config);
        let raw_row = rows.iter().map(calamine_to_json).collect::<Vec<_>>();
        info!(
            raw_row = ?raw_row,
            fields = ?base_config.import_config.fields.iter().map(|field| (&field.header, &field.column, &field.fetcher)).collect::<Vec<_>>(),
            "LSI vacancy dataset received import row"
        );
        let mut result = self
            .dynamic_dataset
            .import(rows, &base_config, cache)
            .await?;

        info!(
            dataset_result = ?result,
            "LSI vacancy dataset result after fetcher resolution"
        );

        self.normalize_numeric_columns(config, &mut result)?;

        self.append_config_scope_columns(config, &mut result)?;
        self.append_main_payload_columns(config, &mut result)?;
        self.append_publish_on_rows(config, &mut result)?;
        self.append_stage_rows(rows, config, &mut result).await?;
        self.append_requirement_rows(rows, config, &mut result)
            .await?;

        info!(
            final_dataset_result = ?result,
            "LSI vacancy dataset result ready for temporary loader"
        );

        Ok(result)
    }
}

#[async_trait::async_trait]
impl ImporterDatasetV1 for LsiImportVacancyV1 {
    async fn import(
        &self,
        rows: &[Data],
        config: &ImportDatasetConfig,
        cache: &Cache<String, Value>,
    ) -> anyhow::Result<std::collections::HashMap<String, ImportTableResult>> {
        let base_config = self.build_base_config(config);
        let mut validations = self
            .prepare_field_validations(&[rows], &base_config)
            .await?;
        let mut errors = Self::validate_row(rows, &mut validations);
        Self::validate_required_input_fields(rows, &base_config, &mut errors);
        let mut result = self.import_mapped(rows, &base_config, cache).await?;
        Self::merge_fetcher_errors(&mut result, &mut errors);
        self.validate_resolved_required_fields(rows, &result, &base_config, &mut errors);
        Self::attach_validation_errors(&mut result, errors);

        Ok(result)
    }

    async fn import_batch(
        &self,
        rows: &[&[Data]],
        config: &ImportDatasetConfig,
        cache: &Cache<String, Value>,
    ) -> anyhow::Result<Vec<HashMap<String, ImportTableResult>>> {
        let base_config = self.build_base_config(config);
        let mut validations = self.prepare_field_validations(rows, &base_config).await?;
        let mut result = Vec::with_capacity(rows.len());

        for row in rows {
            let mut errors = Self::validate_row(row, &mut validations);
            Self::validate_required_input_fields(row, &base_config, &mut errors);
            let mut imported = self.import_mapped(row, &base_config, cache).await?;
            Self::merge_fetcher_errors(&mut imported, &mut errors);
            self.validate_resolved_required_fields(row, &imported, &base_config, &mut errors);
            Self::attach_validation_errors(&mut imported, errors);
            result.push(imported);
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::LsiImportVacancyV1;

    #[test]
    fn extracts_source_column_as_filterable_array() {
        let value = LsiImportVacancyV1::filterable_value(
            json!([{"id": "test"}]),
            Some("id"),
            None,
            None,
            true,
        );

        assert_eq!(value, Some(json!(["test"])));
    }

    #[test]
    fn wraps_scalar_filterable_value_as_array() {
        let value =
            LsiImportVacancyV1::filterable_value(json!("published"), None, None, None, true);

        assert_eq!(value, Some(json!(["published"])));
    }

    #[test]
    fn extracts_job_position_custom_field_values() {
        let value = LsiImportVacancyV1::filterable_value(
            json!({
                "id": "job-position-1",
                "custom_fields": [
                    {"field_key": "jobFunctionId", "field_type": "UUID", "value_id": "id1"},
                    {"field_key": "jobFunctionId", "field_type": "UUID", "value_id": "id2"}
                ]
            }),
            Some("custom_fields"),
            Some("jobFunctionId"),
            None,
            true,
        );

        assert_eq!(value, Some(json!(["id1", "id2"])));
    }
}
