use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use calamine::Data;
use moka::future::Cache;
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::{
    application::fetch_source_regitry::DataFetcherJsonRegistry,
    core::{
        traits::{dataset_trait::ImporterDatasetV1, orchestrator::Filterable},
        types::{CastingEnum, ImportDatasetConfig, ImportTableResult, TableStrategy, WhereField},
    },
    infrastructure::datasets::import::dynamci_dataset_v1::DynamicDatasetV1,
    utils::value::calamine_to_json,
};

pub struct LsiImportVacancyV1 {
    fetcher: Arc<DataFetcherJsonRegistry>,
    dynamic_dataset: DynamicDatasetV1,
}

impl LsiImportVacancyV1 {
    fn parse_table_column_path(path: &str) -> Option<(&str, &str)> {
        path.trim().split_once(':')
    }

    fn value_as_keyword_list(value: &Value) -> Vec<String> {
        match value {
            Value::Null => vec![],
            Value::String(text) if !text.trim().is_empty() => vec![text.trim().to_string()],
            Value::String(_) => vec![],
            Value::Array(items) => items
                .iter()
                .flat_map(Self::value_as_keyword_list)
                .collect::<Vec<_>>(),
            Value::Bool(boolean) => vec![boolean.to_string()],
            Value::Number(number) => vec![number.to_string()],
            other => vec![other.to_string()],
        }
    }

    fn extract_custom_field_resolved_value(value: &Value) -> Option<Value> {
        let Value::Object(map) = value else {
            return None;
        };

        [
            "value_text",
            "value_number",
            "value_id",
            "value_bool",
            "value_date",
        ]
        .iter()
        .find_map(|key| map.get(*key).filter(|value| !value.is_null()).cloned())
    }

    fn value_from_object(source: Option<&Value>, keys: &[&str]) -> Option<Value> {
        let Value::Object(map) = source? else {
            return None;
        };

        keys.iter().find_map(|key| map.get(*key).cloned())
    }

    fn stage_detail_value(detail: &Value, keys: &[&str]) -> Option<Value> {
        Self::value_from_object(detail.get("recruitment_stage_meta"), keys)
            .or_else(|| Self::value_from_object(detail.get("stage_snapshot"), keys))
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

    pub fn new(fetcher: Arc<DataFetcherJsonRegistry>) -> Self {
        Self {
            fetcher: fetcher.clone(),
            dynamic_dataset: DynamicDatasetV1::new(fetcher),
        }
    }

    fn build_base_config(&self, config: &ImportDatasetConfig) -> ImportDatasetConfig {
        let mut cloned = config.clone();
        cloned.import_config.fields = cloned
            .import_config
            .fields
            .into_iter()
            .map(|mut field| {
                let groups = field
                    .column
                    .split('|')
                    .filter(|group| {
                        let trimmed = group.trim();
                        !trimmed.ends_with(":*") && !Self::is_stage_template_group(trimmed)
                    })
                    .map(|group| group.trim().to_string())
                    .collect::<Vec<_>>();

                field.column = groups.join("|");
                field
            })
            .collect();
        cloned
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

                    if field_key == column {
                        return Self::extract_custom_field_resolved_value(custom_value);
                    }
                }

                None
            }
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
            } in main_filterable
            {
                let Some((table, column)) = Self::parse_table_column_path(main_column) else {
                    continue;
                };

                if let Some(value) = self.extract_value_by_path(result, table, column) {
                    filterable_attribute.insert(import_column.to_string(), value);
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
                    keywords.extend(Self::value_as_keyword_list(&value));
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
            .fetch(Some(vec![WhereField {
                field: "template_id".into(),
                operator: "=".into(),
                value: None,
                values: Some(vec![template_id]),
                casting: Some(CastingEnum::Uuid),
            }]))
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
            .fetch(Some(vec![WhereField {
                field: "id".into(),
                operator: "=".into(),
                value: None,
                values: Some(field_ids),
                casting: Some(CastingEnum::Uuid),
            }]))
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
            .fetch(Some(vec![WhereField {
                field: "recruitment_stage_template_id".into(),
                operator: "=".into(),
                value: None,
                values: Some(vec![template_id]),
                casting: Some(CastingEnum::Uuid),
            }]))
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
        let mut result = self
            .dynamic_dataset
            .import(rows, &base_config, cache)
            .await?;

        self.append_config_scope_columns(config, &mut result)?;
        self.append_main_payload_columns(config, &mut result)?;
        self.append_stage_rows(rows, config, &mut result).await?;
        self.append_requirement_rows(rows, config, &mut result)
            .await?;

        Ok(result)
    }
}
