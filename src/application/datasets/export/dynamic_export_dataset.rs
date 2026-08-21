use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use anyhow::{bail, Context};
use moka::future::Cache;
use serde_json::Value;
use sqlx::{PgPool, Postgres, QueryBuilder};

use crate::{
    application::{
        grpcs::{ats::decryptor::DecryptBatchRequest, gprc::GrpcService},
        transform_registry::TransformRegistry,
    },
    core::{
        config::database_manager::DatabaseManager,
        traits::{
            dataset_trait::Dataset,
            load::LoadEnum,
            orchestrator::{
                DynamicExportConfig, DynamicExportRelation, DynamicExportSource, ExporterColumn,
            },
        },
        types::{ExportRow, WhereField},
    },
};

pub struct DynamicExportDataset {
    databases: Arc<DatabaseManager>,
    grpc_service: Arc<GrpcService>,
    cache: Cache<String, Value>,
    transforms: Arc<TransformRegistry>,
}

impl DynamicExportDataset {
    pub fn new(
        databases: Arc<DatabaseManager>,
        grpc_service: Arc<GrpcService>,
        transforms: Arc<TransformRegistry>,
    ) -> Self {
        Self {
            databases,
            grpc_service,
            cache: Cache::builder()
                .time_to_live(Duration::from_mins(30))
                .max_capacity(10_000)
                .build(),
            transforms,
        }
    }

    fn pool(&self, database: &str) -> anyhow::Result<&PgPool> {
        match database {
            "organization" => Ok(&self.databases.organization),
            "ats" => Ok(&self.databases.ats),
            "master" => Ok(&self.databases.master),
            "warehouse" => Ok(&self.databases.warehouse),
            "hris" => Ok(&self.databases.hris),
            _ => bail!("dynamic export database is not allowed: {database}"),
        }
    }

    fn identifier(identifier: &str) -> anyhow::Result<&str> {
        let valid = identifier
            .chars()
            .next()
            .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
            && identifier
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || character == '_');
        if valid {
            Ok(identifier)
        } else {
            bail!("invalid dynamic export identifier: {identifier}")
        }
    }

    fn table(source: &DynamicExportSource) -> anyhow::Result<String> {
        Ok(format!(
            "{}.{}",
            Self::identifier(&source.schema)?,
            Self::identifier(&source.table)?
        ))
    }

    fn scalar(value: &Value) -> Option<String> {
        match value {
            Value::Null => None,
            Value::String(value) => Some(value.clone()),
            Value::Number(value) => Some(value.to_string()),
            Value::Bool(value) => Some(value.to_string()),
            _ => None,
        }
    }

    fn append_filters(
        query: &mut QueryBuilder<'_, Postgres>,
        filters: &[WhereField],
        alias: &str,
    ) -> anyhow::Result<()> {
        for filter in filters {
            let field = Self::identifier(&filter.field)?;
            let operator = filter.operator.trim().to_ascii_uppercase();
            query.push(" AND ").push(alias).push('.').push(field);
            match operator.as_str() {
                "=" | "!=" | "<>" | ">" | ">=" | "<" | "<=" => {
                    query.push("::text ").push(operator).push(' ').push_bind(
                        filter
                            .value
                            .clone()
                            .context("where field value is required")?,
                    );
                }
                "IS NULL" | "IS NOT NULL" => {
                    query.push(' ').push(operator);
                }
                "IN" | "NOT IN" => {
                    let values = filter
                        .values
                        .clone()
                        .or_else(|| filter.value.clone().map(|value| vec![value]))
                        .unwrap_or_default();
                    if values.is_empty() {
                        bail!("{operator} requires a value");
                    }
                    query.push("::text ").push(operator).push(" (");
                    let mut separated = query.separated(", ");
                    for value in values {
                        separated.push_bind(value);
                    }
                    separated.push_unseparated(")");
                }
                "ARRAY_OVERLAPS" | "&&" => {
                    let values = filter
                        .values
                        .clone()
                        .or_else(|| filter.value.clone().map(|value| vec![value]))
                        .unwrap_or_default();
                    if values.is_empty() {
                        bail!("{operator} requires a value");
                    }
                    query.push(" && ").push_bind(values).push("::text[]");
                }
                "JSONB_CONTAINS" | "@>" => {
                    let values = filter
                        .values
                        .clone()
                        .or_else(|| filter.value.clone().map(|value| vec![value]))
                        .unwrap_or_default();
                    if values.is_empty() {
                        bail!("{operator} requires a value");
                    }
                    query
                        .push(" @> ")
                        .push_bind(serde_json::to_string(&values)?)
                        .push("::jsonb");
                }
                _ => bail!(
                    "dynamic export operator is not allowed: {}",
                    filter.operator
                ),
            }
        }
        Ok(())
    }

    async fn fetch_source(
        &self,
        source: &DynamicExportSource,
        filters: &[WhereField],
        after: Option<&str>,
        limit: i64,
    ) -> anyhow::Result<Vec<Value>> {
        let table = Self::table(source)?;
        let cursor = source
            .cursor
            .as_ref()
            .context("dynamic export source.cursor is required")?;
        let cursor_column = Self::identifier(&cursor.column)?;
        let mut query =
            QueryBuilder::<Postgres>::new(format!("SELECT to_jsonb(t) FROM {table} t WHERE 1=1"));
        Self::append_filters(&mut query, &source.where_fields, "t")?;
        Self::append_filters(&mut query, filters, "t")?;
        let direction = match cursor.direction.to_ascii_lowercase().as_str() {
            "asc" => "ASC",
            "desc" => "DESC",
            _ => bail!("cursor direction must be asc or desc"),
        };
        if let Some(after) = after.filter(|cursor| !cursor.is_empty()) {
            query
                .push(" AND t.")
                .push(cursor_column)
                .push(if direction == "ASC" {
                    "::text > "
                } else {
                    "::text < "
                })
                .push_bind(after);
        }
        query
            .push(" ORDER BY t.")
            .push(cursor_column)
            .push(' ')
            .push(direction)
            .push(" LIMIT ")
            .push_bind(limit);
        Ok(query
            .build_query_scalar::<Value>()
            .fetch_all(self.pool(&source.database)?)
            .await?)
    }

    async fn count_source(
        &self,
        source: &DynamicExportSource,
        filters: &[WhereField],
    ) -> anyhow::Result<i64> {
        let table = Self::table(source)?;
        let mut query =
            QueryBuilder::<Postgres>::new(format!("SELECT COUNT(*) FROM {table} t WHERE 1=1"));
        Self::append_filters(&mut query, &source.where_fields, "t")?;
        Self::append_filters(&mut query, filters, "t")?;
        Ok(query
            .build_query_scalar::<i64>()
            .fetch_one(self.pool(&source.database)?)
            .await?)
    }

    async fn load_relation(
        &self,
        rows: &[Value],
        relation: &DynamicExportRelation,
    ) -> anyhow::Result<HashMap<String, Value>> {
        let foreign_key = Self::identifier(&relation.foreign_key)?;
        let keys = Self::local_keys(rows, &relation.local_key);
        if keys.is_empty() {
            return Ok(HashMap::new());
        }

        let mut resolved = HashMap::<String, Value>::new();
        let cache_prefix = format!(
            "{}:{}:{}:{}",
            relation.source.database, relation.source.schema, relation.source.table, foreign_key
        );
        let mut missing = Vec::new();
        for key in keys {
            let cache_key = format!("{cache_prefix}:{key}");
            if relation.is_cache {
                if let Some(value) = self.cache.get(&cache_key).await {
                    resolved.insert(key, value);
                    continue;
                }
            }
            missing.push(key);
        }

        if !missing.is_empty() {
            let table = Self::table(&relation.source)?;
            let mut query = QueryBuilder::<Postgres>::new(format!(
                "SELECT to_jsonb(t) FROM {table} t WHERE t.{foreign_key}::text IN ("
            ));
            let mut separated = query.separated(", ");
            for key in &missing {
                separated.push_bind(key);
            }
            separated.push_unseparated(")");
            Self::append_filters(&mut query, &relation.source.where_fields, "t")?;
            if let Some(order_by) = &relation.order_by {
                let column = Self::identifier(&order_by.column)?;
                let direction = match order_by.direction.to_ascii_lowercase().as_str() {
                    "asc" => "ASC",
                    "desc" => "DESC",
                    _ => bail!("order direction must be asc or desc"),
                };
                query
                    .push(" ORDER BY t.")
                    .push(column)
                    .push(' ')
                    .push(direction);
            }
            let relation_rows = query
                .build_query_scalar::<Value>()
                .fetch_all(self.pool(&relation.source.database)?)
                .await?;
            let mut grouped = HashMap::<String, Vec<Value>>::new();
            for row in relation_rows {
                if let Some(key) = row.get(foreign_key).and_then(Self::scalar) {
                    grouped.entry(key).or_default().push(row);
                }
            }
            for key in missing {
                let rows = grouped.remove(&key).unwrap_or_default();
                let value = if relation.cardinality.eq_ignore_ascii_case("one") {
                    rows.into_iter().next().unwrap_or(Value::Null)
                } else {
                    Value::Array(rows)
                };
                if relation.is_cache {
                    self.cache
                        .insert(format!("{cache_prefix}:{key}"), value.clone())
                        .await;
                }
                resolved.insert(key, value);
            }
        }

        Ok(resolved)
    }

    fn apply_relation(
        rows: &mut [Value],
        relation: &DynamicExportRelation,
        resolved: &HashMap<String, Value>,
    ) {
        let local_key = &relation.local_key;
        for row in rows {
            let Some(object) = row.as_object_mut() else {
                continue;
            };
            let keys = Self::path_values(&Value::Object(object.clone()), local_key);
            let values = keys
                .iter()
                .filter_map(|key| resolved.get(key).cloned())
                .flat_map(|value| match value {
                    Value::Array(values) => values,
                    value => vec![value],
                })
                .collect::<Vec<_>>();
            object.insert(
                relation.name.clone(),
                if relation.cardinality.eq_ignore_ascii_case("one") {
                    values.into_iter().next().unwrap_or(Value::Null)
                } else {
                    Value::Array(values)
                },
            );
        }
    }

    fn path(value: &Value, path: &str) -> Value {
        if path.is_empty() {
            return Value::Null;
        }
        Self::path_value(value, &path.split('.').collect::<Vec<_>>())
    }

    fn path_value(value: &Value, path: &[&str]) -> Value {
        let Some((key, rest)) = path.split_first() else {
            return value.clone();
        };
        match value {
            Value::Array(values) => Value::Array(
                values
                    .iter()
                    .map(|value| Self::path_value(value, path))
                    .filter(|value| !value.is_null())
                    .collect(),
            ),
            Value::Object(_) => value
                .get(*key)
                .map(|value| Self::path_value(value, rest))
                .unwrap_or(Value::Null),
            Value::String(raw) => serde_json::from_str::<Value>(raw)
                .ok()
                .map(|value| Self::path_value(&value, path))
                .unwrap_or(Value::Null),
            _ => Value::Null,
        }
    }

    fn scalar_values(value: &Value) -> Vec<String> {
        match value {
            Value::Array(values) => values.iter().flat_map(Self::scalar_values).collect(),
            value => Self::scalar(value).into_iter().collect(),
        }
    }

    fn normalized_key(value: &str) -> String {
        value
            .chars()
            .filter(|character| !character.is_whitespace())
            .flat_map(char::to_lowercase)
            .collect()
    }

    fn path_values(value: &Value, path: &str) -> Vec<String> {
        Self::scalar_values(&Self::path(value, path))
    }

    fn local_keys(rows: &[Value], local_key: &str) -> HashSet<String> {
        rows.iter()
            .flat_map(|row| Self::path_values(row, local_key))
            .collect()
    }

    fn custom_json_value(value: Value, key: &str) -> Value {
        let value = match value {
            Value::String(raw) => serde_json::from_str(&raw).unwrap_or(Value::String(raw)),
            value => value,
        };

        value.get(key).cloned().unwrap_or(Value::Null)
    }

    fn option_texts(row: &Value, value: Value, relation: &str) -> Value {
        if value.is_null() {
            return Value::Null;
        }
        let options = row
            .get(relation)
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let values = match value {
            Value::Array(values) => values,
            value => vec![value],
        };
        Value::Array(
            values
                .into_iter()
                .map(|value| {
                    let id = Self::scalar(&value).unwrap_or_default();
                    options
                        .iter()
                        .find(|option| {
                            option
                                .get("id")
                                .and_then(Value::as_str)
                                .is_some_and(|option_id| option_id == id)
                        })
                        .and_then(|option| option.get("option_text"))
                        .cloned()
                        .unwrap_or(Value::String(id))
                })
                .collect(),
        )
    }

    fn question_matches(row: &Value, question_id: &str, question_text: &str) -> bool {
        row.get("form_questions")
            .and_then(Value::as_array)
            .is_some_and(|questions| {
                questions.iter().any(|question| {
                    question
                        .get("id")
                        .and_then(Self::scalar)
                        .is_some_and(|id| id == question_id)
                        && question
                            .get("question_text")
                            .and_then(Value::as_str)
                            .is_some_and(|text| {
                                Self::normalized_key(text) == Self::normalized_key(question_text)
                            })
                })
            })
    }

    fn custom_value(
        row: &Value,
        field: &ExporterColumn,
        relation: &DynamicExportRelation,
    ) -> Value {
        let field_key_column = relation.field_key_column.as_deref().unwrap_or("field_key");
        let field_type_column = relation
            .field_type_column
            .as_deref()
            .unwrap_or("field_type");
        let values = row
            .get(&relation.name)
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter(|custom| {
                let key = custom.get(field_key_column).and_then(Value::as_str);
                field.custom_key.as_deref().is_some_and(|custom_key| {
                    key.is_some_and(|key| {
                        Self::normalized_key(key) == Self::normalized_key(custom_key)
                    }) || key.is_some_and(|question_id| {
                        Self::question_matches(row, question_id, custom_key)
                    })
                })
            })
            .filter_map(|custom| {
                let field_type = custom
                    .get(field_type_column)
                    .and_then(Value::as_str)
                    .unwrap_or("TEXT");
                let column = relation
                    .value_columns
                    .as_ref()
                    .and_then(|columns| columns.get(field_type))
                    .map(String::as_str)
                    .unwrap_or_else(|| match field_type {
                        "NUMBER" => "value_number",
                        "UUID" | "ID" => "value_id",
                        "BOOLEAN" | "BOOL" => "value_bool",
                        "DATE" => "value_date",
                        _ => "value_text",
                    });
                let column = field
                    .custom_json_key
                    .as_ref()
                    .map(|key| key.column.as_str())
                    .unwrap_or(column);
                let value = custom.get(column).cloned()?;
                let value = field.custom_json_key.as_ref().map_or(value.clone(), |key| {
                    let value = Self::custom_json_value(value, &key.value);
                    key.options_relation
                        .as_deref()
                        .map(|relation| Self::option_texts(row, value.clone(), relation))
                        .unwrap_or(value)
                });
                Some(value)
            })
            .collect::<Vec<_>>();
        Value::Array(values)
    }

    fn format_rows(&self, rows: Vec<Value>, config: &DynamicExportConfig) -> Vec<ExportRow> {
        rows.into_iter()
            .map(|row| {
                let fields = config
                    .fields
                    .iter()
                    .map(|field| {
                        let value = field
                            .relation
                            .as_deref()
                            .and_then(|name| {
                                config
                                    .relations
                                    .iter()
                                    .find(|relation| relation.name == name)
                            })
                            .filter(|relation| relation.is_custom)
                            .map(|relation| Self::custom_value(&row, field, relation))
                            .unwrap_or_else(|| Self::path(&row, &field.column));
                        let value = self.transforms.resolve(field.transform.as_deref(), &value);
                        if value.is_empty() {
                            field.default_value.clone().unwrap_or_else(|| "-".into())
                        } else {
                            value
                        }
                    })
                    .collect();
                ExportRow { fields }
            })
            .collect()
    }

    async fn decrypt_fields(
        &self,
        rows: &mut [Value],
        config: &DynamicExportConfig,
    ) -> anyhow::Result<()> {
        let fields = config
            .fields
            .iter()
            .filter(|field| field.transform.as_deref() == Some("decrypt"))
            .filter(|field| !field.column.contains('.'))
            .collect::<Vec<_>>();
        let ciphertexts = rows
            .iter()
            .flat_map(|row| fields.iter().filter_map(|field| row.get(&field.column)))
            .filter_map(Value::as_str)
            .filter(|value| !value.is_empty() && *value != "-")
            .map(str::to_owned)
            .collect::<HashSet<_>>();
        if ciphertexts.is_empty() {
            return Ok(());
        }
        let plaintexts = self
            .grpc_service
            .ats_decryptor_grpc
            .clone()
            .batch_decrypt(DecryptBatchRequest {
                ciphertexts: ciphertexts.into_iter().collect(),
            })
            .await?
            .plaintexts;
        for row in rows {
            let Some(row) = row.as_object_mut() else {
                continue;
            };
            for field in &fields {
                if let Some(plaintext) = row
                    .get(&field.column)
                    .and_then(Value::as_str)
                    .and_then(|ciphertext| plaintexts.get(ciphertext))
                {
                    row.insert(field.column.clone(), Value::String(plaintext.clone()));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::DynamicExportDataset;

    #[test]
    fn local_keys_support_nested_paths() {
        let keys = DynamicExportDataset::local_keys(
            &[json!({ "applicant": { "vacancy_id": "vacancy-1" } })],
            "applicant.vacancy_id",
        );

        assert!(keys.contains("vacancy-1"));
    }

    #[test]
    fn custom_json_key_extracts_nested_value() {
        assert_eq!(
            DynamicExportDataset::custom_json_value(
                json!(r#"{"number_value":60}"#),
                "number_value",
            ),
            json!(60)
        );
        assert_eq!(
            DynamicExportDataset::option_texts(
                &json!({
                    "options": [
                        {"id": "uuid1", "option_text": "GET"},
                        {"id": "uuid2", "option_text": "POST"}
                    ]
                }),
                json!(["uuid1", "uuid2"]),
                "options",
            ),
            json!(["GET", "POST"])
        );
        assert_eq!(
            DynamicExportDataset::local_keys(
                &[json!({
                    "form_answers": [{
                        "answer_value": "{\"selected_options\":[\"uuid1\",\"uuid2\"]}"
                    }]
                })],
                "form_answers.answer_value.selected_options",
            ),
            ["uuid1".to_string(), "uuid2".to_string()]
                .into_iter()
                .collect()
        );
    }

    #[test]
    fn question_text_matches_form_question_id() {
        assert!(DynamicExportDataset::question_matches(
            &json!({
                "form_questions": [{
                    "id": "question-1",
                    "question_text": "Nama\nPerusahaan/Organisasi Anda"
                }]
            }),
            "question-1",
            "nama perusahaan / organisasi anda",
        ));
        assert!(!DynamicExportDataset::question_matches(
            &json!({
                "form_questions": [{
                    "id": "question-1",
                    "question_text": "Jabatan Anda"
                }]
            }),
            "question-1",
            "Nama Perusahaan/Organisasi Anda",
        ));
    }
}

#[async_trait::async_trait]
impl Dataset for DynamicExportDataset {
    async fn get_min_cursor(&self, _: Option<Vec<WhereField>>) -> anyhow::Result<String> {
        bail!("dynamic export must use dynamic_export_config")
    }

    async fn fetch_batch(
        &self,
        _: &LoadEnum,
        _: Option<String>,
        _: i64,
    ) -> anyhow::Result<(Vec<ExportRow>, Option<String>)> {
        bail!("dynamic export must use dynamic_export_config")
    }

    async fn fetch_batch_with_dynamic_export_config(
        &self,
        strategy: &LoadEnum,
        cursor: Option<String>,
        limit: i64,
        config: &DynamicExportConfig,
    ) -> anyhow::Result<(Vec<ExportRow>, Option<String>)> {
        let filters = match strategy {
            LoadEnum::Select(filters, _) => filters.clone().unwrap_or_default(),
            _ => vec![],
        };
        let rows = self
            .fetch_source(&config.source, &filters, cursor.as_deref(), limit)
            .await?;
        let cursor_column = &config
            .source
            .cursor
            .as_ref()
            .context("dynamic export source.cursor is required")?
            .column;
        let next_cursor = rows
            .last()
            .and_then(|row| row.get(cursor_column))
            .and_then(Self::scalar);
        let mut rows = rows;
        let mut pending = config.relations.iter().collect::<Vec<_>>();
        let mut completed = HashSet::new();
        while !pending.is_empty() {
            let (ready, waiting): (Vec<_>, Vec<_>) = pending.into_iter().partition(|relation| {
                relation
                    .depends_on
                    .as_deref()
                    .map(|dependency| completed.contains(dependency))
                    .unwrap_or(true)
            });
            if ready.is_empty() {
                bail!("dynamic export has unresolved relation dependency");
            }
            let loaded = futures_util::future::try_join_all(
                ready
                    .iter()
                    .map(|relation| self.load_relation(&rows, relation)),
            )
            .await?;
            for (relation, resolved) in ready.into_iter().zip(loaded) {
                Self::apply_relation(&mut rows, relation, &resolved);
                completed.insert(relation.name.as_str());
            }
            pending = waiting;
        }
        self.decrypt_fields(&mut rows, config).await?;
        Ok((self.format_rows(rows, config), next_cursor))
    }

    async fn count_with_dynamic_export_config(
        &self,
        strategy: &LoadEnum,
        config: &DynamicExportConfig,
    ) -> anyhow::Result<i64> {
        let filters = match strategy {
            LoadEnum::Select(filters, _) => filters.clone().unwrap_or_default(),
            _ => vec![],
        };
        self.count_source(&config.source, &filters).await
    }
}
