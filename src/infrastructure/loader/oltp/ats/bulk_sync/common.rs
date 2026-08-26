use anyhow::Result;
use bigdecimal::BigDecimal;
use chrono::{NaiveDate, NaiveDateTime};
use serde_json::Value;
use sqlx::{Postgres, QueryBuilder, Transaction};
use std::str::FromStr;
use uuid::Uuid;

use crate::core::types::{CastingEnum, SycnTableResult};

pub(super) struct ColumnCasting {
    pub(super) column: &'static str,
    pub(super) casting: CastingEnum,
}

pub(super) struct OltpBulkTableLoader<'a> {
    pub(super) source_table: &'a str,
    pub(super) target_table: &'a str,
    pub(super) column_map: &'a [(&'a str, &'a str)],
    pub(super) column_castings: &'a [ColumnCasting],
    pub(super) conflict_columns: &'a [&'a str],
}

impl<'a> OltpBulkTableLoader<'a> {
    #[tracing::instrument(
        skip(self, tx, result),
        fields(types = "oltp_bulk_table_loader", source_table = %self.source_table, target_table = %self.target_table)
    )]
    pub(super) async fn load(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        result: &SycnTableResult,
    ) -> Result<i64> {
        let Some(table_data) = result.sources.sources.get(self.source_table) else {
            return Ok(0);
        };

        if table_data.values.is_empty() {
            return Ok(0);
        }

        let target_columns = table_data
            .columns
            .iter()
            .map(|column| self.target_column(column))
            .collect::<Vec<_>>();

        let mut qb: QueryBuilder<Postgres> =
            QueryBuilder::new(format!("INSERT INTO {} (", self.target_table));
        let mut separated = qb.separated(", ");
        for column in &target_columns {
            separated.push(Self::quote_identifier(column));
        }
        qb.push(") ");

        qb.push_values(&table_data.values, |mut row_builder, row| {
            for (index, column) in target_columns.iter().enumerate() {
                let value = row.get(index).unwrap_or(&Value::Null);
                let casting = self.column_casting(column);
                Self::bind_value(&mut row_builder, value, column, casting);
            }
        });

        qb.push(" ON CONFLICT (");
        let mut conflict_columns = qb.separated(", ");
        for column in self.conflict_columns {
            conflict_columns.push(Self::quote_identifier(column));
        }
        qb.push(")");

        let update_columns = target_columns
            .iter()
            .filter(|column| {
                !self
                    .conflict_columns
                    .iter()
                    .any(|conflict_column| *conflict_column == column.as_str())
            })
            .collect::<Vec<_>>();

        if update_columns.is_empty() {
            qb.push(" DO NOTHING");
            let query_result = qb.build().execute(&mut **tx).await?;
            return Ok(query_result.rows_affected() as i64);
        }

        qb.push(" DO UPDATE SET ");
        let mut updates = qb.separated(", ");
        for column in update_columns {
            let quoted = Self::quote_identifier(column);
            updates.push(format!("{quoted} = EXCLUDED.{quoted}"));
        }

        let query_result = qb.build().execute(&mut **tx).await?;

        Ok(query_result.rows_affected() as i64)
    }

    fn target_column(&self, source_column: &str) -> String {
        self.column_map
            .iter()
            .find_map(|(from, to)| (*from == source_column).then_some((*to).to_string()))
            .unwrap_or_else(|| source_column.to_string())
    }

    fn quote_identifier(identifier: &str) -> String {
        format!("\"{}\"", identifier.replace('"', "\"\""))
    }

    fn column_casting(&self, column_name: &str) -> Option<&CastingEnum> {
        self.column_castings.iter().find_map(|column_casting| {
            (column_casting.column == column_name).then_some(&column_casting.casting)
        })
    }

    fn push_cast<'b>(
        builder: &mut sqlx::query_builder::Separated<'b, '_, Postgres, &str>,
        casting: &CastingEnum,
    ) {
        builder.push_unseparated(format!("::{}", casting.as_str()));
    }

    fn bind_casted_value<'b>(
        builder: &mut sqlx::query_builder::Separated<'b, '_, Postgres, &str>,
        value: &Value,
        casting: &CastingEnum,
    ) {
        match value {
            Value::Null => {
                builder.push_bind(None::<String>);
            }
            Value::String(text) => {
                builder.push_bind(text.clone());
            }
            Value::Bool(boolean) => {
                builder.push_bind(boolean.to_string());
            }
            Value::Number(number) => {
                builder.push_bind(number.to_string());
            }
            Value::Object(_) | Value::Array(_) => {
                builder.push_bind(value.to_string());
            }
        }

        Self::push_cast(builder, casting);
    }

    fn is_uuid_column(column_name: &str) -> bool {
        column_name != "logging_id"
            && (column_name == "id" || column_name == "created_by" || column_name.ends_with("_id"))
    }

    fn is_timestamp_column(column_name: &str) -> bool {
        column_name.ends_with("_at") || matches!(column_name, "expired_date")
    }

    fn is_date_column(column_name: &str) -> bool {
        column_name.ends_with("_date") && !Self::is_timestamp_column(column_name)
    }

    fn is_json_column(column_name: &str) -> bool {
        column_name.ends_with("_meta")
            || matches!(
                column_name,
                "snapshot"
                    | "job_functions"
                    | "verified_status"
                    | "filterable_attribute"
                    | "share_access"
                    | "multiple_choice_option"
                    | "allowed_file_type"
                    | "criteria"
                    | "master_data"
                    | "data_mapping"
                    | "meta"
                    | "employee_meta"
            )
    }

    fn is_bool_column(column_name: &str) -> bool {
        column_name.starts_with("is_") || matches!(column_name, "value_bool")
    }

    fn is_number_column(column_name: &str) -> bool {
        matches!(
            column_name,
            "headcount_quota"
                | "salary_min"
                | "salary_max"
                | "total_hired"
                | "current_hired"
                | "currentHired"
                | "sla"
                | "sequence"
                | "maximum_file"
                | "maximum_file_size_in_kb"
                | "pivot"
                | "min_pivot"
                | "max_pivot"
                | "value_number"
        )
    }

    fn parse_timestamp(value: &str) -> Option<NaiveDateTime> {
        chrono::DateTime::parse_from_rfc3339(value)
            .map(|date_time| date_time.naive_utc())
            .ok()
            .or_else(|| NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f").ok())
            .or_else(|| NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f").ok())
            .or_else(|| NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S").ok())
            .or_else(|| NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S").ok())
    }

    fn bind_value<'b>(
        builder: &mut sqlx::query_builder::Separated<'b, '_, Postgres, &str>,
        value: &Value,
        column_name: &str,
        casting: Option<&CastingEnum>,
    ) {
        if let Some(casting) = casting {
            Self::bind_casted_value(builder, value, casting);
            return;
        }

        if matches!(
            column_name,
            "keywords" | "active_publish_keys" | "permission_keys"
        ) {
            let values = value
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter_map(|item| item.as_str().map(ToString::to_string))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            builder.push_bind(values);
            return;
        }

        if column_name == "job_function_ids" {
            let values = value.as_array().map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .filter_map(|value| Uuid::parse_str(value).ok())
                    .collect::<Vec<_>>()
            });
            builder.push_bind(values);
            return;
        }

        match value {
            Value::Null => {
                if Self::is_uuid_column(column_name) {
                    builder.push_bind(None::<Uuid>);
                } else if Self::is_timestamp_column(column_name) {
                    builder.push_bind(None::<NaiveDateTime>);
                } else if Self::is_date_column(column_name) {
                    builder.push_bind(None::<NaiveDate>);
                } else if Self::is_json_column(column_name) {
                    builder.push_bind(None::<Value>);
                } else if Self::is_bool_column(column_name) {
                    builder.push_bind(None::<bool>);
                } else if column_name == "value_number" {
                    builder.push_bind(None::<BigDecimal>);
                } else if Self::is_number_column(column_name) {
                    builder.push_bind(None::<i64>);
                } else {
                    builder.push_bind(None::<String>);
                }
            }
            Value::Bool(boolean) => {
                builder.push_bind(*boolean);
            }
            Value::Number(number) => {
                if column_name == "value_number" {
                    builder.push_bind(BigDecimal::from_str(&number.to_string()).ok());
                } else if let Some(integer) = number.as_i64() {
                    builder.push_bind(integer);
                } else if let Some(float) = number.as_f64() {
                    builder.push_bind(float);
                } else {
                    builder.push_bind(number.to_string());
                }
            }
            Value::String(text) => {
                let trimmed = text.trim();
                if Self::is_uuid_column(column_name) {
                    builder.push_bind(Uuid::parse_str(trimmed).ok());
                } else if Self::is_timestamp_column(column_name) {
                    builder.push_bind(Self::parse_timestamp(trimmed));
                } else if Self::is_date_column(column_name) {
                    builder.push_bind(NaiveDate::parse_from_str(trimmed, "%Y-%m-%d").ok());
                } else if column_name == "value_number" {
                    builder.push_bind(BigDecimal::from_str(trimmed).ok());
                } else if let Ok(uuid) = Uuid::parse_str(trimmed) {
                    builder.push_bind(uuid);
                } else if let Some(date_time) = Self::parse_timestamp(trimmed) {
                    builder.push_bind(date_time);
                } else if let Ok(date) = NaiveDate::parse_from_str(trimmed, "%Y-%m-%d") {
                    builder.push_bind(date);
                } else if let Ok(integer) = trimmed.parse::<i64>() {
                    builder.push_bind(integer);
                } else if let Ok(float) = trimmed.parse::<f64>() {
                    if float.fract() == 0.0 {
                        builder.push_bind(float as i64);
                    } else {
                        builder.push_bind(float);
                    }
                } else {
                    builder.push_bind(text.clone());
                }
            }
            Value::Object(_) | Value::Array(_) => {
                builder.push_bind(value.clone());
            }
        }
    }
}
