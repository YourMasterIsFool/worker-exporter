use std::path::PathBuf;

use anyhow::{Context, Result};
use calamine::{Data, DataType};
use serde_json::Value;
use sqlx::{Postgres, QueryBuilder};
use tracing::debug;

use crate::core::traits::load::{LoadEnum, LoaderPersitence};
use crate::core::types::{CastingEnum, WhereField};

pub fn reformated_value(key: &str, value: &serde_json::Value) -> Result<String> {
    let val = value[key]
        .as_str()
        .context(format!("missing {key}"))?
        .to_string();

    Ok(val)
}

pub fn resolve_query_builder_with_where_fields(qb: &mut QueryBuilder<Postgres>, optional_where_fields: &Option<Vec<WhereField>>){
    if let Some(where_fields) =  optional_where_fields {
        for filter in where_fields {

            let casting =  format!("::{}", filter.casting.unwrap_or_else(||CastingEnum::Varchar).as_str());
            if let Some(value) =  filter.value {
                qb.push(format!(" AND {} {} ${}", filter.field, filter.operator, casting));
                qb.push_bind(value);

            }

            if let Some(values) = filter.values {
                qb.push(format!(" AND {} {} ${}", filter.field, filter.operator, casting));
            }
        }
    }

}
pub fn calamine_to_json(dt: &Data) -> Value {
    match dt {
        Data::Int(i) => Value::from(*i),
        Data::Float(f) => Value::from(*f),
        Data::String(s) => Value::String(s.clone()),
        Data::Bool(b) => Value::Bool(*b),
        Data::DateTime(d) => Value::String(d.to_string()),
        _ => Value::Null,
    }
}

pub fn build_query<T: LoaderPersitence>(loader: &T, mode: &LoadEnum) -> String {
    let table = loader.table();
    let mut columns = loader.columns().join(",");
    let placeholders = loader
        .columns()
        .iter()
        .enumerate()
        .map(|(i, _)| format!("${}", i + 1))
        .collect::<Vec<_>>()
        .join(",");

    match mode {
        LoadEnum::Upsert => {
            let conflicted_columns = loader.conflicted_columns().join(",");
            format!(
                "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING",
                table, columns, placeholders, conflicted_columns
            )
        }
        LoadEnum::Insert => {
            format!(
                "INSERT INTO {} ({}) VALUES ({}) ",
                table, columns, placeholders
            )
        }
        LoadEnum::Update(updated_columns) => {
            let updated_columns = updated_columns.join(",");

            format!("UPDATE {} SET {} WHERE 1=1  ", table, updated_columns)
        }
        LoadEnum::Select(where_fields, from_column) => {
            if let Some(from_columns) = from_column {
                columns = from_columns.join(",");
            }

            debug!("columns {:?}", columns);
            let where_clause = where_fields
                .as_ref()
                .map(|fields| {
                    fields
                        .iter()
                        .map(|field| {
                            format!(
                                "{} {} {}",
                                field.field,
                                field.operator,
                                field.value.as_deref().unwrap_or_default()
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(" AND ")
                })
                .unwrap_or_default();
            if where_clause.is_empty() {
                format!("SELECT {} FROM {} WHERE 1=1  ", columns, table)
            } else {
                format!(
                    "SELECT {} FROM {} WHERE 1=1  {}  ",
                    columns, table, where_clause
                )
            }
        }
    }
}

pub fn remove_file(path: &PathBuf) -> std::io::Result<()> {
    std::fs::remove_file(path)
}
