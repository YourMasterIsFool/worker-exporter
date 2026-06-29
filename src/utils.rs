use std::path::PathBuf;

use anyhow::{Context, Result};
use tracing::debug;

use crate::core::traits::load::{LoadEnum, LoaderWarehouse};

pub fn reformated_value(key: &str, value: &serde_json::Value) -> Result<String> {
    let val = value[key]
        .as_str()
        .context(format!("missing {key}"))?
        .to_string();

    Ok(val)
}

pub fn build_query<T: LoaderWarehouse>(loader: &T, mode: &LoadEnum) -> String {
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
