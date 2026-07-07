use std::sync::Arc;

use anyhow::Result;
use chrono::{NaiveDate, NaiveDateTime};
use serde_json::Value;
use sqlx::{Postgres, QueryBuilder, Transaction};
use uuid::Uuid;

use crate::{
    core::{traits::load::BulkLoaderTrait, types::SycnTableResult},
    infrastructure::db::ats_client::AtsClient,
};

pub struct AtsVacancyBulkLoader {
    pub client: Arc<AtsClient>,
}

pub struct AtsVacancyCustomFieldBulkLoader {
    pub client: Arc<AtsClient>,
}

pub struct AtsVacancyPicBulkLoader {
    pub client: Arc<AtsClient>,
}

pub struct AtsVacancyRequirementBulkLoader {
    pub client: Arc<AtsClient>,
}

pub struct AtsVacancyStageBulkLoader {
    pub client: Arc<AtsClient>,
}

struct OltpBulkTableLoader<'a> {
    source_table: &'a str,
    target_table: &'a str,
    column_map: &'a [(&'a str, &'a str)],
}

impl<'a> OltpBulkTableLoader<'a> {
    async fn load(
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
            for column in &table_data.columns {
                let value = row.get(column).unwrap_or(&Value::Null);
                Self::bind_value(&mut row_builder, value, column);
            }
        });

        qb.push(" ON CONFLICT (id) DO UPDATE SET ");
        let update_columns = target_columns
            .iter()
            .filter(|column| column.as_str() != "id")
            .collect::<Vec<_>>();
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

    fn bind_value<'b>(
        builder: &mut sqlx::query_builder::Separated<'b, '_, Postgres, &str>,
        value: &Value,
        column_name: &str,
    ) {
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

        match value {
            Value::Null => {
                builder.push_bind(None::<String>);
            }
            Value::Bool(boolean) => {
                builder.push_bind(*boolean);
            }
            Value::Number(number) => {
                if let Some(integer) = number.as_i64() {
                    builder.push_bind(integer);
                } else if let Some(float) = number.as_f64() {
                    builder.push_bind(float);
                } else {
                    builder.push_bind(number.to_string());
                }
            }
            Value::String(text) => {
                let trimmed = text.trim();
                if let Ok(uuid) = Uuid::parse_str(trimmed) {
                    builder.push_bind(uuid);
                } else if let Ok(date_time) =
                    NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%dT%H:%M:%S%.f")
                {
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

#[async_trait::async_trait]
impl BulkLoaderTrait for AtsVacancyBulkLoader {
    async fn load(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        result: &SycnTableResult,
    ) -> Result<i64> {
        OltpBulkTableLoader {
            source_table: "warehouse.ats_vacancy_temporary",
            target_table: "vacancy.vacancy",
            column_map: &[("current_hired", "currentHired")],
        }
        .load(tx, result)
        .await
    }
}

#[async_trait::async_trait]
impl BulkLoaderTrait for AtsVacancyCustomFieldBulkLoader {
    async fn load(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        result: &SycnTableResult,
    ) -> Result<i64> {
        OltpBulkTableLoader {
            source_table: "warehouse.ats_vacancy_custom_field",
            target_table: "vacancy.vacancy_custom_field",
            column_map: &[],
        }
        .load(tx, result)
        .await
    }
}

#[async_trait::async_trait]
impl BulkLoaderTrait for AtsVacancyPicBulkLoader {
    async fn load(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        result: &SycnTableResult,
    ) -> Result<i64> {
        OltpBulkTableLoader {
            source_table: "warehouse.ats_vacancy_pic",
            target_table: "vacancy.vacancy_pic",
            column_map: &[],
        }
        .load(tx, result)
        .await
    }
}

#[async_trait::async_trait]
impl BulkLoaderTrait for AtsVacancyRequirementBulkLoader {
    async fn load(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        result: &SycnTableResult,
    ) -> Result<i64> {
        OltpBulkTableLoader {
            source_table: "warehouse.ats_vacancy_requirement",
            target_table: "vacancy.vacancy_requirement",
            column_map: &[],
        }
        .load(tx, result)
        .await
    }
}

#[async_trait::async_trait]
impl BulkLoaderTrait for AtsVacancyStageBulkLoader {
    async fn load(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        result: &SycnTableResult,
    ) -> Result<i64> {
        OltpBulkTableLoader {
            source_table: "warehouse.ats_vacancy_stage",
            target_table: "vacancy.vacancy_stage",
            column_map: &[],
        }
        .load(tx, result)
        .await
    }
}
