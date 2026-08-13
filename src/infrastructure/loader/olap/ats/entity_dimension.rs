use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, Result};
use chrono::{DateTime, NaiveDateTime, Utc};
use serde_json::Value;
use sqlx::{FromRow, Postgres, QueryBuilder};
use uuid::Uuid;

use crate::{
    core::traits::load::IncrementalLoadPersitence,
    infrastructure::{
        db::warehouse_client::WarehouseClient, loader::oltp::ats::entity_mapping::AtsEntityMapping,
    },
};

struct EntityDimension {
    source_key: String,
    source_parent_id: Option<Uuid>,
    source_tenant_id: Option<Uuid>,
    source_group_id: Option<Uuid>,
    source_created_at: Option<DateTime<Utc>>,
    source_updated_at: DateTime<Utc>,
    source_data: Value,
}

#[derive(FromRow)]
struct CurrentEntityDimension {
    source_key: String,
    source_updated_at: DateTime<Utc>,
}

const BATCH_SIZE: usize = 500;

impl EntityDimension {
    fn from_row(mapping: &AtsEntityMapping, value: &Value) -> Result<Self> {
        let object = value
            .as_object()
            .ok_or_else(|| anyhow!("{} dimension source must be an object", mapping.entity))?;
        let source_key = mapping
            .identity_columns
            .iter()
            .map(|column| {
                object
                    .get(*column)
                    .filter(|value| !value.is_null())
                    .map(|value| {
                        value
                            .as_str()
                            .map(ToString::to_string)
                            .unwrap_or_else(|| value.to_string())
                    })
                    .ok_or_else(|| {
                        anyhow!("{} is missing identity column {column}", mapping.entity)
                    })
            })
            .collect::<Result<Vec<_>>>()?
            .join(":");
        let source_created_at = Self::timestamp(object.get("created_at"))?;
        let source_updated_at = Self::timestamp(object.get("updated_at"))?
            .or(source_created_at)
            .or_else(|| Self::uuid_timestamp(object.get("id")))
            .ok_or_else(|| {
                anyhow!(
                    "{} requires updated_at, created_at, or UUIDv7 id",
                    mapping.entity
                )
            })?;

        Ok(Self {
            source_key,
            source_parent_id: mapping
                .parent_column
                .and_then(|column| Self::uuid(object.get(column))),
            source_tenant_id: Self::uuid(object.get("tenant_id")),
            source_group_id: Self::uuid(object.get("group_id")),
            source_created_at,
            source_updated_at,
            source_data: value.clone(),
        })
    }

    fn uuid(value: Option<&Value>) -> Option<Uuid> {
        value
            .and_then(Value::as_str)
            .and_then(|value| Uuid::parse_str(value).ok())
    }

    fn uuid_timestamp(value: Option<&Value>) -> Option<DateTime<Utc>> {
        let uuid = Self::uuid(value)?;
        let (seconds, nanos) = uuid.get_timestamp()?.to_unix();
        DateTime::from_timestamp(seconds as i64, nanos)
    }

    fn timestamp(value: Option<&Value>) -> Result<Option<DateTime<Utc>>> {
        let Some(value) = value.and_then(Value::as_str) else {
            return Ok(None);
        };

        chrono::DateTime::parse_from_rfc3339(value)
            .map(|value| Some(value.with_timezone(&Utc)))
            .or_else(|_| {
                NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f")
                    .map(|value| Some(value.and_utc()))
            })
            .or_else(|_| {
                NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
                    .map(|value| Some(value.and_utc()))
            })
            .map_err(Into::into)
    }
}

pub struct AtsEntityDimensionLoader {
    pub client: Arc<WarehouseClient>,
    pub mapping: &'static AtsEntityMapping,
}

impl AtsEntityDimensionLoader {
    fn table(&self) -> String {
        format!("olap_ats.{}_dimensions", self.mapping.entity)
    }
}

#[async_trait::async_trait]
impl IncrementalLoadPersitence for AtsEntityDimensionLoader {
    async fn fetch_max_source_updated_at(&self) -> Result<Option<NaiveDateTime>> {
        let query = format!("SELECT MAX(source_updated_at) FROM {}", self.table());
        Ok(sqlx::query_scalar::<_, Option<DateTime<Utc>>>(&query)
            .fetch_one(&self.client.pool)
            .await?
            .map(|updated_at| updated_at.naive_utc()))
    }

    async fn load(&self, rows: &[Value]) -> Result<i64> {
        if rows.is_empty() {
            return Ok(0);
        }

        let table = self.table();
        let dimensions = rows
            .iter()
            .map(|row| EntityDimension::from_row(self.mapping, row))
            .collect::<Result<Vec<_>>>()?;
        let mut tx = self.client.pool.begin().await?;
        let mut loaded = 0_i64;

        for chunk in dimensions.chunks(BATCH_SIZE) {
            let source_keys = chunk
                .iter()
                .map(|row| row.source_key.clone())
                .collect::<Vec<_>>();
            let current = sqlx::query_as::<_, CurrentEntityDimension>(&format!(
                "SELECT source_key, source_updated_at FROM {table} \
                 WHERE source_key = ANY($1) AND is_current FOR UPDATE"
            ))
            .bind(&source_keys)
            .fetch_all(&mut *tx)
            .await?
            .into_iter()
            .map(|row| (row.source_key, row.source_updated_at))
            .collect::<HashMap<_, _>>();
            let pending = chunk
                .iter()
                .filter(|row| {
                    current
                        .get(&row.source_key)
                        .is_none_or(|updated_at| *updated_at < row.source_updated_at)
                })
                .collect::<Vec<_>>();
            let existing = pending
                .iter()
                .filter(|row| current.contains_key(&row.source_key))
                .collect::<Vec<_>>();

            if !existing.is_empty() {
                sqlx::query(&format!(
                    "UPDATE {table} AS dimension \
                     SET is_current = false, end_from = updates.source_updated_at \
                     FROM UNNEST($1::text[], $2::timestamptz[]) \
                        AS updates(source_key, source_updated_at) \
                     WHERE dimension.source_key = updates.source_key AND dimension.is_current"
                ))
                .bind(
                    existing
                        .iter()
                        .map(|row| row.source_key.clone())
                        .collect::<Vec<_>>(),
                )
                .bind(
                    existing
                        .iter()
                        .map(|row| row.source_updated_at)
                        .collect::<Vec<_>>(),
                )
                .execute(&mut *tx)
                .await?;
            }

            if pending.is_empty() {
                continue;
            }

            let mut insert = QueryBuilder::<Postgres>::new(format!(
                "INSERT INTO {table} (\
                    id, source_key, source_parent_id, source_tenant_id, source_group_id,\
                    source_created_at, source_updated_at, source_data, start_from, is_current\
                 ) "
            ));
            insert.push_values(&pending, |mut builder, row| {
                builder
                    .push_bind(Uuid::now_v7())
                    .push_bind(&row.source_key)
                    .push_bind(row.source_parent_id)
                    .push_bind(row.source_tenant_id)
                    .push_bind(row.source_group_id)
                    .push_bind(row.source_created_at)
                    .push_bind(row.source_updated_at)
                    .push_bind(sqlx::types::Json(row.source_data.clone()))
                    .push_bind(row.source_updated_at)
                    .push_bind(true);
            });
            insert.build().execute(&mut *tx).await?;
            loaded += pending.len() as i64;
        }

        tx.commit().await?;
        Ok(loaded)
    }
}

#[cfg(test)]
mod tests {
    use super::EntityDimension;
    use crate::infrastructure::loader::oltp::ats::entity_mapping::VACANCY_FORM_OPTION_SNAPSHOT;

    #[test]
    fn derives_form_option_timestamp_from_its_uuidv7_id() {
        let id = uuid::Uuid::now_v7();
        let value = serde_json::json!({
            "id": id,
            "vacancy_form_snapshot_id": "018f55c3-4a5d-7d8e-8f9a-b0c1d2e3f401",
            "sequence": 1,
            "option_value": "Yes",
            "is_correct_answer": true
        });

        let dimension = EntityDimension::from_row(&VACANCY_FORM_OPTION_SNAPSHOT, &value).unwrap();
        assert_eq!(dimension.source_key, id.to_string());
    }
}
