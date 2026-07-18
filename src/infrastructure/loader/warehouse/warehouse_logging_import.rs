use std::{collections::HashMap, sync::Arc};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{FromRow, Postgres, QueryBuilder, Transaction, Type};
use uuid::Uuid;

use crate::{
    core::types::{ImportDatasetConfig, ImportTableResult, IMPORT_ENTITY_STATE_RESULT_KEY},
    infrastructure::db::warehouse_client::WarehouseClient,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Type)]
#[sqlx(
    type_name = "import_validation_status",
    rename_all = "SCREAMING_SNAKE_CASE"
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ImportValidationStatus {
    Pending,
    Invalid,
    Valid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Type)]
#[sqlx(type_name = "import_sync_status", rename_all = "SCREAMING_SNAKE_CASE")]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ImportSyncStatus {
    Pending,
    Syncing,
    Sync,
    SyncFailed,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct ImportEntityState {
    pub id: Uuid,
    pub logging_id: Uuid,
    pub entity_table: String,
    pub entity_id: Uuid,

    pub validation_status: ImportValidationStatus,
    pub sync_status: ImportSyncStatus,

    pub errors: Value,
    pub sync_error: Option<String>,
    pub metadata: Value,

    pub validated_at: Option<DateTime<Utc>>,
    pub synced_at: Option<DateTime<Utc>>,

    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// A master temporary row with its optional import state. The state is optional
/// because the detail endpoint must still return a stored import row when its
/// state has not been created yet.
#[derive(Debug, Clone, FromRow)]
pub struct ImportMasterEntityDetail {
    pub entity_id: Uuid,
    pub entity: Value,
    pub validation_status: Option<ImportValidationStatus>,
    pub sync_status: Option<ImportSyncStatus>,
    pub errors: Option<Value>,
    pub sync_error: Option<String>,
    pub metadata: Option<Value>,
    pub validated_at: Option<DateTime<Utc>>,
    pub synced_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct UpsertImportEntityState {
    pub id: Uuid,
    pub logging_id: Uuid,
    pub entity_table: String,
    pub entity_id: Uuid,

    pub validation_status: ImportValidationStatus,
    pub sync_status: ImportSyncStatus,

    pub errors: Value,
    pub sync_error: Option<String>,
    pub metadata: Value,

    pub validated_at: Option<DateTime<Utc>>,
    pub synced_at: Option<DateTime<Utc>>,
}

pub struct WarehouseLoggingImport {
    pub client: Arc<WarehouseClient>,
}

fn quote_identifier(identifier: &str) -> anyhow::Result<String> {
    let valid = !identifier.is_empty()
        && identifier
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_')
        && identifier
            .chars()
            .next()
            .is_some_and(|character| character.is_ascii_alphabetic() || character == '_');

    if !valid {
        anyhow::bail!("invalid SQL identifier '{identifier}'");
    }

    Ok(format!("\"{identifier}\""))
}

fn quote_table(table: &str) -> anyhow::Result<String> {
    let parts = table.split('.').collect::<Vec<_>>();
    if parts.len() != 2 {
        anyhow::bail!("invalid qualified table '{table}'");
    }

    Ok(format!(
        "{}.{}",
        quote_identifier(parts[0])?,
        quote_identifier(parts[1])?
    ))
}

pub fn build_import_entity_states(
    rows: &[HashMap<String, ImportTableResult>],
    logging_id: &str,
    config: &ImportDatasetConfig,
) -> anyhow::Result<Vec<UpsertImportEntityState>> {
    let logging_id = Uuid::parse_str(logging_id)
        .map_err(|error| anyhow::anyhow!("logging_id '{}' is not a UUID: {error}", logging_id))?;
    let entity_table = config.import_config.master_table.clone();
    let mut states = Vec::new();

    for row in rows {
        let master = row.get(&entity_table).ok_or_else(|| {
            anyhow::anyhow!(
                "main table '{}' is missing from import result",
                entity_table
            )
        })?;
        let entity_id_index = master
            .columns
            .iter()
            .position(|column| column == "id")
            .or_else(|| {
                master
                    .columns
                    .iter()
                    .position(|column| column == &config.import_config.primary_column)
            })
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "main table '{}' does not include an id column",
                    entity_table
                )
            })?;
        let errors = row
            .get(IMPORT_ENTITY_STATE_RESULT_KEY)
            .and_then(|state| {
                state
                    .columns
                    .iter()
                    .position(|column| column == "errors")
                    .map(|index| (state, index))
            })
            .and_then(|(state, index)| state.values.first().and_then(|values| values.get(index)))
            .cloned()
            .filter(Value::is_object)
            .unwrap_or_else(|| json!({}));
        let validation_status = if errors.as_object().is_some_and(|errors| !errors.is_empty()) {
            ImportValidationStatus::Invalid
        } else {
            ImportValidationStatus::Valid
        };

        for values in &master.values {
            let entity_id = values
                .get(entity_id_index)
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    anyhow::anyhow!("main table '{}' has an invalid id value", entity_table)
                })
                .and_then(|value| {
                    Uuid::parse_str(value).map_err(|error| {
                        anyhow::anyhow!(
                            "main table '{}' id '{}' is not a UUID: {error}",
                            entity_table,
                            value
                        )
                    })
                })?;

            states.push(UpsertImportEntityState {
                id: Uuid::now_v7(),
                logging_id,
                entity_table: entity_table.clone(),
                entity_id,
                validation_status,
                sync_status: ImportSyncStatus::Pending,
                errors: errors.clone(),
                sync_error: None,
                metadata: json!({}),
                validated_at: Some(Utc::now()),
                synced_at: None,
            });
        }
    }

    Ok(states)
}

impl WarehouseLoggingImport {
    pub fn new(client: Arc<WarehouseClient>) -> Self {
        Self { client }
    }

    pub async fn bulk_upsert(&self, rows: &[UpsertImportEntityState]) -> anyhow::Result<u64> {
        let mut tx = self.client.pool.begin().await?;

        let affected = self.bulk_upsert_with_tx(&mut tx, rows).await?;

        tx.commit().await?;

        Ok(affected)
    }

    /// Marks all master entities for one import logging as successfully synced.
    /// Existing validation errors are retained while old sync errors are cleared.
    pub async fn bulk_mark_sync_success(
        &self,
        logging_id: Uuid,
        entity_table: &str,
    ) -> anyhow::Result<u64> {
        let result = sqlx::query(
            r#"
            UPDATE public.import_entity_states
            SET
                sync_status = $1,
                synced_at = NOW(),
                sync_error = NULL,
                errors = COALESCE(errors, '{}'::jsonb) - 'sync',
                updated_at = NOW()
            WHERE logging_id = $2
                AND entity_table = $3
                AND validation_status = $4
            "#,
        )
        .bind(ImportSyncStatus::Sync)
        .bind(logging_id)
        .bind(entity_table)
        .bind(ImportValidationStatus::Valid)
        .execute(&self.client.pool)
        .await?;

        Ok(result.rows_affected())
    }

    /// Marks all master entities for one import logging as failed. Sync errors
    /// are accumulated in `errors.sync` and mirrored to `sync_error`.
    pub async fn bulk_mark_sync_failed(
        &self,
        logging_id: Uuid,
        entity_table: &str,
        errors: &[String],
    ) -> anyhow::Result<u64> {
        let errors = Value::Array(errors.iter().cloned().map(Value::String).collect());
        let sync_error = errors
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>()
            .join("\n");
        let result = sqlx::query(
            r#"
            UPDATE public.import_entity_states
            SET
                sync_status = $1,
                synced_at = NULL,
                sync_error = $2,
                errors = COALESCE(errors, '{}'::jsonb)
                    || jsonb_build_object(
                        'sync',
                        COALESCE(errors -> 'sync', '[]'::jsonb) || $3::jsonb
                    ),
                updated_at = NOW()
            WHERE logging_id = $4
                AND entity_table = $5
                AND validation_status = $6
            "#,
        )
        .bind(ImportSyncStatus::SyncFailed)
        .bind(sync_error)
        .bind(sqlx::types::Json(errors))
        .bind(logging_id)
        .bind(entity_table)
        .bind(ImportValidationStatus::Valid)
        .execute(&self.client.pool)
        .await?;

        Ok(result.rows_affected())
    }

    /// Invalid import rows must never reach OLTP. Mark them once as failed so
    /// their detail row explains why they were excluded from sync retries.
    pub async fn mark_invalid_as_sync_failed(
        &self,
        logging_id: Uuid,
        entity_table: &str,
    ) -> anyhow::Result<u64> {
        let reason = "Tidak disinkronkan karena validasi import gagal";
        let result = sqlx::query(
            r#"
            UPDATE public.import_entity_states
            SET
                sync_status = $1,
                synced_at = NULL,
                sync_error = $2,
                errors = COALESCE(errors, '{}'::jsonb)
                    || jsonb_build_object('sync', jsonb_build_array($2::text)),
                updated_at = NOW()
            WHERE logging_id = $3
                AND entity_table = $4
                AND validation_status = $5
                AND sync_status <> $1
            "#,
        )
        .bind(ImportSyncStatus::SyncFailed)
        .bind(reason)
        .bind(logging_id)
        .bind(entity_table)
        .bind(ImportValidationStatus::Invalid)
        .execute(&self.client.pool)
        .await?;

        Ok(result.rows_affected())
    }

    pub async fn count_valid_entities(
        &self,
        logging_id: Uuid,
        entity_table: &str,
    ) -> anyhow::Result<i64> {
        sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM public.import_entity_states
            WHERE logging_id = $1
                AND entity_table = $2
                AND validation_status = $3
            "#,
        )
        .bind(logging_id)
        .bind(entity_table)
        .bind(ImportValidationStatus::Valid)
        .fetch_one(&self.client.pool)
        .await
        .map_err(Into::into)
    }

    pub async fn count_invalid_entities(
        &self,
        logging_id: Uuid,
        entity_table: &str,
    ) -> anyhow::Result<i64> {
        sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM public.import_entity_states
            WHERE logging_id = $1
                AND entity_table = $2
                AND validation_status = $3
            "#,
        )
        .bind(logging_id)
        .bind(entity_table)
        .bind(ImportValidationStatus::Invalid)
        .fetch_one(&self.client.pool)
        .await
        .map_err(Into::into)
    }

    /// Return only entity IDs that are explicitly valid for the current
    /// import logging. Missing state is intentionally not treated as valid.
    pub async fn valid_entity_ids(
        &self,
        logging_id: Uuid,
        entity_table: &str,
        entity_ids: &[Uuid],
    ) -> anyhow::Result<Vec<Uuid>> {
        if entity_ids.is_empty() {
            return Ok(Vec::new());
        }

        sqlx::query_scalar(
            r#"
            SELECT entity_id
            FROM public.import_entity_states
            WHERE logging_id = $1
                AND entity_table = $2
                AND validation_status = $3
                AND entity_id = ANY($4)
            "#,
        )
        .bind(logging_id)
        .bind(entity_table)
        .bind(ImportValidationStatus::Valid)
        .bind(entity_ids)
        .fetch_all(&self.client.pool)
        .await
        .map_err(Into::into)
    }

    pub async fn bulk_upsert_with_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        rows: &[UpsertImportEntityState],
    ) -> anyhow::Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }

        let mut query = QueryBuilder::<Postgres>::new(
            r#"
            INSERT INTO public.import_entity_states (
                id,
                logging_id,
                entity_table,
                entity_id,
                validation_status,
                sync_status,
                errors,
                sync_error,
                metadata,
                validated_at,
                synced_at
            )
            "#,
        );

        query.push_values(rows, |mut builder, row| {
            builder
                .push_bind(row.id)
                .push_bind(row.logging_id)
                .push_bind(&row.entity_table)
                .push_bind(row.entity_id)
                .push_bind(row.validation_status)
                .push_bind(row.sync_status)
                .push_bind(&row.errors)
                .push_bind(&row.sync_error)
                .push_bind(&row.metadata)
                .push_bind(row.validated_at)
                .push_bind(row.synced_at);
        });

        query.push(
            r#"
            ON CONFLICT (
                logging_id,
                entity_table,
                entity_id
            )
            DO UPDATE SET
                validation_status = EXCLUDED.validation_status,
                sync_status = EXCLUDED.sync_status,
                errors = EXCLUDED.errors,
                sync_error = EXCLUDED.sync_error,
                metadata = EXCLUDED.metadata,
                validated_at = EXCLUDED.validated_at,
                synced_at = EXCLUDED.synced_at,
                updated_at = NOW()
            "#,
        );

        let result = query.build().execute(&mut **tx).await?;

        Ok(result.rows_affected())
    }

    pub async fn list_by_logging_id(
        &self,
        logging_id: Uuid,
    ) -> anyhow::Result<Vec<ImportEntityState>> {
        sqlx::query_as::<_, ImportEntityState>(
            r#"
            SELECT *
            FROM public.import_entity_states
            WHERE logging_id = $1
            ORDER BY created_at ASC, id ASC
            "#,
        )
        .bind(logging_id)
        .fetch_all(&self.client.pool)
        .await
        .map_err(Into::into)
    }

    /// Fetches rows from the configured master table and left joins their
    /// import state by `(logging_id, entity_table, entity_id)`.
    pub async fn list_master_entity_details(
        &self,
        logging_id: Uuid,
        entity_table: &str,
    ) -> anyhow::Result<Vec<ImportMasterEntityDetail>> {
        let table = quote_table(entity_table)?;
        let sql = format!(
            r#"
            SELECT
                entity."id" AS entity_id,
                to_jsonb(entity) AS entity,
                state.validation_status,
                state.sync_status,
                state.errors,
                state.sync_error,
                state.metadata,
                state.validated_at,
                state.synced_at
            FROM {table} AS entity
            LEFT JOIN public.import_entity_states AS state
                ON state.logging_id = $1
                AND state.entity_table = $2
                AND state.entity_id = entity."id"
            -- Temporary master tables may store logging_id as varchar while
            -- import_entity_states uses UUID. Compare the master value as
            -- text so both storage layouts are supported.
            WHERE entity."logging_id"::text = $1::text
            ORDER BY entity."id" ASC
            "#
        );

        sqlx::query_as::<_, ImportMasterEntityDetail>(&sql)
            .bind(logging_id)
            .bind(entity_table)
            .fetch_all(&self.client.pool)
            .await
            .map_err(Into::into)
    }

    pub async fn fetch_entity_json(
        &self,
        table: &str,
        entity_id: Uuid,
    ) -> anyhow::Result<Option<Value>> {
        let table = quote_table(table)?;
        let sql =
            format!("SELECT to_jsonb(entity) FROM {table} AS entity WHERE entity.\"id\" = $1");

        sqlx::query_scalar::<_, Value>(&sql)
            .bind(entity_id)
            .fetch_optional(&self.client.pool)
            .await
            .map_err(Into::into)
    }

    pub async fn fetch_rows_by_uuid(
        &self,
        table: &str,
        foreign_key: &str,
        entity_id: Uuid,
    ) -> anyhow::Result<Vec<Value>> {
        let table = quote_table(table)?;
        let foreign_key = quote_identifier(foreign_key)?;
        let sql = format!(
            "SELECT to_jsonb(entity) FROM {table} AS entity WHERE entity.{foreign_key} = $1 ORDER BY entity.\"id\" ASC"
        );

        sqlx::query_scalar::<_, Value>(&sql)
            .bind(entity_id)
            .fetch_all(&self.client.pool)
            .await
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        core::traits::orchestrator::ImporterColumn,
        core::{
            traits::orchestrator::ImporterConfig,
            types::{ImportDatasetConfig, TableStrategy, IMPORT_ENTITY_STATE_RESULT_KEY},
        },
    };

    fn config() -> ImportDatasetConfig {
        ImportDatasetConfig {
            where_fields: None,
            import_config: ImporterConfig {
                fields: Vec::<ImporterColumn>::new(),
                chunk: 1,
                tenant_id: Uuid::now_v7().to_string(),
                group_id: Uuid::now_v7().to_string(),
                dataset: "LSI_IMPORT_VACANCY_V1".to_string(),
                auto_sync: false,
                main_filterable: None,
                main_searchable: None,
                key: "test.xlsx".to_string(),
                data_source_type: "buffer".to_string(),
                pipeline: "unused".to_string(),
                loader: "WAREHOUSE_ATS_VACANCY_TEMPORARY".to_string(),
                master_table: "warehouse.ats_vacancy_temporary".to_string(),
                primary_column: "id".to_string(),
                sequential_table: None,
                fetcher_caching: None,
                start_row: None,
                permission_key: None,
            },
        }
    }

    #[test]
    fn records_invalid_state_with_header_keyed_errors() {
        let config = config();
        let entity_id = Uuid::now_v7();
        let logging_id = Uuid::now_v7();
        let mut row = HashMap::new();
        row.insert(
            config.import_config.master_table.clone(),
            ImportTableResult {
                strategy: TableStrategy::Main,
                columns: vec!["id".to_string()],
                values: vec![vec![Value::String(entity_id.to_string())]],
            },
        );
        row.insert(
            IMPORT_ENTITY_STATE_RESULT_KEY.to_string(),
            ImportTableResult {
                strategy: TableStrategy::Eav,
                columns: vec!["errors".to_string()],
                values: vec![vec![json!({"No RR*": ["Required", "must unique"]})]],
            },
        );

        let states = build_import_entity_states(&[row], &logging_id.to_string(), &config).unwrap();

        assert_eq!(states.len(), 1);
        assert_eq!(states[0].logging_id, logging_id);
        assert_eq!(states[0].entity_table, config.import_config.master_table);
        assert_eq!(states[0].entity_id, entity_id);
        assert_eq!(states[0].validation_status, ImportValidationStatus::Invalid);
        assert_eq!(
            states[0].errors,
            json!({"No RR*": ["Required", "must unique"]})
        );
    }
}
