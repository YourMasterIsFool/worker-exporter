use anyhow::Result;
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use uuid::Uuid;

use crate::{
    application::{
        fetch_source_regitry::DataFetcherJsonRegistry, ingestion::source_registry::SourceRegistry,
    },
    core::{
        traits::dataset_trait::SyncDataset,
        types::{
            CastingEnum, SourceMappingTable, SourceSyncConfig, SycnTableResult, SyncPayloadDataset,
            WhereField,
        },
    },
    infrastructure::loader::warehouse::warehouse_logging_import::WarehouseLoggingImport,
};

pub struct LsiVacancySyncDataset {
    pub source: Arc<SourceRegistry>,
    pub fetcher: Arc<DataFetcherJsonRegistry>,
    pub import_logging: Arc<WarehouseLoggingImport>,
}

impl LsiVacancySyncDataset {
    const ENTITY_TABLE: &'static str = "warehouse.ats_vacancy_temporary";
    const OLTP_MANAGED_COLUMNS: &'static [&'static str] =
        &["created_at", "updated_at", "deleted_at", "created_by"];

    /// Mapping antara key internal dan nama fetcher di registry
    fn get_fetcher_map(&self) -> HashMap<String, String> {
        [
            (
                "ATS_VACANCY_CUSTOM_FIELD",
                "WAREHOUSE_ATS_VACANCY_CUSTOM_FIELD",
            ),
            ("ATS_VACANCY_PIC", "WAREHOUSE_ATS_VACANCY_PIC"),
            ("ATS_VACANCY_PUBLISH_ON", "WAREHOUSE_ATS_VACANCY_PUBLISH_ON"),
            (
                "ATS_VACANCY_REQUIREMENT",
                "WAREHOUSE_ATS_VACANCY_REQUIREMENT",
            ),
            ("ATS_VACANCY_STAGE", "WAREHOUSE_ATS_VACANCY_STAGE"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
    }

    fn should_sync_column(table_key: &str, column: &str) -> bool {
        if Self::OLTP_MANAGED_COLUMNS.contains(&column) {
            return false;
        }

        if column == "loggin_id" {
            return false;
        }

        if table_key == "ATS_VACANCY_PIC" && column == "id" {
            return false;
        }

        true
    }

    fn resolve_logging_id(payload: &SyncPayloadDataset, master_rows: &[Value]) -> String {
        if !payload.logging_id.trim().is_empty() {
            return payload.logging_id.clone();
        }

        if let Some(logging_id) = payload.config.logging_id.as_deref() {
            if !logging_id.trim().is_empty() {
                return logging_id.to_string();
            }
        }

        master_rows
            .iter()
            .find_map(|row| row.get("logging_id")?.as_str().map(ToString::to_string))
            .unwrap_or_default()
    }

    fn with_logging_id(rows: &[Value], logging_id: &str) -> Vec<Value> {
        rows.iter()
            .map(|row| match row {
                Value::Object(map) => {
                    let mut normalized = map.clone();

                    if let Some(value) = normalized.remove("loggin_id") {
                        normalized.entry("logging_id".to_string()).or_insert(value);
                    }

                    if !logging_id.trim().is_empty() && !normalized.contains_key("logging_id") {
                        normalized.insert(
                            "logging_id".to_string(),
                            Value::String(logging_id.to_string()),
                        );
                    }

                    Value::Object(normalized)
                }
                _ => row.clone(),
            })
            .collect()
    }

    /// Mengambil daftar nama kolom dari semua row agar kolom penting tidak hilang jika row pertama null/missing.
    fn resolve_columns(&self, table_key: &str, rows: &[Value]) -> Vec<String> {
        let mut seen = HashSet::new();
        let mut columns = Vec::new();

        for row in rows {
            let Some(obj) = row.as_object() else {
                continue;
            };

            for column in obj.keys() {
                if Self::should_sync_column(table_key, column) && seen.insert(column.clone()) {
                    columns.push(column.clone());
                }
            }
        }

        if seen.insert("logging_id".to_string()) {
            columns.push("logging_id".to_string());
        }

        columns
    }

    /// Mengonversi Value array menjadi String matrix yang siap masuk ke DB
    fn resolve_value_matrix(&self, rows: &[Value], columns: &[String]) -> Vec<Vec<Value>> {
        rows.iter()
            .map(|row| match row {
                Value::Object(map) => columns
                    .iter()
                    .map(|column| map.get(column).cloned().unwrap_or(Value::Null))
                    .collect(),
                _ => vec![row.clone()],
            })
            .collect()
    }

    fn resolve_table(
        &self,
        table_key: &str,
        data: &[Value],
        logging_id: &str,
    ) -> anyhow::Result<(Vec<String>, Vec<Vec<serde_json::Value>>)> {
        let rows = Self::with_logging_id(data, logging_id);
        let columns = self.resolve_columns(table_key, &rows);
        let values = self.resolve_value_matrix(&rows, &columns);

        Ok((columns, values))
    }

    async fn only_valid_master_rows(
        &self,
        logging_id: &str,
        master_rows: Vec<Value>,
    ) -> Result<Vec<Value>> {
        let logging_id = Uuid::parse_str(logging_id)?;
        let entity_ids = master_rows
            .iter()
            .filter_map(|row| row.get("id")?.as_str())
            .filter_map(|value| Uuid::parse_str(value).ok())
            .collect::<Vec<_>>();
        let valid_ids = self
            .import_logging
            .valid_entity_ids(logging_id, Self::ENTITY_TABLE, &entity_ids)
            .await?
            .into_iter()
            .collect::<HashSet<_>>();

        Ok(master_rows
            .into_iter()
            .filter(|row| {
                row.get("id")
                    .and_then(Value::as_str)
                    .and_then(|value| Uuid::parse_str(value).ok())
                    .is_some_and(|id| valid_ids.contains(&id))
            })
            .collect())
    }
    /// Resolver utama untuk menarik data dari fetcher berdasarkan ID lowongan
    #[tracing::instrument(
        skip(self, ids, result),
        fields(types = "sync_dataset_fetcher", dataset = "LSI_VACANCY_SYNC_DATASET", vacancy_count = ids.len())
    )]
    async fn resolve_fetcher(
        &self,
        ids: &[String],
        logging_id: &str,
        result: &mut SycnTableResult,
    ) -> Result<()> {
        // insert first master table =
        for (key, fetcher_name) in self.get_fetcher_map() {
            let fetcher = self.fetcher.get(&fetcher_name)?;

            let mut field: String = "vacancy_id".into();
            if fetcher_name == "WAREHOUSE_ATS_VACANCY_CUSTOM_FIELD" {
                field = "parent_id".into();
            }
            let mut filters = vec![WhereField {
                field: field,
                casting: Some(CastingEnum::Uuid),
                operator: "=".into(),
                values: Some(ids.to_vec()),
                value: None,
            }];

            if !logging_id.trim().is_empty() {
                let logging_id_casting = if fetcher_name == "WAREHOUSE_ATS_VACANCY_CUSTOM_FIELD" {
                    Some(CastingEnum::Uuid)
                } else {
                    Some(CastingEnum::Varchar)
                };

                filters.push(WhereField {
                    field: "logging_id".into(),
                    casting: logging_id_casting,
                    operator: "=".into(),
                    values: Some(vec![logging_id.to_string()]),
                    value: None,
                });
            }

            let data = fetcher.fetch(Some(filters)).await?;
            if data.is_empty() {
                continue;
            }

            let (columns, values) = self.resolve_table(&key, &data, logging_id)?;

            // 3. Masukkan ke result
            result.sources.sources.insert(
                key,
                SourceMappingTable {
                    columns,
                    values, // Pastikan SourceMappingTable menggunakan Vec<Vec<Value>>
                },
            );
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl SyncDataset for LsiVacancySyncDataset {
    async fn format(&self, _payload: &SyncPayloadDataset) -> Result<SycnTableResult> {
        todo!("Implementasi format utama")
    }

    #[tracing::instrument(
        skip(self, payload, master_rows),
        fields(types = "sync_dataset", dataset = "LSI_VACANCY_SYNC_DATASET", master_rows = master_rows.len())
    )]
    async fn format_from_master(
        &self,
        payload: &SyncPayloadDataset,
        master_rows: Vec<Value>,
    ) -> Result<SycnTableResult> {
        let logging_id = Self::resolve_logging_id(payload, &master_rows);
        let master_rows = self
            .only_valid_master_rows(&logging_id, master_rows)
            .await?;

        if master_rows.is_empty() {
            return Ok(SycnTableResult {
                config: payload.config.clone(),
                sources: SourceSyncConfig {
                    sources: HashMap::new(),
                },
            });
        }

        let ids: Vec<String> = master_rows
            .iter()
            .filter_map(|r| r.get("id")?.as_str().map(|s| s.to_string()))
            .collect();

        let (master_columns, master_values) =
            self.resolve_table("ATS_VACANCY", &master_rows, &logging_id)?;

        let mut master_mapper: HashMap<String, SourceMappingTable> = HashMap::new();

        master_mapper.insert(
            "ATS_VACANCY".into(),
            SourceMappingTable {
                columns: master_columns,
                values: master_values,
            },
        );

        let mut result = SycnTableResult {
            config: payload.config.clone(),
            sources: SourceSyncConfig {
                sources: master_mapper,
            },
        };

        self.resolve_fetcher(&ids, &logging_id, &mut result).await?;

        Ok(result)
    }
}
