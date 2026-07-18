use bigdecimal::BigDecimal;
use calamine::Data;
use chrono::{NaiveDate, NaiveDateTime, Utc};
use futures_util::future::join_all;
use moka::future::Cache;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tracing::info;
use uuid::Uuid;

use crate::{
    application::fetch_source_regitry::DataFetcherJsonRegistry,
    core::{
        traits::{
            dataset_trait::{ImporterDataset, ImporterDatasetV1},
            orchestrator::{ImporterColumn, ImporterConfig, SequentialTable, TypeDataEnum},
        },
        types::{
            CastingEnum, ImportDatasetConfig, ImportRow, ImportTableResult, TableStrategy,
            WhereField, IMPORT_FETCHER_ERROR_RESULT_KEY,
        },
    },
    utils::{resolve_custom_field_key, resolve_custom_field_typed_value, value::calamine_to_json},
};

use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CustomFields {
    pub id: Uuid,
    pub updated_at: Option<NaiveDateTime>,
    pub created_at: NaiveDateTime,
    pub deleted_at: Option<NaiveDateTime>,
    pub tenant_id: Uuid,
    pub group_id: Uuid,
    pub parent_id: Option<Uuid>,
    pub field_key: String,
    pub field_type: String,
    pub meta: Option<serde_json::Value>,
    pub reference_module: Option<String>,
    pub value_text: Option<String>,
    pub value_number: Option<BigDecimal>,
    pub value_id: Option<Uuid>,
    pub value_bool: Option<bool>,
    pub value_date: Option<NaiveDate>,
}

impl Default for CustomFields {
    fn default() -> Self {
        Self {
            id: Uuid::now_v7(),
            updated_at: None,
            created_at: Utc::now().naive_utc(),
            deleted_at: None,
            tenant_id: Uuid::nil(),
            group_id: Uuid::nil(),
            parent_id: None,
            field_key: String::new(),
            field_type: String::new(),
            meta: None,
            reference_module: None,
            value_text: None,
            value_number: None,
            value_id: None,
            value_bool: None,
            value_date: None,
        }
    }
}

pub struct DynamicDatasetV1 {
    pub fetcher: Arc<DataFetcherJsonRegistry>,
    pub local_cache: Cache<String, serde_json::Value>,
}

impl DynamicDatasetV1 {
    pub fn new(fetcher: Arc<DataFetcherJsonRegistry>) -> Self {
        let local_cache = Cache::builder()
            .time_to_live(Duration::from_secs(600))
            .max_capacity(20_000)
            .build();
        Self {
            fetcher,
            local_cache,
        }
    }
}

impl DynamicDatasetV1 {
    fn normalized_lookup_token(value: &str) -> String {
        value.trim().to_lowercase()
    }

    fn lookup_operator(casting: Option<&CastingEnum>) -> &'static str {
        match casting {
            Some(CastingEnum::Uuid)
            | Some(CastingEnum::Number)
            | Some(CastingEnum::Date)
            | Some(CastingEnum::Enum(_)) => "=",
            Some(CastingEnum::Varchar) | None => "ILIKE",
        }
    }

    async fn resolve_fetcher(
        &self,
        value: &str,
        fetcher_name: &str,
        fetcher_field_key: &str,
        casting: Option<CastingEnum>,
        use_cache: bool,
        cache: &Cache<String, serde_json::Value>,
    ) -> anyhow::Result<Option<serde_json::Value>> {
        // Bersihkan spasi dan normalkan case agar cache lookup tidak
        // membedakan "Sales", "sales", atau "SALES".
        let clean_value = value.trim();
        let clean_fetcher = fetcher_name.trim().to_uppercase();
        let cache_key = format!(
            "{}:{}",
            clean_fetcher,
            Self::normalized_lookup_token(clean_value)
        );
        tracing::info!("Mengecek cache key: {}", cache_key);

        // 1. Jika diperbolehkan di-cache oleh payload JSON, cek cache hit dulu
        if use_cache {
            if let Some(cached_val) = cache.get(&cache_key).await {
                // Ubah ke info agar terlihat jelas di log terminal konsol Anda
                tracing::info!("🔥 CACHE HIT BERHASIL: {}", cache_key);
                return Ok(Some(cached_val));
            }
            tracing::info!("🧊 Cache MISS untuk key: {}", cache_key);
        }

        // 2. Jika cache miss atau tidak masuk whitelist, fetch asli ke DB/gRPC
        let filter = vec![WhereField {
            field: fetcher_field_key.to_string(),
            operator: Self::lookup_operator(casting.as_ref()).into(),
            casting,
            values: Some(vec![clean_value.into()]), // Gunakan clean_value
            value: None,
        }];

        let fetcher = self
            .fetcher
            .get(fetcher_name)
            .map_err(|e| anyhow::anyhow!("Fetcher '{}' tidak ditemukan: {}", fetcher_name, e))?;

        let rows = fetcher.fetch(Some(filter)).await?;

        if let Some(hit_row) = rows.into_iter().next() {
            // 3. Simpan ke cache jika fetcher ini terdaftar di whitelist payload
            if use_cache {
                tracing::info!("💾 Menyimpan ke cache lokal: {}", cache_key);
                cache.insert(cache_key, hit_row.clone()).await;
            }
            return Ok(Some(hit_row));
        }

        Ok(None)
    }
}

#[async_trait::async_trait]
impl ImporterDatasetV1 for DynamicDatasetV1 {
    async fn import(
        &self,
        rows: &[Data],
        config: &ImportDatasetConfig,
        cache: &Cache<String, serde_json::Value>,
    ) -> anyhow::Result<HashMap<String, ImportTableResult>> {
        tracing::info!("Memproses matriks dataset dengan skema penamaan TableStrategy");

        let mut result: HashMap<String, ImportTableResult> = HashMap::new();

        let primary_table = &config.import_config.master_table;
        let primary_column = &config.import_config.primary_column;
        let seq_tables = config.import_config.sequential_table.clone();
        let fields = &config.import_config.fields;
        let tenant_id = Uuid::from_str(&config.import_config.tenant_id)?;

        let cache_whitelist: HashSet<String> = config
            .import_config
            .fetcher_caching
            .clone()
            .unwrap_or_default()
            .into_iter()
            .collect();
        let mut mapping_sequential_table: HashMap<String, SequentialTable> = HashMap::new();
        if let Some(list) = &seq_tables {
            for seq in list {
                mapping_sequential_table.insert(seq.table.clone(), seq.clone());
            }
        }

        let generated_pk = Uuid::now_v7();

        let mut master_row_accumulator: HashMap<String, Value> = HashMap::new();
        let mut sequential_rows_accumulator: HashMap<String, Vec<HashMap<String, Value>>> =
            HashMap::new();
        let mut fetcher_errors = Map::new();

        master_row_accumulator.insert(
            primary_column.clone(),
            Value::String(generated_pk.to_string()),
        );

        // =========================================================
        // STAGE 1: EXTRACT, ASYNC FETCH & SPLIT MULTIPLE TOKENS
        // =========================================================
        for (index, field) in fields.iter().enumerate() {
            // Workbook dapat memiliki kolom lebih sedikit daripada konfigurasi
            // ketika field trailing bersifat opsional. Perlakukan cell yang
            // hilang sebagai NULL agar import tidak panic.
            let cell_data = rows.get(index).map(calamine_to_json).unwrap_or(Value::Null);

            let raw_tokens: Vec<String> = match &cell_data {
                Value::String(s) if s.contains(',') => s
                    .split(',')
                    .map(|t| t.trim().to_string())
                    .filter(|t| !t.is_empty())
                    .collect(),
                Value::String(s) if !s.trim().is_empty() => vec![s.trim().to_string()],
                Value::Null => vec![],
                other => vec![other.to_string()],
            };

            let mut resolved_meta_list: Vec<Value> = Vec::new();
            let mut resolved_clean_values: Vec<Value> = Vec::new();

            if let Some(fetcher_name) = &field.fetcher {
                let use_cache = cache_whitelist.contains(fetcher_name);
                let fetcher_field_key = field
                    .fetcher_field_key
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .unwrap_or("id");
                let fetcher_key = field
                    .fetcher_key
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .unwrap_or("id");
                let casting = field.casting.clone().or_else(|| {
                    fetcher_field_key
                        .eq_ignore_ascii_case("id")
                        .then_some(CastingEnum::Uuid)
                });
                tracing::info!("using is cache {}", use_cache);
                if !raw_tokens.is_empty() {
                    let fetch_futures = raw_tokens.iter().map(|token| {
                        self.resolve_fetcher(
                            token,
                            fetcher_name,
                            fetcher_field_key,
                            casting.clone(),
                            use_cache,
                            cache,
                        )
                    });

                    let fetch_results = join_all(fetch_futures).await;

                    for (idx, res) in fetch_results.into_iter().enumerate() {
                        match res {
                            Ok(Some(meta_obj)) => {
                                resolved_clean_values.push(
                                    meta_obj
                                        .get(fetcher_key)
                                        .cloned()
                                        .unwrap_or_else(|| Value::String(raw_tokens[idx].clone())),
                                );
                                resolved_meta_list.push(meta_obj);
                            }
                            Ok(None) => {
                                let message = Value::String(format!(
                                    "Data fetcher '{}' tidak menemukan nilai '{}'",
                                    fetcher_name, raw_tokens[idx]
                                ));
                                let errors = fetcher_errors
                                    .entry(field.header.clone())
                                    .or_insert_with(|| Value::Array(Vec::new()));
                                if let Value::Array(errors) = errors {
                                    if !errors.contains(&message) {
                                        errors.push(message);
                                    }
                                }
                                tracing::warn!(
                                    header = %field.header,
                                    column = %field.column,
                                    fetcher = %fetcher_name,
                                    lookup_value = %raw_tokens[idx],
                                    "Fetcher tidak menemukan data; field akan menghasilkan NULL"
                                );
                            }
                            Err(e) => return Err(e),
                        }
                    }
                }
            } else {
                resolved_clean_values = raw_tokens.into_iter().map(Value::String).collect();
            }

            let final_data_value = if resolved_clean_values.len() > 1 {
                Value::Array(resolved_clean_values)
            } else {
                resolved_clean_values
                    .into_iter()
                    .next()
                    .unwrap_or(Value::Null)
            };

            let final_meta_value = if resolved_meta_list.len() > 1 {
                Value::Array(resolved_meta_list)
            } else {
                resolved_meta_list.into_iter().next().unwrap_or(Value::Null)
            };

            tracing::info!(
                header = %field.header,
                column = %field.column,
                fetcher = ?field.fetcher,
                raw_value = ?cell_data,
                resolved_value = ?final_data_value,
                resolved_meta = ?final_meta_value,
                "Resolved import field before routing to temporary tables"
            );

            // =========================================================
            // STAGE 2: PIVOTING & ROUTING BERDASARKAN SKEMA STRATEGI
            // =========================================================
            let split_columns = field.column.split('|');

            for group in split_columns {
                if let Some((table, column)) = group.trim().split_once(':') {
                    // Tentukan strategi pemetaan tabel secara langsung dan tegas
                    let strategy = if table == primary_table {
                        TableStrategy::Main
                    } else if let Some(seq) = mapping_sequential_table.get(table) {
                        if seq.is_custom {
                            TableStrategy::Eav
                        } else {
                            TableStrategy::HasMany
                        }
                    } else {
                        continue;
                    };

                    // Jalankan routing berbasis matching enum arsitektur baru
                    match strategy {
                        TableStrategy::Main => {
                            if column.ends_with("_meta") {
                                master_row_accumulator
                                    .insert(column.to_string(), final_meta_value.clone());
                            } else {
                                master_row_accumulator
                                    .insert(column.to_string(), final_data_value.clone());
                            }
                        }
                        TableStrategy::Eav => {
                            let field_type: String = match field.type_data.clone() {
                                Some(val) => val.into(),
                                None => String::new(),
                            };

                            let mut custom_field_row = CustomFields {
                                id: Uuid::now_v7(),
                                meta: Some(final_meta_value.clone()).filter(|v| !v.is_null()),
                                field_key: resolve_custom_field_key(field, column),
                                field_type,
                                group_id: tenant_id,
                                tenant_id,
                                reference_module: field.fetcher.clone(),
                                created_at: Utc::now().naive_utc(),
                                parent_id: Some(generated_pk),
                                updated_at: Some(Utc::now().naive_utc()),
                                ..Default::default()
                            };

                            let typed_value = resolve_custom_field_typed_value(
                                field.type_data.as_ref(),
                                &final_data_value,
                                &final_meta_value,
                                field.fetcher_key.as_deref(),
                            );
                            custom_field_row.value_text = typed_value.value_text;
                            custom_field_row.value_number = typed_value.value_number;
                            custom_field_row.value_id = typed_value.value_id;
                            custom_field_row.value_bool = typed_value.value_bool;
                            custom_field_row.value_date = typed_value.value_date;

                            let entries = sequential_rows_accumulator
                                .entry(table.to_string())
                                .or_default();
                            let mut row_obj = HashMap::new();
                            row_obj.insert(
                                "value".to_string(),
                                serde_json::to_value(custom_field_row)?,
                            );
                            entries.push(row_obj);
                        }
                        TableStrategy::HasMany => {
                            let seq = mapping_sequential_table.get(table).unwrap();
                            let tokens_to_process = match &final_data_value {
                                Value::Array(arr) => arr.clone(),
                                Value::Null => vec![],
                                other => vec![other.clone()],
                            };

                            let entries = sequential_rows_accumulator
                                .entry(table.to_string())
                                .or_default();

                            if entries.is_empty() && !tokens_to_process.is_empty() {
                                for _ in 0..tokens_to_process.len() {
                                    let mut new_row = HashMap::new();
                                    new_row.insert(
                                        "id".to_string(),
                                        Value::String(Uuid::now_v7().to_string()),
                                    );
                                    new_row.insert(
                                        seq.fk_column.clone(),
                                        Value::String(generated_pk.to_string()),
                                    );
                                    entries.push(new_row);
                                }
                            }

                            for (idx, token_val) in tokens_to_process.into_iter().enumerate() {
                                if let Some(row_object) = entries.get_mut(idx) {
                                    if column.ends_with("_meta") {
                                        let meta_val = match &final_meta_value {
                                            Value::Array(arr) => {
                                                arr.get(idx).cloned().unwrap_or(Value::Null)
                                            }
                                            other => other.clone(),
                                        };
                                        row_object.insert(column.to_string(), meta_val);
                                    } else {
                                        row_object.insert(column.to_string(), token_val);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // =========================================================
        // STAGE 3: EXPORT KE STRUKTUR MATRIKS + METADATA STRATEGY
        // =========================================================

        // 1. Pack Master Table Result
        let mut master_columns: Vec<String> = master_row_accumulator.keys().cloned().collect();
        master_columns.sort();
        let master_values: Vec<Value> = master_columns
            .iter()
            .map(|col| {
                master_row_accumulator
                    .get(col)
                    .cloned()
                    .unwrap_or(Value::Null)
            })
            .collect();

        result.insert(
            primary_table.to_string(),
            ImportTableResult {
                strategy: TableStrategy::Main, // 🔥 Menyimpan tipe data enum Main
                columns: master_columns,
                values: vec![master_values],
            },
        );

        // 2. Pack All Sequential & Custom (EAV) Tables Result
        for (table_name, grouped_rows) in sequential_rows_accumulator {
            if grouped_rows.is_empty() {
                continue;
            }

            // Dapatkan kembali strategi penulisan tabel dinamis saat ini
            let current_strategy = if mapping_sequential_table
                .get(&table_name)
                .map(|s| s.is_custom)
                .unwrap_or(false)
            {
                TableStrategy::Eav
            } else {
                TableStrategy::HasMany
            };

            let mut seq_columns: Vec<String> =
                HashSet::<String>::from_iter(grouped_rows.iter().flat_map(|m| m.keys().cloned()))
                    .into_iter()
                    .collect();
            seq_columns.sort();

            let mut matrix_values: Vec<Vec<Value>> = Vec::new();
            for row_map in grouped_rows {
                let row_values: Vec<Value> = seq_columns
                    .iter()
                    .map(|col| row_map.get(col).cloned().unwrap_or(Value::Null))
                    .collect();
                matrix_values.push(row_values);
            }

            result.insert(
                table_name,
                ImportTableResult {
                    strategy: current_strategy, // 🔥 Menyimpan tipe data enum HasMany atau Eav
                    columns: seq_columns,
                    values: matrix_values,
                },
            );
        }

        if !fetcher_errors.is_empty() {
            result.insert(
                IMPORT_FETCHER_ERROR_RESULT_KEY.to_string(),
                ImportTableResult {
                    strategy: TableStrategy::Eav,
                    columns: vec!["errors".to_string()],
                    values: vec![vec![Value::Object(fetcher_errors)]],
                },
            );
        }

        Ok(result)
    }
}
