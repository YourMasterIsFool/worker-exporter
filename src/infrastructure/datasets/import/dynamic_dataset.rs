use bigdecimal::BigDecimal;
use calamine::Data;
use chrono::{NaiveDate, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::info;
use uuid::Uuid;

use crate::{
    application::fetch_source_regitry::DataFetcherJsonRegistry,
    core::{
        traits::{
            dataset_trait::ImporterDataset,
            orchestrator::{ImporterColumn, ImporterConfig, SequentialTable, TypeDataEnum},
        },
        types::{CastingEnum, ImportDatasetConfig, ImportRow, WhereField},
    },
    utils::calamine_to_json,
};
use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    str::FromStr,
    sync::Arc,
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
            tenant_id: Uuid::nil(), // Berikan nilai default yang aman
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

pub struct DynamicDataset {
    pub fetcher: Arc<DataFetcherJsonRegistry>,
}

impl DynamicDataset {
    // NOTE: `fetcher.fetch(...)` mengembalikan `anyhow::Result<Vec<Value>>` (list baris),
    // bukan `Value` tunggal. Karena filternya `id = value`, hasilnya logisnya hanya 0
    // atau 1 baris, jadi kita ambil elemen pertama saja. Return type diubah jadi
    // `Option<Value>` supaya caller bisa membedakan "tidak ketemu" vs "error".
    async fn resolve_fetcher(
        &self,
        value: &str,
        fetcher_name: &str,
    ) -> anyhow::Result<Option<serde_json::Value>> {
        let filter = vec![WhereField {
            field: "id".into(),
            operator: "=".into(),
            casting: Some(CastingEnum::Uuid),
            values: Some(vec![value.into()]),
            value: None,
        }];

        // 1. Mengambil fetcher
        let fetcher = self
            .fetcher
            .get(fetcher_name)
            .map_err(|e| anyhow::anyhow!("Fetcher '{}' tidak ditemukan: {}", fetcher_name, e))?;

        // 2. Melakukan fetch dan menangani error secara langsung
        let rows = fetcher.fetch(Some(filter)).await.map_err(|e| {
            tracing::error!("Gagal fetch resolve fetcher {}: {:?}", fetcher_name, e);
            e // Mengembalikan error asli (asumsi fetcher.fetch mengembalikan anyhow::Result)
        })?;

        Ok(rows.into_iter().next())
    }
}

fn value_to_uuid(value: &Value) -> Option<Uuid> {
    // 1. Ambil sebagai string
    // 2. Coba parse menggunakan library uuid
    value.as_str().and_then(|s| Uuid::parse_str(s).ok())
}

fn value_to_text(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        // Untuk tipe data Array atau Object, kita konversi menjadi JSON String
        other => Some(other.to_string()),
    }
}

// NOTE: `cf` sekarang bertipe `&mut CustomFields` (sebelumnya `&mut VacancyCustomField`,
// yang tidak konsisten dengan tipe object yang sebenarnya dibuat di `import()`).
// NOTE: `val` sekarang bertipe `&Value` (serde_json), bukan `&Data` (calamine),
// karena data sudah dikonversi lewat `calamine_to_json` sebelum fungsi ini dipanggil.
fn map_value_to_field(field_type: Option<TypeDataEnum>, val: &Value, cf: &mut CustomFields) {
    match field_type {
        Some(TypeDataEnum::Uuid) => cf.value_id = value_to_uuid(val),
        Some(TypeDataEnum::Number) => {
            // 1. Coba ambil langsung sebagai f64 jika JSON bertipe Number
            // 2. Jika gagal (berupa Value::String), ambil str-nya lalu parse manual ke f64
            let numeric_val = val
                .as_f64()
                .or_else(|| val.as_str().and_then(|s| s.trim().parse::<f64>().ok()));

            // Ubah f64 menjadi BigDecimal untuk kebutuhan struct database
            cf.value_number = numeric_val.and_then(|f| BigDecimal::try_from(f).ok());
        }
        Some(TypeDataEnum::Boolean) => cf.value_bool = val.as_bool(),
        Some(TypeDataEnum::Date) => {
            cf.value_date = val
                .as_str()
                .and_then(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d").ok());
        }
        Some(TypeDataEnum::Text) | None => {
            cf.value_text = value_to_text(val);
        }
    }
}

#[async_trait::async_trait]
impl ImporterDataset for DynamicDataset {
    async fn import(
        &self,
        rows: &[Data],
        config: &ImportDatasetConfig,
    ) -> anyhow::Result<HashMap<String, Vec<ImportRow>>> {
        tracing::info!("row data {:?}", rows[0]);

        let mut result: HashMap<String, Vec<ImportRow>> = HashMap::new();
        // TODO: trait `ImporterDataset::import` saat ini hanya mengembalikan
        // `HashMap<String, Vec<ImportRow>>`, jadi custom fields yang dibangun di
        // bawah tidak ikut dikembalikan ke caller. Ditampung di sini agar tidak
        // hilang begitu saja, tapi Anda perlu memutuskan: ubah signature trait
        // menjadi tuple `(HashMap<String, Vec<ImportRow>>, HashMap<String, Vec<CustomFields>>)`,
        // atau langsung persist ke storage di titik ini.
        let mut custom_fields_result: HashMap<String, Vec<CustomFields>> = HashMap::new();

        let primary_table = &config.import_config.master_table;
        let primary_column = &config.import_config.primary_column;
        let seq_tables = config.import_config.sequential_table.clone();
        let fields = &config.import_config.fields;
        let tenant_id = Uuid::from_str(&config.import_config.tenant_id)?;

        // mapping into map because wanna O(1)
        let mut mapping_sequential_table: HashMap<String, SequentialTable> = HashMap::new();

        if let Some(list) = &seq_tables {
            for seq in list {
                mapping_sequential_table.insert(seq.table.clone(), seq.clone());
            }
        }

        let _is_custom_table: HashSet<String> = seq_tables
            .clone()
            .unwrap_or_default() // Jika None, anggap sebagai vector kosong
            .iter()
            .filter(|f| f.is_custom)
            .map(|f| f.table.clone())
            .collect();

        let generated_pk = Uuid::now_v7();

        // loop columns
        for (index, field) in fields.iter().enumerate() {
            let data = calamine_to_json(&rows[index]);

            let splitted_data = data.as_str().unwrap_or_default().split(",");

            for split in splitted_data {}

            // warehouse.vacancy:name | warehouse.vacancy_id:id
            // -> ["warehouse.vacancy:name", "warehouse.vacancy_id:id"]
            let split_columns = field.column.split('|');

            // resolve fetcher
            let mut meta_resolver: Option<serde_json::Value> = None;
            if let Some(fetcher) = &field.fetcher {
                let data_str = data
                    .as_str()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| data.to_string());
                meta_resolver = self.resolve_fetcher(&data_str, fetcher).await?;
            }

            info!("meta resolver {:?}", meta_resolver);

            for group in split_columns {
                // split "table:column" -> (table, column)
                if let Some((table, column)) = group.trim().split_once(':') {
                    // set primary column into id;
                    if table == primary_table {
                        let import_row = ImportRow {
                            field: primary_column.clone(),
                            value: Value::String(generated_pk.to_string()),
                            fetcher: None,
                            type_data: Some(TypeDataEnum::Uuid),
                        };
                        result
                            .entry(table.to_string())
                            .or_insert_with(Vec::new)
                            .push(import_row);
                    }

                    // if mapping sequential
                    //

                    if let Some(seq) = mapping_sequential_table.get(table) {
                        // if custom
                        if seq.is_custom {
                            let field_type: String = match field.type_data.clone() {
                                Some(val) => val.into(),
                                None => String::new(),
                            };

                            // mapping into custom
                            let mut custom_field_row = CustomFields {
                                id: Uuid::now_v7(),
                                meta: meta_resolver.clone(),
                                field_key: column.to_string(),
                                field_type,
                                group_id: tenant_id,
                                tenant_id,
                                reference_module: field.fetcher.clone(),
                                deleted_at: None,
                                created_at: Utc::now().naive_utc(),
                                parent_id: Some(generated_pk),
                                updated_at: Some(Utc::now().naive_utc()),
                                ..Default::default()
                            };

                            // edit data
                            map_value_to_field(
                                field.type_data.clone(),
                                &data,
                                &mut custom_field_row,
                            );

                            result
                                .entry(table.to_string())
                                .or_insert_with(Vec::new)
                                .push(ImportRow {
                                    field: column.to_string(),
                                    value: serde_json::to_value(custom_field_row)?,
                                    fetcher: field.fetcher.clone(),
                                    type_data: field.type_data.clone(),
                                });
                        } else {
                            let value: serde_json::Value = serde_json::to_value(&generated_pk)
                                .unwrap_or(serde_json::Value::Null);
                            let import_row: ImportRow = ImportRow {
                                fetcher: field.fetcher.clone(),
                                field: seq.fk_column.clone(),
                                type_data: field.type_data.clone(),
                                value,
                            };

                            let generated_id = Uuid::now_v7();
                            let value: serde_json::Value = serde_json::to_value(&generated_id)
                                .unwrap_or(serde_json::Value::Null);

                            // generated id for id
                            let generated_id_import_row: ImportRow = ImportRow {
                                fetcher: None,
                                field: "id".into(),
                                type_data: Some(TypeDataEnum::Uuid),
                                value: value,
                            };

                            result
                                .entry(table.to_string())
                                .or_insert_with(Vec::new)
                                .push(import_row);
                            result
                                .entry(table.to_string())
                                .or_insert_with(Vec::new)
                                .push(generated_id_import_row);
                        }
                    }

                    let import_row: ImportRow = if column.ends_with("_meta") {
                        ImportRow {
                            fetcher: field.fetcher.clone(),
                            field: column.to_string(),
                            type_data: field.type_data.clone(),
                            value: meta_resolver.clone().unwrap_or_else(|| Value::Null),
                        }
                    } else {
                        ImportRow {
                            field: column.to_string(),
                            value: data.clone(),
                            fetcher: field.fetcher.clone(),
                            type_data: field.type_data.clone(),
                        }
                    };

                    result
                        .entry(table.to_string())
                        .or_insert_with(Vec::new)
                        .push(import_row);
                }
            }
        }

        let _ = custom_fields_result; // lihat TODO di atas

        Ok(result)
    }
}
