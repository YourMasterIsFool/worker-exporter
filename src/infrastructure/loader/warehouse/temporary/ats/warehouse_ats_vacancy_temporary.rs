use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use crate::{
    core::{
        traits::load::{
            ImportLoadPersitence, IncrementalLoadPersitence, LoadEnum, LoaderPersitence,
        },
        // Pastikan mengimpor TableStrategy dan ImportTableResult dari types Anda
        types::{ImportDatasetConfig, ImportTableResult, TableStrategy},
    },
    domain::logging,
    infrastructure::{
        db::warehouse_client::WarehouseClient, ingestion::master,
        loader::warehouse::types::WarehouseTemporaryTable,
    },
    utils::{build_query, reformated_value},
};
use anyhow::Result;

use bigdecimal::BigDecimal;
use chrono::{NaiveDate, NaiveDateTime};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{Postgres, QueryBuilder};
use tracing::info;
use uuid::Uuid;

// Struct model db tetap dipertahankan
#[derive(Debug, Serialize, Deserialize, sqlx::FromRow, Clone)]
pub struct VacancyCustomField {
    pub id: Uuid,
    pub updated_at: Option<NaiveDateTime>,
    pub created_at: NaiveDateTime,
    pub deleted_at: Option<NaiveDateTime>,
    pub tenant_id: Uuid,
    pub group_id: Uuid,
    pub parent_id: Option<Uuid>,
    pub field_key: String,
    pub field_type: String,
    pub meta: Option<Value>,
    pub reference_module: Option<String>,
    pub value_text: Option<String>,
    pub value_number: Option<BigDecimal>,
    pub value_id: Option<Uuid>,
    pub value_bool: Option<bool>,
    pub value_date: Option<NaiveDate>,
}

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow, Clone)]
pub struct VacancyStage {
    pub id: Uuid,
    pub updated_at: Option<NaiveDateTime>,
    pub created_at: NaiveDateTime,
    pub deleted_at: Option<NaiveDateTime>,
    pub vacancy_id: Uuid,
    pub stage_name: String,
    pub stage_type: String,
    pub is_default: bool,
    pub stage_email_template_id: Option<Uuid>,
    pub stage_email_template_reject: Option<Uuid>,
    pub sequence: i32,
    pub is_locked: bool,
    pub form_template_id: Option<Uuid>,
    pub share_access: Value,
    pub logging_id: Option<String>,
}

pub struct WarehouseAtsVacancyTemporaryLoader {
    pub client: Arc<WarehouseClient>,
}

impl WarehouseAtsVacancyTemporaryLoader {
    fn is_uuid_column(col_name: &str) -> bool {
        matches!(
            col_name,
            "id" | "tenant_id"
                | "group_id"
                | "parent_id"
                | "created_by"
                | "job_position_id"
                | "organization_id"
                | "employment_status_id"
                | "work_placement_type_id"
                | "work_location_id"
                | "province_id"
                | "city_id"
                | "vacancy_id"
                | "employee_id"
                | "stage_email_template_id"
                | "stage_email_template_reject"
                | "form_template_id"
                | "requirement_stage_template_id"
                | "recruitment_form_template_id"
                | "pre_onboarding_template_id"
        ) || col_name.ends_with("_id")
    }

    /// Binder cerdas untuk mencocokkan tipe data primitive JSON ke driver Postgres (SQLx)
    fn bind_dynamic_value<'a>(
        &self,
        b: &mut sqlx::query_builder::Separated<'a, '_, Postgres, &str>,
        val: &Value,
        col_name: &str,
    ) {
        if col_name == "keywords" {
            if let Value::Array(arr) = val {
                let keywords = arr
                    .iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect::<Vec<_>>();

                b.push_bind(keywords);
                return;
            }
        }
        match val {
            Value::Null => {
                if Self::is_uuid_column(col_name) {
                    b.push_bind(None::<Uuid>);
                } else {
                    b.push_bind(None::<String>);
                }
            }
            Value::Bool(boolean) => {
                b.push_bind(*boolean);
            }
            Value::Number(num) => {
                if let Some(i) = num.as_i64() {
                    b.push_bind(i);
                } else if let Some(f) = num.as_f64() {
                    b.push_bind(f);
                } else {
                    b.push_bind(num.to_string());
                }
            }
            Value::String(s) => {
                let trimmed = s.trim();

                if Self::is_uuid_column(col_name) {
                    b.push_bind(Uuid::parse_str(trimmed).ok());
                }
                // 1. Coba deteksi jika string sebenarnya adalah Uuid
                else if let Ok(uuid_val) = Uuid::parse_str(trimmed) {
                    b.push_bind(uuid_val);
                }
                // 2. Coba parse string ke i64 jika kolom database butuh Integer
                else if let Ok(i) = trimmed.parse::<i64>() {
                    b.push_bind(i);
                }
                // 3. Coba parse string ke f64 jika berupa angka desimal (seperti "2.0")
                else if let Ok(f) = trimmed.parse::<f64>() {
                    if f.fract() == 0.0 {
                        b.push_bind(f as i64);
                    } else {
                        b.push_bind(f);
                    }
                }
                // 4. Fallback terakhir sebagai teks biasa
                else {
                    b.push_bind(s.clone());
                }
            }
            Value::Object(_) | Value::Array(_) => {
                b.push_bind(val.clone());
            }
        }
    }
}

impl LoaderPersitence for WarehouseAtsVacancyTemporaryLoader {
    fn table(&self) -> &'static str {
        WarehouseTemporaryTable::AtsVacancyTemporary.table_name()
    }

    fn columns(&self) -> &'static [&'static str] {
        &["id", "name", "description", "logging_id"]
    }

    fn conflicted_columns(&self) -> &'static [&'static str] {
        &[""]
    }
}

#[async_trait::async_trait]
impl ImportLoadPersitence for WarehouseAtsVacancyTemporaryLoader {
    #[tracing::instrument(
        skip(self, rows, config),
        fields(types = "warehouse_ats_vacancy_temporary_loader", logging_id = %logging_id)
    )]
    async fn load(
        &self,
        rows: &[HashMap<String, ImportTableResult>],
        logging_id: &str,
        config: &ImportDatasetConfig,
    ) -> Result<i64> {
        if rows.is_empty() {
            return Ok(0);
        }

        info!(
            "Memulai proses loading dengan format matriks terpadu strategi enum. Sampel baris 1: {:?}",
            rows[0]
        );
        let master_table = &config.import_config.master_table;
        let seq_tables = &config.import_config.sequential_table;

        let mut tx = self.client.pool.begin().await?;
        let mut master_affected_rows = 0;

        // =========================================================
        // STAGE 1: BULK INSERT MASTER TABLE (Menggunakan Matrix)
        // =========================================================
        for row_map in rows {
            if let Some(table_data) = row_map.get(master_table) {
                // 🔥 Menggunakan TableStrategy::Main untuk validasi
                if table_data.strategy == TableStrategy::Main && !table_data.values.is_empty() {
                    let mut qb: QueryBuilder<Postgres> =
                        QueryBuilder::new(format!("INSERT INTO {} (", master_table));
                    let mut separated = qb.separated(", ");
                    for col in &table_data.columns {
                        separated.push(col);
                    }
                    qb.push(") ");

                    // Direct binding dari 2D Vector values secara linear!
                    qb.push_values(&table_data.values, |mut b, row_values| {
                        master_affected_rows += 1;
                        for (index, val) in row_values.iter().enumerate() {
                            self.bind_dynamic_value(&mut b, val, &table_data.columns[index]);
                        }
                    });

                    if let Err(e) = qb.build().execute(&mut *tx).await {
                        tracing::error!(
                            logging_id = logging_id,
                            "Gagal bulk insert master table '{}': {:?}",
                            master_table,
                            e
                        );
                        tx.rollback().await?;
                        return Err(anyhow::anyhow!(
                            "Gagal insert master table '{}': {:?}",
                            master_table,
                            e
                        ));
                    }
                }
            }
        }

        if master_affected_rows == 0 {
            tracing::warn!(
                logging_id = logging_id,
                "Tidak ada data master table '{}' yang valid untuk di-insert, rolling back.",
                master_table
            );
            tx.rollback().await?;
            return Ok(0);
        }

        // =========================================================
        // STAGE 2: SEQUENTIAL TABLES INSERT (BERURUTAN)
        // =========================================================
        if let Some(sequential_list) = seq_tables {
            for seq in sequential_list {
                let target_table = &seq.table;

                for row_map in rows {
                    if let Some(table_data) = row_map.get(target_table) {
                        if table_data.values.is_empty() {
                            continue;
                        }

                        // 🔥 MENGGUNAKAN PATTERN MATCHING BERDASARKAN STRATEGY ENUM
                        match table_data.strategy {
                            // --- KASUS A: CUSTOM FIELD (EAV MODEL) ---
                            TableStrategy::Eav => {
                                let mut custom_fields_to_insert: Vec<VacancyCustomField> =
                                    Vec::new();

                                for row_values in &table_data.values {
                                    if let Some(first_val) = row_values.first() {
                                        if let Ok(cf) = serde_json::from_value::<VacancyCustomField>(
                                            first_val.clone(),
                                        ) {
                                            custom_fields_to_insert.push(cf);
                                        } else {
                                            tracing::warn!(
                                                logging_id = logging_id,
                                                "Gagal memetakan Object JSON ke VacancyCustomField pada tabel kustom '{}'",
                                                target_table
                                            );
                                        }
                                    }
                                }

                                if !custom_fields_to_insert.is_empty() {
                                    let mut qb_custom = QueryBuilder::new(format!(
                                        "INSERT INTO {} (id, tenant_id, group_id, parent_id, field_key, field_type, meta, reference_module, value_text, value_number, value_id, value_bool, value_date, created_at, updated_at) ",
                                        target_table
                                    ));

                                    qb_custom.push_values(&custom_fields_to_insert, |mut b, cf| {
                                        b.push_bind(cf.id)
                                            .push_bind(cf.tenant_id)
                                            .push_bind(cf.group_id)
                                            .push_bind(cf.parent_id)
                                            .push_bind(cf.field_key.clone())
                                            .push_bind(cf.field_type.clone())
                                            .push_bind(cf.meta.clone())
                                            .push_bind(cf.reference_module.clone())
                                            .push_bind(cf.value_text.clone())
                                            .push_bind(cf.value_number.clone())
                                            .push_bind(cf.value_id)
                                            .push_bind(cf.value_bool)
                                            .push_bind(cf.value_date)
                                            .push_bind(cf.created_at)
                                            .push_bind(cf.updated_at);
                                    });

                                    if let Err(e) = qb_custom.build().execute(&mut *tx).await {
                                        tracing::error!(
                                            logging_id = logging_id,
                                            "Gagal bulk insert ke custom table '{}': {:?}",
                                            target_table,
                                            e
                                        );
                                        tx.rollback().await?;
                                        return Err(anyhow::anyhow!(
                                            "Gagal bulk insert ke custom table '{}': {:?}",
                                            target_table,
                                            e
                                        ));
                                    }
                                }
                            }
                            // --- KASUS B: RELASIONAL SEKUENSIAL BIASA (ex: ats_vacancy_pic) ---
                            TableStrategy::HasMany => {
                                let mut qb_seq: QueryBuilder<Postgres> =
                                    QueryBuilder::new(format!("INSERT INTO {} (", target_table));
                                let mut separated_seq = qb_seq.separated(", ");
                                for col in &table_data.columns {
                                    separated_seq.push(col);
                                }
                                qb_seq.push(") ");

                                // Masukkan matrix nilai 2D values (multiple rows token) secara direct linear!
                                qb_seq.push_values(&table_data.values, |mut b, row_values| {
                                    for (index, val) in row_values.iter().enumerate() {
                                        self.bind_dynamic_value(
                                            &mut b,
                                            val,
                                            &table_data.columns[index],
                                        );
                                    }
                                });

                                if let Err(e) = qb_seq.build().execute(&mut *tx).await {
                                    tracing::error!(
                                        logging_id = logging_id,
                                        "Gagal bulk insert sequential table '{}': {:?}",
                                        target_table,
                                        e
                                    );
                                    tx.rollback().await?;
                                    return Err(anyhow::anyhow!(
                                        "Gagal insert sequential table '{}': {:?}",
                                        target_table,
                                        e
                                    ));
                                }
                            }
                            // --- KASUS C: BACKUP SAFE GUARD (ANOMALI DATA MAIN NYASAR DI SEQUENTIAL) ---
                            TableStrategy::Main => {}
                        }
                    }
                }
            }
        }

        tx.commit().await?;

        tracing::info!(
            logging_id = logging_id,
            "Berhasil melakukan transaksi bulk insert {} baris ke master table '{}'",
            master_affected_rows,
            master_table
        );

        Ok(master_affected_rows)
    }
}
