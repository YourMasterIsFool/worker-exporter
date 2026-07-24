use std::{collections::HashSet, sync::Arc};

use moka::future::Cache;
use serde_json::{json, Value};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::{
    application::{
        fetch_source_regitry::DataFetcherJsonRegistry, ingestion::source_registry::SourceRegistry,
    },
    core::{
        traits::{dataset_trait::Dataset, load::LoadEnum, orchestrator::ExporterColumn},
        types::{ExportRow, WhereField},
    },
    utils::{
        export_value_to_string, fetch_many_with_optional_cache,
        group_custom_field_values_by_parent, index_rows_by_string, resolve_export_value,
        resolved_custom_field_value, unique_string_field_values, uuid_values_filter,
    },
};

const APPLICANT_BUCKET_SOURCE: &str = "oltp_ats_applicant_bucket";
const APPLICANT_SNAPSHOT_FETCHER: &str = "ats_applicant_snapshot_fetcher";
const VACANCY_FETCHER: &str = "ats_vacancy_fetcher";
const VACANCY_CUSTOM_FIELD_FETCHER: &str = "OLTP_VACANCY_CUSTOM_FIELD";
const JOB_GRADE_FETCHER: &str = "JOB_GRADE";
const WORK_LOCATION_FETCHER: &str = "MASTER_GEO";
const JOB_GRADE_FIELD_KEY: &str = "jobGradeId";

/// Export dataset for applicant bucket rows.
///
/// The applicant bucket is the page/cursor source. Related data is resolved in
/// batches from the applicant snapshot and vacancy fetchers, avoiding one
/// database request per applicant row.
pub struct ApplicantDataset {
    pub source: Arc<SourceRegistry>,
    pub fetcher: Arc<DataFetcherJsonRegistry>,
    pub export_cache: Arc<RwLock<Option<Cache<String, Value>>>>,
    pub cached_fetchers: Arc<RwLock<HashSet<String>>>,
}

#[async_trait::async_trait]
impl Dataset for ApplicantDataset {
    async fn get_min_cursor(&self, filters: Option<Vec<WhereField>>) -> anyhow::Result<String> {
        let _ = filters;
        // The source resolver uses `id > cursor`. Starting from UUID nil keeps
        // the first applicant bucket row in the export result.
        Ok(Uuid::nil().to_string())
    }

    async fn configure_export(
        &self,
        fields: &[ExporterColumn],
        cache: &Cache<String, Value>,
    ) -> anyhow::Result<()> {
        let fetchers = fields
            .iter()
            .filter(|field| field.is_cache)
            .filter_map(|field| field.fetcher.as_deref())
            .map(|fetcher| fetcher.to_uppercase())
            .collect::<HashSet<_>>();

        *self.export_cache.write().await = Some(cache.clone());
        *self.cached_fetchers.write().await = fetchers;
        Ok(())
    }

    async fn fetch_batch(
        &self,
        strategy_enum: &LoadEnum,
        cursor: Option<String>,
        limit: i64,
    ) -> anyhow::Result<(Vec<ExportRow>, Option<String>)> {
        let cache = self.export_cache.read().await.clone();
        let cached_fetchers = self.cached_fetchers.read().await.clone();
        let (filters, columns) = match strategy_enum {
            LoadEnum::Select(filters, columns) => {
                (filters.clone(), columns.clone().unwrap_or_default())
            }
            _ => (None, Vec::new()),
        };

        let cursor = cursor
            .map(|cursor| {
                Uuid::parse_str(&cursor)
                    .map_err(|error| anyhow::anyhow!("cursor applicant bucket invalid: {error}"))
            })
            .transpose()?;

        let source = self.source.get(APPLICANT_BUCKET_SOURCE)?;
        let source_strategy = LoadEnum::Select(filters, None);
        let (buckets, next_cursor) = source.extract(cursor, limit, &source_strategy).await?;

        if buckets.is_empty() {
            return Ok((Vec::new(), next_cursor.map(|cursor| cursor.to_string())));
        }

        let bucket_ids = unique_string_field_values(&buckets, "id");
        let vacancy_ids = unique_string_field_values(&buckets, "vacancy_id");

        let vacancy_custom_field_fetcher = self.fetcher.get(VACANCY_CUSTOM_FIELD_FETCHER)?;

        let snapshots = if bucket_ids.is_empty() {
            Vec::new()
        } else {
            self.fetch_rows(
                APPLICANT_SNAPSHOT_FETCHER,
                bucket_ids,
                "applicant_bucket_id",
                "applicant_bucket_id",
                cached_fetchers.contains(&APPLICANT_SNAPSHOT_FETCHER.to_uppercase()),
                cache.as_ref(),
            )
            .await?
        };

        let vacancies = if vacancy_ids.is_empty() {
            Vec::new()
        } else {
            self.fetch_rows(
                VACANCY_FETCHER,
                vacancy_ids.clone(),
                "id",
                "id",
                cached_fetchers.contains(&VACANCY_FETCHER.to_uppercase()),
                cache.as_ref(),
            )
            .await?
        };

        let vacancy_custom_fields = if vacancy_ids.is_empty() {
            Vec::new()
        } else {
            vacancy_custom_field_fetcher
                .fetch(Some(vec![
                    uuid_values_filter("parent_id", vacancy_ids),
                    WhereField {
                        field: "field_key".to_string(),
                        operator: "=".to_string(),
                        value: Some(JOB_GRADE_FIELD_KEY.to_string()),
                        values: None,
                        casting: Some(crate::core::types::CastingEnum::Varchar),
                    },
                ]))
                .await?
        };

        let job_grade_ids =
            unique_custom_field_value_ids(&vacancy_custom_fields, JOB_GRADE_FIELD_KEY);
        let work_location_ids = unique_string_field_values(&vacancies, "work_location_id");

        let job_grades = if job_grade_ids.is_empty() {
            Vec::new()
        } else {
            self.fetch_rows(
                JOB_GRADE_FETCHER,
                job_grade_ids,
                "id",
                "id",
                cached_fetchers.contains(&JOB_GRADE_FETCHER.to_uppercase()),
                cache.as_ref(),
            )
            .await?
        };

        let work_locations = if work_location_ids.is_empty() {
            Vec::new()
        } else {
            self.fetch_rows(
                WORK_LOCATION_FETCHER,
                work_location_ids,
                "id",
                "id",
                cached_fetchers.contains(&WORK_LOCATION_FETCHER.to_uppercase()),
                cache.as_ref(),
            )
            .await?
        };

        let snapshots_by_bucket = index_rows_by_string(&snapshots, "applicant_bucket_id");
        let vacancies_by_id = index_rows_by_string(&vacancies, "id");
        let job_grades_by_id = index_rows_by_string(&job_grades, "id");
        let work_locations_by_id = index_rows_by_string(&work_locations, "id");
        let custom_fields_by_vacancy = group_custom_field_values_by_parent(&vacancy_custom_fields);
        let empty = Value::Null;

        let rows = buckets
            .iter()
            .map(|bucket| {
                let bucket_id = bucket.get("id").and_then(Value::as_str).unwrap_or_default();
                let vacancy_id = bucket
                    .get("vacancy_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let snapshot = snapshots_by_bucket
                    .get(bucket_id)
                    .copied()
                    .unwrap_or(&empty);
                let candidate_data = snapshot.get("candidate_data").unwrap_or(&empty);
                let vacancy = vacancies_by_id.get(vacancy_id).copied().unwrap_or(&empty);
                let vacancy_custom_fields =
                    custom_fields_by_vacancy.get(vacancy_id).unwrap_or(&empty);
                let work_location_id = vacancy
                    .get("work_location_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let work_location = work_locations_by_id
                    .get(work_location_id)
                    .copied()
                    .unwrap_or(&empty);
                let job_grade_id = vacancy_custom_fields
                    .get(JOB_GRADE_FIELD_KEY)
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let job_grade = job_grades_by_id
                    .get(job_grade_id)
                    .copied()
                    .unwrap_or(&empty);
                let candidate_id = candidate_data
                    .get("id")
                    .cloned()
                    .or_else(|| bucket.get("candidate_id").cloned())
                    .unwrap_or(Value::Null);
                let candidate = json!({"id": candidate_id});
                let candidate_custom_fields = candidate_data.get("customFields").unwrap_or(&empty);
                let sources = [
                    ("candidate", &candidate),
                    ("candidate_data", candidate_data),
                    ("customFields", candidate_custom_fields),
                    ("applicant_bucket", bucket),
                    ("applicant_snapshot", snapshot),
                    ("vacancy", vacancy),
                    ("workLocation", work_location),
                    ("work_location", work_location),
                    ("jobGrade", job_grade),
                    ("job_grade", job_grade),
                    ("vacancyCustomFields", vacancy_custom_fields),
                    ("vacancy_custom_fields", vacancy_custom_fields),
                ];

                ExportRow {
                    fields: columns
                        .iter()
                        .map(|column| {
                            export_value_to_string(resolve_export_value(column, &sources))
                        })
                        .collect(),
                }
            })
            .collect();

        Ok((rows, next_cursor.map(|cursor| cursor.to_string())))
    }
}

impl ApplicantDataset {
    async fn fetch_rows(
        &self,
        fetcher_name: &str,
        requested_values: Vec<String>,
        fetcher_field_key: &str,
        fetcher_key: &str,
        use_cache: bool,
        cache: Option<&Cache<String, Value>>,
    ) -> anyhow::Result<Vec<Value>> {
        let fetcher = self.fetcher.get(fetcher_name).map_err(|error| {
            anyhow::anyhow!("Fetcher '{}' tidak ditemukan: {}", fetcher_name, error)
        })?;

        fetch_many_with_optional_cache(
            fetcher.as_ref(),
            fetcher_name,
            requested_values,
            fetcher_field_key,
            fetcher_key,
            Some(crate::core::types::CastingEnum::Uuid),
            use_cache,
            cache,
        )
        .await
    }
}

fn unique_custom_field_value_ids(rows: &[Value], field_key: &str) -> Vec<String> {
    rows.iter()
        .filter(|row| row.get("field_key").and_then(Value::as_str) == Some(field_key))
        .filter_map(resolved_custom_field_value)
        .filter_map(Value::as_str)
        .map(ToOwned::to_owned)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect()
}
