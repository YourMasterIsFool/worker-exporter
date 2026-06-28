use std::{
    collections::{HashMap, VecDeque},
    hash::Hash,
    sync::{Arc, WaitTimeoutResult},
};

use crate::{
    application::grpcs::facilitator::S3FacilitatorGrpc,
    core::traits::extractor::incremental_source::WhereField as CoreWhereField,
    infrastructure::ingestion::ats::{
        applicant::applicant_bucket,
        candidate::{candidate, candidate_client_field, education},
        vacancy::vacancy,
    },
};

use anyhow::anyhow;
use bigdecimal::BigDecimal;
use chrono::{format::Numeric, NaiveDateTime};
use serde::{Deserialize, Serialize};
use sqlx::{query::Query, QueryBuilder};
use tracing::info;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct LsiCandidateDatasetRow {
    pub candidate_id: String,
    pub created_at: String,

    pub vacancy_name: Option<String>,
    pub date_applied: String,
    pub level: String,
    pub institute_name: Option<String>,
    pub identity_number_encrypt: Option<String>,
    pub email_address: Option<String>,
    pub phone_number: Option<String>,
    pub name: Option<String>,
    pub gender: Option<String>,
    pub education: Option<String>,
    pub major: Option<String>,
    pub start_date: String,
    pub end_date: String,
    pub academic_score: Option<String>,
    pub birth_place: Option<String>,
    pub dob: String,
    pub religion: String,
    pub marital_status: Option<String>,
    pub height: i16,
    pub weight: i16,
    pub nationality: Option<String>,
    pub ktp_street: Option<String>,
    pub ktp_province_id: Option<String>,
    pub ktp_city_id: Option<String>,
    pub ktp_district_id: Option<String>,
    pub ktp_sub_district_id: Option<String>,
    pub ktp_postal_code: Option<String>,
    pub domicile_street: Option<String>,
    pub domicile_province_id: Option<String>,
    pub domicile_city_id: Option<String>,
    pub domicile_district_id: Option<String>,
    pub domicile_sub_district_id: Option<String>,
    pub domicile_postal_code: Option<String>,
    pub company_name: Option<String>,
    pub industry: Option<String>,
    pub job_title: Option<String>,
    pub job_description: Option<String>,
    pub job_start_date: Option<String>,
    pub job_end_date: Option<String>,
    pub other_internal_family_name: Option<String>,
    pub other_internal_family_relationship: Option<String>,
    pub other_internal_family_position: Option<String>,
    pub other_internal_family_branch: Option<String>,
    pub other_internal_family_branch_location: Option<String>,
}
use crate::{
    application::{
        fetch_source_regitry::DataFetcherJsonRegistry, ingestion::source_registry::SourceRegistry,
    },
    core::{
        datasets::dataset_trait::Dataset,
        traits::{
            load::LoadEnum,
            strategy::{self, registry::Registry},
            writter::writter_trait::ExportRow,
        },
        types::WhereField,
    },
    infrastructure::{
        db::ats_client::AtsClient,
        ingestion::ats::{candidate::experient, types},
        warehouse::snapshots::organization::tenants::query::TenantPoolQuerySnapshot,
    },
};

#[derive(Clone)]
pub struct LsiCandidateDataset {
    pub client: Arc<AtsClient>,
    pub source: Arc<SourceRegistry>,
    pub data_fetcher: Arc<DataFetcherJsonRegistry>,
}

impl LsiCandidateDataset {
    pub fn get_map_custom_field(
        &self,
        key: &str,
        mapper: &HashMap<String, serde_json::Value>,
    ) -> String {
        let mut result = String::new();
        if let Some(value) = mapper.get(key) {
            if let Some(value_text) = value.get("value_text") {
                if let Some(text) = value_text.as_str() {
                    result = text.to_string();
                }
            } else if let Some(value_number) = value.get("value_number") {
                if let Some(number) = value_number.as_str() {
                    result = number.to_string();
                }
            } else if let Some(value_date) = value.get("value_date") {
                if let Some(text) = value_date.as_str() {
                    result = text.to_string();
                }
            } else if let Some(value_id) = value.get("value_id") {
                if let Some(id) = value_id.as_str() {
                    result = id.to_string();
                }
            }
        }

        result
    }

    pub fn mapper_custom_fileds(
        &self,
        map: &mut HashMap<String, serde_json::Value>,
        data: &Vec<serde_json::Value>,
    ) {
        for key in data {
            if let (Some(candidate_id), Some(key_field)) =
                (key.get("parent_id"), key.get("field_key"))
            {
                map.insert(format!("{}_{}", candidate_id, key_field), key.clone());
            }
        }
    }
}

#[async_trait::async_trait]
impl Dataset for LsiCandidateDataset {
    async fn get_min_cursor(&self, filters: Option<Vec<WhereField>>) -> anyhow::Result<String> {
        let sql = format!(" SELECT id from {} ", "applicant.applicant_bucket");
        let mut qb = QueryBuilder::new(&sql);

        if let Some(filters) = filters {
            qb.push(" WHERE 1=1 ");
            for field in filters {
                qb.push(format!(" AND {} {} ", field.field, field.operator))
                    .push_bind(field.value);
            }
        }

        qb.push(" ORDER BY id ASC limit 1");

        let result = qb
            .build_query_scalar::<Uuid>()
            .fetch_one(&self.client.pool)
            .await?;

        Ok(result.to_string())
    }

    async fn fetch_batch(
        &self,
        strategy_enum: &LoadEnum,
        cursor: Option<String>,
        limit: i64,
    ) -> anyhow::Result<(Vec<ExportRow>, Option<String>)> {
        let mut columns = vec![];
        let mut map_master_data = HashMap::<String, String>::new();
        let mut map_custom_fields = HashMap::<String, serde_json::Value>::new();

        if let LoadEnum::Select(f, c) = strategy_enum {
            if let Some(c) = c {
                columns = c.clone();
            }
        }

        let candidate_fetcher = self.data_fetcher.get("ats_candidate_fetcher")?;
        let applicants_source = self.source.get("oltp_ats_applicant_bucket")?;
        let vacancy_fetcher = self.data_fetcher.get("ats_vacancy_fetcher")?;
        // let candidate_field_source = self.source.get("oltp_ats_candidate_client_field")?;
        // let education_source = self.source.get("oltp_ats_education_source")?;
        let education_fetcher = self.data_fetcher.get("ats_education_fetcher")?;
        let candidate_field_fetcher = self.data_fetcher.get("ats_candidate_field_fetcher")?;
        let experient_fetcher = self.data_fetcher.get("ats_experience_fetcher")?;
        // let experient_source = self.source.get("oltp_ats_experience_source")?;
        // let geo_fetcher = self.data_fetcher.get("master_geo_fetcher")?;
        let master_items_fetcher = self.data_fetcher.get("master_master_items_fetcher")?;

        let mut master_data_fetch: HashMap<String, String> = HashMap::new();
        let mut geo_master: HashMap<String, String> = HashMap::new();

        let mut cursor_uuid = None;
        if let Some(c) = cursor {
            match Uuid::parse_str(&c) {
                Ok(u) => cursor_uuid = Some(u),
                Err(_) => return Err(anyhow!("Gagal Parsing data")),
            }
        }

        // get data applicant first
        let (applicants, next_cursor) = applicants_source
            .extract(cursor_uuid, limit, strategy_enum)
            .await?;

        // get data candidate
        let candidate_ids: Vec<String> = applicants
            .iter()
            .filter_map(|f| {
                // .get("candidate_id") mengembalikan Option<&Value>
                // .as_str() mengambil &str tanpa tanda kutip tambahan
                // .map(ToString::to_string) mengubah &str menjadi String
                f.get("candidate_id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .collect();
        let vacancy_ids: Vec<String> = applicants
            .iter()
            .filter_map(|f| {
                // .get("vacancy_id") mengembalikan Option<&Value>
                // .as_str() mengambil &str tanpa tanda kutip tambahan
                // .map(ToString::to_string) mengubah &str menjadi String
                f.get("vacancy_id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            })
            .collect();
        let candidate_filter = vec![WhereField {
            field: "id".into(),
            operator: "=".into(),
            values: Some(candidate_ids.clone()),
            value: None,
        }];

        let education_filter = vec![WhereField {
            field: "candidate_id".into(),
            operator: "=".into(),
            values: Some(candidate_ids.clone()),
            value: None,
        }];

        let candidate_client_filter = vec![WhereField {
            field: "parent_id".into(),
            operator: "=".into(),
            values: Some(candidate_ids.clone()),
            value: None,
        }];
        let vacancy_filter = vec![WhereField {
            field: "id".into(),
            operator: "=".into(),
            values: Some(vacancy_ids),
            value: None,
        }];
        let experient_filter = vec![WhereField {
            field: "candidate_id".into(),
            operator: "=".into(),
            values: Some(candidate_ids.clone()),
            value: None,
        }];
        let candidate_fetcher_data = candidate_fetcher.fetch(Some(candidate_filter)).await?;
        let education_fetcher_data = education_fetcher.fetch(Some(education_filter)).await?;
        let experient_fetcher_data = experient_fetcher.fetch(Some(experient_filter)).await?;
        let candidate_client_fetcher_data = candidate_field_fetcher
            .fetch(Some(candidate_client_filter))
            .await?;
        let vacancy_fetcher_data = vacancy_fetcher.fetch(Some(vacancy_filter)).await?;

        let vacancy_mapper_data: HashMap<String, String> = vacancy_fetcher_data
            .iter()
            .map(|v| {
                let id = v.get("id").and_then(|v| v.as_str()).unwrap_or("");
                let name = v.get("name").and_then(|v| v.as_str()).unwrap_or("");
                (id.to_string(), name.to_string())
            })
            .collect();
        self.mapper_custom_fileds(&mut map_custom_fields, &candidate_client_fetcher_data);

        // get master gender;
        let mut master_ids = Vec::new();
        let mut master_geo_ids = Vec::new();
        let candidate_mapper_data = candidate_fetcher_data
            .iter()
            .map(|c| {
                let id = c.get("id").and_then(|v| v.as_str()).unwrap_or("");
                (id.to_string(), c.clone())
            })
            .collect::<HashMap<String, _>>();
        for candidate in &candidate_fetcher_data {
            if let Some(id) = candidate.get("gender").and_then(|v| v.as_str()) {
                if Uuid::parse_str(id).is_ok() {
                    master_ids.push(id.to_string());
                } else {
                    master_data_fetch.insert(id.to_string(), id.to_string());
                }
            }

            if let Some(id) = candidate.get("marital_status").and_then(|v| v.as_str()) {
                if Uuid::parse_str(id).is_ok() {
                    master_ids.push(id.to_string());
                } else {
                    master_data_fetch.insert(id.to_string(), id.to_string());
                }
            }

            if let Some(id) = candidate.get("religion").and_then(|v| v.as_str()) {
                if Uuid::parse_str(id).is_ok() {
                    master_ids.push(id.to_string());
                } else {
                    master_data_fetch.insert(id.to_string(), id.to_string());
                }
            }

            if let Some(id) = candidate.get("ktp_province_id").and_then(|v| v.as_str()) {
                if Uuid::parse_str(id).is_ok() {
                    master_geo_ids.push(id.to_string());
                } else {
                    geo_master.insert(id.to_string(), id.to_string());
                }
            }

            if let Some(id) = candidate.get("ktp_city_id").and_then(|v| v.as_str()) {
                if Uuid::parse_str(id).is_ok() {
                    master_geo_ids.push(id.to_string());
                } else {
                    geo_master.insert(id.to_string(), id.to_string());
                }
            }
            if let Some(id) = candidate.get("ktp_district_id").and_then(|v| v.as_str()) {
                if Uuid::parse_str(id).is_ok() {
                    master_geo_ids.push(id.to_string());
                } else {
                    geo_master.insert(id.to_string(), id.to_string());
                }
            }
            if let Some(id) = candidate
                .get("ktp_sub_district_id")
                .and_then(|v| v.as_str())
            {
                if Uuid::parse_str(id).is_ok() {
                    master_geo_ids.push(id.to_string());
                } else {
                    geo_master.insert(id.to_string(), id.to_string());
                }
            }
            if let Some(id) = candidate.get("domicile_street").and_then(|v| v.as_str()) {
                if Uuid::parse_str(id).is_ok() {
                    master_geo_ids.push(id.to_string());
                } else {
                    geo_master.insert(id.to_string(), id.to_string());
                }
            }
            if let Some(id) = candidate
                .get("domicile_province_id")
                .and_then(|v| v.as_str())
            {
                if Uuid::parse_str(id).is_ok() {
                    master_geo_ids.push(id.to_string());
                } else {
                    geo_master.insert(id.to_string(), id.to_string());
                }
            }
            if let Some(id) = candidate.get("domicile_city_id").and_then(|v| v.as_str()) {
                if Uuid::parse_str(id).is_ok() {
                    master_geo_ids.push(id.to_string());
                } else {
                    geo_master.insert(id.to_string(), id.to_string());
                }
            }
            if let Some(id) = candidate
                .get("domicile_sub_district_id")
                .and_then(|v| v.as_str())
            {
                if Uuid::parse_str(id).is_ok() {
                    master_geo_ids.push(id.to_string());
                } else {
                    geo_master.insert(id.to_string(), id.to_string());
                }
            }
            if let Some(id) = candidate.get("domicile_city_id").and_then(|v| v.as_str()) {
                if Uuid::parse_str(id).is_ok() {
                    master_geo_ids.push(id.to_string());
                } else {
                    geo_master.insert(id.to_string(), id.to_string());
                }
            }
            if let Some(id) = candidate
                .get("domicile_district_id")
                .and_then(|v| v.as_str())
            {
                if Uuid::parse_str(id).is_ok() {
                    master_geo_ids.push(id.to_string());
                } else {
                    geo_master.insert(id.to_string(), id.to_string());
                }
            }
            if let Some(id) = candidate
                .get("domicile_sub_district_id")
                .and_then(|v| v.as_str())
            {
                if Uuid::parse_str(id).is_ok() {
                    master_geo_ids.push(id.to_string());
                } else {
                    geo_master.insert(id.to_string(), id.to_string());
                }
            }

            if let Some(id) = candidate
                .get("domicile_sub_district_id")
                .and_then(|v| v.as_str())
            {
                if Uuid::parse_str(id).is_ok() {
                    master_geo_ids.push(id.to_string());
                } else {
                    geo_master.insert(id.to_string(), id.to_string());
                }
            }
        }

        for education in &education_fetcher_data {
            if let Some(id) = education.get("major").and_then(|v| v.as_str()) {
                if Uuid::parse_str(id).is_ok() {
                    master_ids.push(id.to_string());
                } else {
                    master_data_fetch.insert(id.to_string(), id.to_string());
                }
            }
            if let Some(id) = education.get("institute_name").and_then(|v| v.as_str()) {
                if Uuid::parse_str(id).is_ok() {
                    master_ids.push(id.to_string());
                } else {
                    master_data_fetch.insert(id.to_string(), id.to_string());
                }
            }
        }

        info!("master geo ids {:?}", master_geo_ids.clone().len());
        let master_items_filter = Some(vec![WhereField {
            field: "id".to_string(),
            operator: "=".into(), // Gunakan IN
            value: None,
            values: Some(master_ids),
        }]);

        let education_mapper_data = education_fetcher_data
            .iter()
            .map(|c| {
                let id = c.get("candidate_id").and_then(|v| v.as_str()).unwrap_or("");
                (id.to_string(), c.clone())
            })
            .collect::<HashMap<String, _>>();

        let experient_mapper_data = experient_fetcher_data
            .iter()
            .map(|c| {
                let id = c.get("candidate_id").and_then(|v| v.as_str()).unwrap_or("");
                (id.to_string(), c.clone())
            })
            .collect::<HashMap<String, _>>();

        let list_master_data = master_items_fetcher.fetch(master_items_filter).await?;

        for master in &list_master_data {
            // Gunakan as_str() untuk key dan name agar bersih dari tanda kutip
            if let (Some(id), Some(name)) = (
                master.get("id").and_then(|v| v.as_str()),
                master.get("name").and_then(|v| v.as_str()),
            ) {
                map_master_data.insert(id.to_string(), name.to_string());
            }
        }
        info!("master data bang {:?}", list_master_data);
        let next_cursor = next_cursor.map(|c| c.to_string());

        let mut rows: Vec<LsiCandidateDatasetRow> = vec![];

        for applicant in &applicants {
            let candidate_id = applicant
                .get("candidate_id")
                .and_then(|v| v.as_str())
                .unwrap_or("-")
                .to_string();
            let mut vacancy_name = String::from("-");
            let applicant_date = applicant
                .get("created_at")
                .and_then(|v| v.as_str())
                .unwrap_or("-");

            let candidate_map = candidate_mapper_data
                .get(
                    applicant
                        .get("candidate_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or(""),
                )
                .cloned()
                .unwrap_or_default();
            let created_at = candidate_map
                .get("created_at")
                .and_then(|v| v.as_str())
                .unwrap_or("-");

            let email = candidate_map
                .get("email")
                .and_then(|v| v.as_str())
                .unwrap_or("-");

            let mut fullname = String::from("-");

            if let Some(first_name) = candidate_map.get("first_name") {
                if let Some(first_name_str) = first_name.as_str() {
                    fullname.push_str(first_name_str);
                }
            }
            if let Some(last_name) = candidate_map.get("last_name") {
                if let Some(last_name_str) = last_name.as_str() {
                    fullname.push_str(" ");
                    fullname.push_str(last_name_str);
                }
            }

            let candidate_education = education_mapper_data
                .get(
                    applicant
                        .get("candidate_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or(""),
                )
                .cloned()
                .unwrap_or_default();
            let candidate_gender = candidate_map
                .get("gender")
                .and_then(|v| v.as_str())
                .unwrap_or("-");

            let academic_score = candidate_education
                .get("academic_score")
                .and_then(|v| v.as_str())
                .unwrap_or("-")
                .to_string();

            let candidate_level = candidate_education
                .get("level")
                .and_then(|v| v.as_str())
                .unwrap_or("-");

            let candidate_education_start = candidate_education
                .get("start_date")
                .and_then(|v| v.as_str())
                .unwrap_or("-")
                .to_string();

            let candidate_education_end = candidate_education
                .get("end_date")
                .and_then(|v| v.as_str())
                .unwrap_or("-")
                .to_string();

            let candidate_major = candidate_education
                .get("major")
                .and_then(|v| v.as_str())
                .unwrap_or("-");

            let candidate_institute = candidate_education
                .get("institute_name")
                .and_then(|v| v.as_str())
                .unwrap_or("-");

            let candidate_level = candidate_education
                .get("level")
                .and_then(|v| v.as_str())
                .unwrap_or("-");

            let gender = map_master_data
                .get(candidate_gender)
                .cloned()
                .unwrap_or("-".to_string());

            let phone_number = candidate_map
                .get("phone_number")
                .and_then(|v| v.as_str())
                .unwrap_or("-");

            let education = map_master_data
                .get(candidate_level)
                .cloned()
                .unwrap_or("-".to_string());
            let major = map_master_data
                .get(candidate_major)
                .cloned()
                .unwrap_or("-".to_string());

            let education_level = map_master_data
                .get(candidate_level)
                .cloned()
                .unwrap_or("-".to_string());
            let institute_name = map_master_data
                .get(candidate_institute)
                .cloned()
                .unwrap_or("-".to_string());

            if let Some(vacancy_id) = applicant.get("vacancy_id") {
                if let Some(parse_vacancy_id) = vacancy_id.as_str() {
                    vacancy_name = vacancy_mapper_data
                        .get(parse_vacancy_id)
                        .cloned()
                        .unwrap_or_else(|| "-".to_string());
                }
            }

            let birth_place = candidate_map
                .get("birth_place")
                .and_then(|v| v.as_str())
                .unwrap_or("-")
                .to_string();

            let candidate_id = candidate_map
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or("-")
                .to_string();

            let weight = map_custom_fields
                .get(&format!("{}_{}", candidate_id, "weight"))
                .and_then(|v| v.as_i64())
                .unwrap_or(0) as i16;
            let height = map_custom_fields
                .get(&format!("{}_{}", candidate_id, "height"))
                .and_then(|v| v.as_i64())
                .unwrap_or(0) as i16;
            let nationality = map_custom_fields
                .get(&format!("{}_{}", candidate_id, "nationality"))
                .and_then(|v| v.as_str())
                .unwrap_or("-")
                .to_string();

            let other_internal_family_name = map_custom_fields
                .get(&format!(
                    "{}_{}",
                    candidate_id, "other_internal_family_name"
                ))
                .and_then(|v| v.as_str())
                .unwrap_or("-")
                .to_string();
            let other_internal_family_relationship = map_custom_fields
                .get(&format!(
                    "{}_{}",
                    candidate_id, "other_internal_family_relationship"
                ))
                .and_then(|v| v.as_str())
                .unwrap_or("-")
                .to_string();
            let other_internal_family_position = map_custom_fields
                .get(&format!(
                    "{}_{}",
                    candidate_id, "other_internal_family_position"
                ))
                .and_then(|v| v.as_str())
                .unwrap_or("-")
                .to_string();
            let other_internal_family_branch = map_custom_fields
                .get(&format!(
                    "{}_{}",
                    candidate_id, "other_internal_family_branch"
                ))
                .and_then(|v| v.as_str())
                .unwrap_or("-")
                .to_string();
            let other_internal_family_branch_location = map_custom_fields
                .get(&format!(
                    "{}_{}",
                    candidate_id, "other_internal_family_branch_location"
                ))
                .and_then(|v| v.as_str())
                .unwrap_or("-")
                .to_string();

            let candidate_marital_status = candidate_map
                .get("marital_status")
                .and_then(|v| v.as_str())
                .unwrap_or("-");

            let marital_status = map_master_data
                .get(candidate_marital_status)
                .cloned()
                .unwrap_or("-".to_string());

            let candidate_religion = candidate_map
                .get("religion")
                .and_then(|v| v.as_str())
                .unwrap_or("-");

            let religion = map_master_data
                .get(candidate_religion)
                .cloned()
                .unwrap_or("-".to_string());

            let candidate_industry = candidate_map
                .get("industry")
                .and_then(|v| v.as_str())
                .unwrap_or("-");

            let experient_mapper_data = experient_mapper_data
                .get(&candidate_id)
                .cloned()
                .unwrap_or_default();
            let candidate_experiente = experient_mapper_data
                .get(
                    applicant
                        .get("candidate_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or(""),
                )
                .cloned()
                .unwrap_or_default();

            let industry = candidate_experiente
                .get("industry")
                .and_then(|v| v.as_str())
                .unwrap_or("-");

            let job_title = candidate_experiente
                .get("job_title")
                .and_then(|v| v.as_str())
                .unwrap_or("-")
                .to_string();
            let job_description = candidate_experiente
                .get("job_description")
                .and_then(|v| v.as_str())
                .unwrap_or("-")
                .to_string();

            let job_start_date = candidate_experiente
                .get("start_date")
                .and_then(|v| v.as_str())
                .unwrap_or("-")
                .to_string();
            let job_end_date = candidate_experiente
                .get("end_date")
                .and_then(|v| v.as_str())
                .unwrap_or("-")
                .to_string();

            let company_name = candidate_experiente
                .get("company_name")
                .and_then(|v| v.as_str())
                .unwrap_or("-")
                .to_string();

            let domicile_city = candidate_map
                .get("domicile_city_id")
                .and_then(|v| v.as_str())
                .unwrap_or("-");

            let domicile_province = candidate_map
                .get("domicile_province_id")
                .and_then(|v| v.as_str())
                .unwrap_or("-");

            let domicile_postal_code = candidate_map
                .get("domicile_postal_code")
                .and_then(|v| v.as_str())
                .unwrap_or("-");

            let domicile_district = candidate_map
                .get("domicile_district_id")
                .and_then(|v| v.as_str())
                .unwrap_or("-");

            let domicile_sub_district = candidate_map
                .get("domicile_sub_district_id")
                .and_then(|v| v.as_str())
                .unwrap_or("-");
            let domicile_street = candidate_map
                .get("domicile_street")
                .and_then(|v| v.as_str())
                .unwrap_or("-");

            let ktp_city = candidate_map
                .get("ktp_city_id")
                .and_then(|v| v.as_str())
                .unwrap_or("-");

            let ktp_province = candidate_map
                .get("ktp_province_id")
                .and_then(|v| v.as_str())
                .unwrap_or("-");

            let ktp_postal_code = candidate_map
                .get("ktp_postal_code")
                .and_then(|v| v.as_str())
                .unwrap_or("-");

            let ktp_district = candidate_map
                .get("ktp_district_id")
                .and_then(|v| v.as_str())
                .unwrap_or("-");

            let ktp_sub_district = candidate_map
                .get("ktp_sub_district_id")
                .and_then(|v| v.as_str())
                .unwrap_or("-");
            let ktp_street = candidate_map
                .get("ktp_street")
                .and_then(|v| v.as_str())
                .unwrap_or("-");
            let domicile_city_id = geo_master
                .get(domicile_city)
                .cloned()
                .unwrap_or("-".to_string());

            let domicile_district_id = geo_master
                .get(domicile_district)
                .cloned()
                .unwrap_or("-".to_string());

            let domicile_province_id = geo_master
                .get(domicile_province)
                .cloned()
                .unwrap_or("-".to_string());

            let domicile_sub_district_id = geo_master
                .get(domicile_sub_district)
                .cloned()
                .unwrap_or("-".to_string());

            let domicile_street = geo_master
                .get(domicile_street)
                .cloned()
                .unwrap_or("-".to_string());

            //
            //
            //
            //

            let ktp_city_id = geo_master.get(ktp_city).cloned().unwrap_or("-".to_string());

            let ktp_district_id = geo_master
                .get(ktp_district)
                .cloned()
                .unwrap_or("-".to_string());

            let ktp_province_id = geo_master
                .get(ktp_province)
                .cloned()
                .unwrap_or("-".to_string());

            let ktp_sub_district_id = geo_master
                .get(ktp_sub_district)
                .cloned()
                .unwrap_or("-".to_string());

            let ktp_street = geo_master
                .get(ktp_street)
                .cloned()
                .unwrap_or("-".to_string());

            let dob = candidate_map
                .get("dob")
                .and_then(|v| v.as_str())
                .unwrap_or("-");

            rows.push(LsiCandidateDatasetRow {
                vacancy_name: Some(vacancy_name),
                date_applied: applicant_date.to_string(),
                created_at: created_at.to_string(),
                email_address: Some(email.to_string()),
                phone_number: Some(phone_number.to_string()),
                name: Some(fullname),
                gender: Some(gender.to_string()),
                education: Some(education),
                major: Some(major),

                level: education_level,
                institute_name: Some(institute_name),

                start_date: candidate_education_start,
                weight,
                height,

                nationality: Some(nationality),
                end_date: candidate_education_end,
                birth_place: Some(birth_place),
                academic_score: Some(academic_score),
                candidate_id,
                other_internal_family_name: Some(other_internal_family_name),
                other_internal_family_relationship: Some(other_internal_family_relationship),
                other_internal_family_position: Some(other_internal_family_position),
                other_internal_family_branch: Some(other_internal_family_branch),
                other_internal_family_branch_location: Some(other_internal_family_branch_location),
                marital_status: Some(marital_status),
                religion,
                job_title: Some(job_title),
                job_description: Some(job_description),
                job_start_date: Some(job_start_date),
                job_end_date: Some(job_end_date),
                company_name: Some(company_name),
                domicile_city_id: Some(domicile_city_id),
                domicile_province_id: Some(domicile_province_id),
                domicile_district_id: Some(domicile_district_id),
                domicile_postal_code: Some(domicile_postal_code.to_string()),
                domicile_street: Some(domicile_street),
                ktp_street: Some(ktp_street),
                ktp_province_id: Some(ktp_province_id),
                ktp_city_id: Some(ktp_city_id),
                ktp_district_id: Some(ktp_district_id),
                ktp_sub_district_id: Some(ktp_sub_district_id),
                ktp_postal_code: Some(ktp_postal_code.to_string()),
                domicile_sub_district_id: Some(domicile_sub_district_id),
                identity_number_encrypt: Some("tes".to_string()),
                industry: Some(industry.to_string()),
                dob: dob.to_string(),
            });
        }
        let rows: Vec<ExportRow> = rows
            .into_iter()
            .map(|t| ExportRow::validate_json(t, &columns))
            .collect::<anyhow::Result<Vec<ExportRow>>>()?;

        info!("result rows data {:?}", rows.clone());
        Ok((rows, next_cursor))
    }
}
