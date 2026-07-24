use std::{collections::HashMap, sync::Arc};

use crate::{
    application::grpcs::{ats::decryptor::DecryptBatchRequest, gprc::GrpcService},
    core::{
        traits::dataset_trait::Dataset,
        types::{CastingEnum, ExportRow},
    },
};

use anyhow::anyhow;

use serde::{Deserialize, Serialize};
use sqlx::QueryBuilder;

use tracing::debug;

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
    pub height: String,
    pub weight: String,
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
    core::{traits::load::LoadEnum, types::WhereField},
    infrastructure::db::ats_client::AtsClient,
    utils::{
        index_custom_field_rows_by_parent_and_key, unique_string_field_values, uuid_values_filter,
    },
};

#[derive(Clone)]
pub struct LsiCandidateDataset {
    pub client: Arc<AtsClient>,
    pub source: Arc<SourceRegistry>,
    pub data_fetcher: Arc<DataFetcherJsonRegistry>,
    pub grpc_service: Arc<GrpcService>,
}

impl LsiCandidateDataset {
    fn pg_text_array_literal(values: Vec<String>) -> String {
        let escaped_values = values
            .into_iter()
            .map(|value| {
                let escaped = value.replace('\\', "\\\\").replace('"', "\\\"");
                format!("\"{}\"", escaped)
            })
            .collect::<Vec<String>>();

        format!("{{{}}}", escaped_values.join(","))
    }

    fn normalize_candidate_filters(filters: Option<Vec<WhereField>>) -> Option<Vec<WhereField>> {
        filters.map(|filters| {
            filters
                .into_iter()
                .map(|mut filter| {
                    let field_name = filter
                        .field
                        .split('.')
                        .last()
                        .unwrap_or(&filter.field)
                        .trim();

                    if field_name == "tenant_id" {
                        let tenant_ids = filter
                            .values
                            .clone()
                            .or_else(|| filter.value.clone().map(|value| vec![value]))
                            .unwrap_or_default();

                        filter.field = "tenant_pool_keys".into();
                        filter.operator = "&&".into();
                        filter.value = Some(Self::pg_text_array_literal(tenant_ids));
                        filter.values = None;
                        filter.casting = Some(CastingEnum::Enum("text[]".into()));
                    }

                    filter
                })
                .collect()
        })
    }

    fn tenant_ids_from_filters(filters: Option<&Vec<WhereField>>) -> Vec<String> {
        filters
            .into_iter()
            .flatten()
            .filter(|filter| {
                filter
                    .field
                    .split('.')
                    .last()
                    .unwrap_or(&filter.field)
                    .trim()
                    == "tenant_id"
            })
            .flat_map(|filter| {
                filter
                    .values
                    .clone()
                    .or_else(|| filter.value.clone().map(|value| vec![value]))
                    .unwrap_or_default()
            })
            .collect()
    }

    fn tenant_id_filter(tenant_ids: Vec<String>) -> Option<WhereField> {
        if tenant_ids.is_empty() {
            return None;
        }

        Some(uuid_values_filter("tenant_id", tenant_ids))
    }

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
}

#[async_trait::async_trait]
impl Dataset for LsiCandidateDataset {
    async fn get_min_cursor(&self, filters: Option<Vec<WhereField>>) -> anyhow::Result<String> {
        let sql = format!(" SELECT id from {} ", "candidate.candidate");
        let mut qb = QueryBuilder::new(&sql);

        if let Some(filters) = Self::normalize_candidate_filters(filters) {
            qb.push(" WHERE 1=1 ");
            for field in filters {
                qb.push(format!(" AND {} {} ", field.field, field.operator))
                    .push_bind(field.value);
                if let Some(casting) = field.casting {
                    qb.push(format!("::{}", casting.as_str()));
                }
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

        if let LoadEnum::Select(_f, c) = strategy_enum {
            if let Some(c) = c {
                columns = c.clone();
            }
        }
        let tenant_ids = match strategy_enum {
            LoadEnum::Select(filters, _) => Self::tenant_ids_from_filters(filters.as_ref()),
            _ => vec![],
        };

        let candidate_source = self.source.get("oltp_ats_candidate")?;
        let applicant_bucket_fetcher = self.data_fetcher.get("ats_applicant_bucket_fetcher")?;
        let vacancy_fetcher = self.data_fetcher.get("ats_vacancy_fetcher")?;

        let education_fetcher = self.data_fetcher.get("ats_education_fetcher")?;
        let candidate_field_fetcher = self.data_fetcher.get("ats_candidate_field_fetcher")?;
        let experient_fetcher = self.data_fetcher.get("ats_experience_fetcher")?;

        let geo_fetcher = self.data_fetcher.get("MASTER_GEO")?;
        let master_items_fetcher = self.data_fetcher.get("MASTER_ITEMS")?;

        let mut master_data_fetch: HashMap<String, String> = HashMap::new();
        let mut geo_master: HashMap<String, String> = HashMap::new();

        let mut cursor_uuid = None;
        if let Some(c) = cursor {
            match Uuid::parse_str(&c) {
                Ok(u) => cursor_uuid = Some(u),
                Err(_) => return Err(anyhow!("Gagal Parsing data")),
            }
        }

        let strategy_enum = match strategy_enum {
            LoadEnum::Select(filters, columns) => LoadEnum::Select(
                Self::normalize_candidate_filters(filters.clone()),
                columns.clone(),
            ),
            LoadEnum::Upsert => LoadEnum::Upsert,
            LoadEnum::Insert => LoadEnum::Insert,
            LoadEnum::Update(columns) => LoadEnum::Update(columns.clone()),
        };

        // get candidate data as primary source
        let (candidate_fetcher_data, next_cursor) = candidate_source
            .extract(cursor_uuid, limit, &strategy_enum)
            .await?;

        // pretty print for logging candidate data fetchedd

        tracing::info!(
            candidate_data = %serde_json::to_string(&candidate_fetcher_data)?,
            "candidate fetch data"
        );

        let candidate_ids = unique_string_field_values(&candidate_fetcher_data, "id");

        if candidate_ids.is_empty() {
            return Ok((vec![], next_cursor.map(|c| c.to_string())));
        }

        let applicant_filter = vec![uuid_values_filter("candidate_id", candidate_ids.clone())];

        let education_filter = vec![uuid_values_filter("candidate_id", candidate_ids.clone())];

        let mut candidate_client_filter =
            vec![uuid_values_filter("parent_id", candidate_ids.clone())];
        if let Some(filter) = Self::tenant_id_filter(tenant_ids) {
            candidate_client_filter.push(filter);
        }
        let experient_filter = vec![uuid_values_filter("candidate_id", candidate_ids.clone())];

        // let mut address_after_decrypt_vec = Vec::with_capacity(candidate_ids.len());
        //fetch all paralelly
        let (
            applicants,
            education_fetcher_data,
            experient_fetcher_data,
            candidate_client_fetcher_data,
        ) = tokio::try_join!(
            applicant_bucket_fetcher.fetch(Some(applicant_filter)),
            education_fetcher.fetch(Some(education_filter)),
            experient_fetcher.fetch(Some(experient_filter)),
            candidate_field_fetcher.fetch(Some(candidate_client_filter)),
        )?;
        let vacancy_ids = unique_string_field_values(&applicants, "vacancy_id");

        let vacancy_fetcher_data = if vacancy_ids.is_empty() {
            vec![]
        } else {
            let vacancy_filter = vec![uuid_values_filter("id", vacancy_ids)];
            vacancy_fetcher.fetch(Some(vacancy_filter)).await?
        };

        let pii_ciphertexts = candidate_fetcher_data
            .iter()
            .flat_map(|candidate| {
                ["identity_number_encrypt", "dob", "domicile_street"]
                    .into_iter()
                    .filter_map(|field| candidate.get(field).and_then(|v| v.as_str()))
                    .filter(|value| !value.is_empty() && *value != "-")
                    .map(str::to_owned)
            })
            .collect::<Vec<_>>();

        tracing::info!(
            candidates = candidate_fetcher_data.len(),
            pii_ciphertexts = pii_ciphertexts.len(),
            "Preparing candidate PII decryption"
        );
        let ats_decryptor_grpc = self.grpc_service.ats_decryptor_grpc.clone();

        let pii_response = ats_decryptor_grpc
            .batch_decrypt(DecryptBatchRequest {
                ciphertexts: pii_ciphertexts,
            })
            .await?;

        let pii_hashmap = pii_response.plaintexts;

        let vacancy_mapper_data: HashMap<String, String> = vacancy_fetcher_data
            .iter()
            .map(|v| {
                let id = v.get("id").and_then(|v| v.as_str()).unwrap_or("");
                let name = v.get("name").and_then(|v| v.as_str()).unwrap_or("");
                (id.to_string(), name.to_string())
            })
            .collect();
        let map_custom_fields =
            index_custom_field_rows_by_parent_and_key(&candidate_client_fetcher_data);

        debug!("CANDIDATE EXPERIENT : {:?}", experient_fetcher_data);
        debug!("CANDIDATE CLIENT : {:?}", candidate_client_fetcher_data);

        debug!("CANDIDATE CLIENT CUSTOM_FIELDS : {:?}", map_custom_fields);
        debug!("CANDIDATE FETCHER DATA : {:?}", candidate_fetcher_data);
        debug!("VACANCY FETCHER DATA : {:?}", vacancy_fetcher_data);
        debug!("applicants count: {}", applicants.len());
        debug!(
            "candidate_fetcher_data count: {}",
            candidate_fetcher_data.len()
        );
        debug!(
            "education_fetcher_data count: {}",
            education_fetcher_data.len()
        );
        debug!(
            "experient_fetcher_data count: {}",
            experient_fetcher_data.len()
        );
        debug!("candidate_ids count: {}", candidate_ids.len());

        debug!("map_master_data count: {}", map_master_data.len());

        // get master gender;
        let mut master_ids = Vec::new();
        let mut master_geo_ids = Vec::new();
        let applicant_mapper_data = applicants
            .iter()
            .map(|applicant| {
                let candidate_id = applicant
                    .get("candidate_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                (candidate_id.to_string(), applicant.clone())
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

            if let Some(id) = candidate.get("birth_place").and_then(|v| v.as_str()) {
                if Uuid::parse_str(id).is_ok() {
                    master_geo_ids.push(id.to_string());
                } else {
                    geo_master.insert(id.to_string(), id.to_string());
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
            if let Some(id) = education.get("industry").and_then(|v| v.as_str()) {
                if Uuid::parse_str(id).is_ok() {
                    master_ids.push(id.to_string());
                } else {
                    map_master_data.insert(id.to_string(), id.to_string());
                }
            }

            if let Some(id) = education.get("level").and_then(|v| v.as_str()) {
                if Uuid::parse_str(id).is_ok() {
                    master_ids.push(id.to_string());
                } else {
                    map_master_data.insert(id.to_string(), id.to_string());
                }
            }
            if let Some(id) = education.get("major").and_then(|v| v.as_str()) {
                if Uuid::parse_str(id).is_ok() {
                    master_ids.push(id.to_string());
                } else {
                    map_master_data.insert(id.to_string(), id.to_string());
                }
            }
            if let Some(id) = education.get("institute_name").and_then(|v| v.as_str()) {
                if Uuid::parse_str(id).is_ok() {
                    master_ids.push(id.to_string());
                } else {
                    map_master_data.insert(id.to_string(), id.to_string());
                }
            }
        }

        if !master_geo_ids.is_empty() {
            let geo_filter = Some(vec![uuid_values_filter("id", master_geo_ids)]);

            let geo_data = geo_fetcher.fetch(geo_filter).await?;

            for geo in &geo_data {
                if let (Some(id), Some(name)) = (
                    geo.get("id").and_then(|v| v.as_str()),
                    geo.get("name").and_then(|v| v.as_str()),
                ) {
                    geo_master.insert(id.to_string(), name.to_string());
                }
            }
        }

        let master_items_filter = Some(vec![uuid_values_filter("id", master_ids)]);

        let education_mapper_data = education_fetcher_data
            .iter()
            .map(|c| {
                let id = c.get("candidate_id").and_then(|v| v.as_str()).unwrap_or("");
                (id.to_string(), c.clone())
            })
            .collect::<HashMap<String, _>>();

        debug!("EDUCATION MAPPER DATA {:?}", education_mapper_data);

        let mut experient_mapper_data: HashMap<String, serde_json::Value> = HashMap::new();
        for c in &experient_fetcher_data {
            let id = c
                .get("candidate_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            experient_mapper_data.entry(id).or_insert(c.clone());
        }

        debug!("EXPRIENT MAPPER DATA : {:?}", experient_mapper_data);

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

        let next_cursor = next_cursor.map(|c| c.to_string());

        let mut rows: Vec<LsiCandidateDatasetRow> = vec![];

        for candidate_map in &candidate_fetcher_data {
            let candidate_id = candidate_map
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or("-")
                .to_string();
            let applicant = applicant_mapper_data
                .get(candidate_id.as_str())
                .cloned()
                .unwrap_or_default();
            let mut vacancy_name = String::from("-");
            let applicant_date = applicant
                .get("created_at")
                .and_then(|v| v.as_str())
                .unwrap_or("-");
            let created_at = candidate_map
                .get("created_at")
                .and_then(|v| v.as_str())
                .unwrap_or("-");

            let email = candidate_map
                .get("email")
                .and_then(|v| v.as_str())
                .unwrap_or("-");

            let mut fullname = String::from("-");

            if let Some(name) = candidate_map.get("name") {
                if let Some(name_str) = name.as_str() {
                    fullname.push_str(name_str);
                }
            }

            let candidate_education = education_mapper_data
                .get(candidate_id.as_str())
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

            let institute_name = map_master_data
                .get(candidate_institute)
                .cloned()
                .unwrap_or("-".to_string());

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

            let birth_place_name = geo_master
                .get(&birth_place)
                .cloned()
                .unwrap_or("-".to_string());

            let log_candidate_id = candidate_id.clone();

            let weight = self.get_map_custom_field(
                &format!("{}_{}", candidate_id, "weight"),
                &map_custom_fields,
            );
            let height = self.get_map_custom_field(
                &format!("{}_{}", candidate_id, "height"),
                &map_custom_fields,
            );

            let nationality = self.get_map_custom_field(
                &format!("{}_{}", candidate_id, "nationality"),
                &map_custom_fields,
            );

            let other_internal_family_name = self.get_map_custom_field(
                &format!(
                    "{}_{}",
                    &candidate_id,
                    "otherInternalFamilyName
"
                ),
                &map_custom_fields,
            );

            let other_internal_family_relationship = self.get_map_custom_field(
                &format!(
                    "{}_{}",
                    &candidate_id,
                    "otherInternalFamilyRelationship
"
                ),
                &map_custom_fields,
            );

            let other_internal_family_position = self.get_map_custom_field(
                &format!(
                    "{}_{}",
                    &candidate_id,
                    "otherInternalFamilyPosition
"
                ),
                &map_custom_fields,
            );

            let other_internal_family_branch = self.get_map_custom_field(
                &format!(
                    "{}_{}",
                    &candidate_id,
                    "otherInternalFamilyBranch
"
                ),
                &map_custom_fields,
            );

            let other_internal_family_branch_location = self.get_map_custom_field(
                &format!(
                    "{}_{}",
                    &candidate_id,
                    "otherInternalFamilyBranchLocation
"
                ),
                &map_custom_fields,
            );

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

            let candidate_experiente = experient_mapper_data
                .get(&candidate_id)
                .cloned()
                .unwrap_or_default();

            let _industry = candidate_experiente
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
            let domicile_street_ciphertext = candidate_map
                .get("domicile_street")
                .and_then(|v| v.as_str())
                .unwrap_or("");

            let domicile_street = if domicile_street_ciphertext.is_empty() {
                "-".to_string()
            } else {
                pii_hashmap
                    .get(domicile_street_ciphertext)
                    .cloned()
                    .unwrap_or_else(|| "-".to_string())
            };

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

            let dob_ciphertext = candidate_map
                .get("dob")
                .and_then(|v| v.as_str())
                .unwrap_or("");

            let dob = if dob_ciphertext.is_empty() {
                "-".to_string()
            } else {
                pii_hashmap
                    .get(dob_ciphertext)
                    .cloned()
                    .unwrap_or_else(|| "-".to_string())
            };

            let industry = map_master_data
                .get(candidate_industry)
                .cloned()
                .unwrap_or("-".to_string());

            let identity_ciphertext = candidate_map
                .get("identity_number_encrypt")
                .and_then(|v| v.as_str())
                .unwrap_or("");

            let identity_number = if identity_ciphertext.is_empty() {
                "-".to_string()
            } else {
                pii_hashmap
                    .get(identity_ciphertext)
                    .cloned()
                    .unwrap_or_else(|| "-".to_string())
            };

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

                level: education_level.to_string(),
                institute_name: Some(institute_name.to_string()),

                start_date: candidate_education_start,
                weight,
                height,

                nationality: Some(nationality),
                end_date: candidate_education_end,
                birth_place: Some(birth_place_name),
                academic_score: Some(academic_score),
                candidate_id: log_candidate_id.clone(),
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
                identity_number_encrypt: Some(identity_number),
                industry: Some(industry.to_string()),
                dob,
            });
        }
        let rows: Vec<ExportRow> = rows
            .into_iter()
            .map(|t| ExportRow::validate_json(t, &columns))
            .collect::<anyhow::Result<Vec<ExportRow>>>()?;

        debug!("result rows data {:?}", rows.clone());
        Ok((rows, next_cursor))
    }
}
