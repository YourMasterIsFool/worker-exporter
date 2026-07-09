use std::sync::Arc;

use chrono::{NaiveDate, NaiveDateTime};
use serde::{Deserialize, Serialize};
use sqlx::{Postgres, QueryBuilder};
use tracing::{debug, instrument};
use uuid::Uuid;

use crate::{
    core::{
        traits::{
            etl::{fetch_data_json, DataFetcherJson},
            extractor::{
                build_query_trait::BuildTableTrait,
                source_incremental_extract::SourceIncrementalExtract,
            },
            load::LoadEnum,
        },
        types::WhereField,
    },
    infrastructure::{db::ats_client::AtsClient, ingestion::ats::types::AtsIngestionTable},
};
use crate::core::types::CastingEnum;
use crate::utils::resolve_query_builder_with_where_fields;

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
pub struct AtsCandidateIngestionModel {
    pub id: Uuid,

    pub created_at: NaiveDateTime,
    pub updated_at: Option<NaiveDateTime>,
    pub deleted_at: Option<NaiveDateTime>,

    pub user_id: Option<Uuid>,
    pub name: String,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub email: String,

    // Identitas
    pub identity_number_encrypt: Option<String>,
    pub identity_number_hashing: Option<String>,
    pub is_identity_number_verified: Option<bool>,
    pub identity_number_verified_at: Option<NaiveDateTime>,
    pub nationality: Option<String>, // Bisa diubah ke Enum jika perlu
    pub passport_number: Option<String>,
    pub passport_expired_at: Option<NaiveDate>,

    // Media
    pub profile_photo_media_id: Option<Uuid>,
    pub video_resume_media_id: Option<Uuid>,
    pub cv_media_id: Option<Uuid>,

    // Kontak & Personal
    pub phone_country_code: Option<String>,
    pub phone_number: Option<String>,
    pub is_phone_verified: Option<bool>,
    pub dob: Option<String>,
    pub gender: Option<String>,
    pub birth_place: Option<String>,
    pub religion: Option<String>,
    pub marital_status: Option<String>,

    // Lokasi & Alamat
    pub country_id: Option<Uuid>,
    pub province_id: Option<Uuid>,
    pub city_id: Option<Uuid>,
    pub location_meta: Option<serde_json::Value>,

    // KTP
    pub ktp_street: Option<String>,
    pub ktp_province_id: Option<Uuid>,
    pub ktp_city_id: Option<Uuid>,
    pub ktp_district_id: Option<Uuid>,
    pub ktp_subdistrict_id: Option<Uuid>,
    pub ktp_postal_code: Option<String>,

    // Domisili
    pub domicile_same_as_ktp: Option<bool>,
    pub domicile_street: Option<String>,
    pub domicile_province_id: Option<Uuid>,
    pub domicile_city_id: Option<Uuid>,
    pub domicile_district_id: Option<Uuid>,
    pub domicile_subdistrict_id: Option<Uuid>,
    pub domicile_postal_code: Option<String>,

    // Preferensi & Status
    pub expected_salary: Option<i64>, // Bigint di DB -> i64 di Rust
    pub preference_work: Option<String>,
    pub active_publish_keys: Option<Vec<String>>,
    pub tenant_pool_keys: Option<Vec<String>>,
    pub group_pool_keys: Option<Vec<String>>,
    pub is_show_to_partners: Option<bool>,
    pub is_show_to_hrms_explore: Option<bool>,

    // Lain-lain
    pub coordinate: Option<String>, // 'point' bisa dipetakan ke String atau custom type
    pub interests: Option<Vec<Uuid>>,
    pub keywords: Option<Vec<String>>,
    pub filterable_attribute: Option<serde_json::Value>,
    pub last_education_id: Option<Uuid>,
    pub total_experience_id: Option<Uuid>,
}

pub struct AtsCandidateSource {
    pub pool: Arc<AtsClient>,
}

impl BuildTableTrait<AtsIngestionTable> for AtsCandidateSource {
    fn table(&self) -> AtsIngestionTable {
        AtsIngestionTable::Candidate
    }

    fn columns(&self) -> &'static [&'static str] {
        &[
            "id",
            "created_at",
            "updated_at",
            "deleted_at",
            "user_id",
            "name",
            "first_name",
            "last_name",
            "email",
            "identity_number_encrypt",
            "identity_number_hashing",
            "is_identity_number_verified",
            "identity_number_verified_at",
            "nationality",
            "passport_number",
            "passport_expired_at",
            "profile_photo_media_id",
            "video_resume_media_id",
            "cv_media_id",
            "phone_country_code",
            "phone_number",
            "is_phone_verified",
            "dob",
            "gender",
            "birth_place",
            "religion",
            "marital_status",
            "country_id",
            "province_id",
            "city_id",
            "location_meta",
            "ktp_street",
            "ktp_province_id",
            "ktp_city_id",
            "ktp_district_id",
            "ktp_subdistrict_id",
            "ktp_postal_code",
            "domicile_same_as_ktp",
            "domicile_street",
            "domicile_province_id",
            "domicile_city_id",
            "domicile_district_id",
            "domicile_subdistrict_id",
            "domicile_postal_code",
            "expected_salary",
            "preference_work",
            "active_publish_keys",
            "tenant_pool_keys",
            "group_pool_keys",
            "is_show_to_partners",
            "is_show_to_hrms_explore",
            "coordinate",
            "interests",
            "keywords",
            "filterable_attribute",
            "last_education_id",
            "total_experience_id",
        ]
    }
}
#[async_trait::async_trait]
impl DataFetcherJson for AtsCandidateSource {
    #[instrument(skip(self), fields(types = "candidate data fetcher"))]
    async fn fetch(
        &self,
        filters: Option<Vec<WhereField>>,
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        let sql = format!(
            " SELECT {} from {} ",
            self.columns().join(", "),
            self.table().table_name()
        );

        let mut qb = QueryBuilder::new(&sql);

        let parse_result =
            fetch_data_json::<AtsCandidateIngestionModel>(&mut qb, filters, &self.pool.pool, None)
                .await;

        match parse_result {
            Ok(result) => {
                tracing::debug!(" candidate source {:?}", result.clone());
                Ok(result)
            }
            Err(e) => {
                tracing::error!("{}", e);
                Err(e)
            }
        }
    }
}
#[async_trait::async_trait]
impl SourceIncrementalExtract for AtsCandidateSource {
    async fn extract(
        &self,
        cursor: Option<Uuid>,
        limit: i64,
        _strategy_enum: &LoadEnum,
    ) -> anyhow::Result<(Vec<serde_json::Value>, Option<Uuid>)> {


        tracing::debug!("extract cursor: {:?}, limit: {}", cursor, limit);
        let sql_query = format!("SELECT * FROM {}", self.table().table_name());

        let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(sql_query);

        qb.push(" WHERE 1=1 ");

        if let LoadEnum::Select(filter, _) =  _strategy_enum {
            resolve_query_builder_with_where_fields(&mut qb, filter);
        }

        if let Some(c) = cursor {
            qb.push("  AND id > ").push_bind(c);
        }
        qb.push(" LIMIT ").push_bind(limit);
        let result = qb
            .build_query_as::<AtsCandidateIngestionModel>()
            .fetch_all(&self.pool.pool)
            .await;

        match result {
            Ok(result) => {
                debug!("result candidate ingestion : {:?}", result.iter().clone());
                let last_cursor: Option<Uuid> = result.last().map(|last| last.id);

                tracing::debug!("result ingestion: {:?}", result.iter().clone());
                let parse_result = result
                    .iter()
                    .map(serde_json::to_value)
                    .collect::<Result<Vec<serde_json::Value>, _>>()?;

                tracing::debug!(" candidate source {:?}", parse_result.clone());
                Ok((parse_result, last_cursor))
            }
            Err(e) => {
                debug!("errr {:?}", e);
                return Err(e.into());
            }
        }
    }
}
