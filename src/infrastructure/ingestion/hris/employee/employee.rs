use std::sync::Arc;

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use sqlx::{types::JsonValue, FromRow, Postgres, QueryBuilder};
use tracing::instrument;
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
    infrastructure::{
        db::hris_client::HrisClient,
        ingestion::hris::{normalize_hris_filters, types::HrisIngestionTable},
    },
};

#[derive(Debug, FromRow, Serialize, Deserialize)]
pub struct IngestionEmployee {
    pub id: String,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
    pub deleted_at: Option<NaiveDateTime>,
    pub tenant_id: Uuid,
    pub group_id: Uuid,
    pub first_name: String,
    pub last_name: Option<String>,
    pub full_name: String,
    pub email: String,
    pub phone_number: Option<String>,
    pub birth_place: Option<String>,
    pub birth_date: Option<String>,
    pub profile_photo_id: Option<Uuid>,
    pub profile_photo: Option<JsonValue>,
    pub face_recognition_id: Option<Uuid>,
    pub face_recognition: Option<JsonValue>,
    pub identity_id: Option<Uuid>,
    pub identity_meta: Option<JsonValue>,
    pub nationality_id: Option<Uuid>,
    pub nationality_meta: Option<JsonValue>,
    pub gender_id: Option<Uuid>,
    pub gender_meta: Option<JsonValue>,
    pub religion_id: Option<Uuid>,
    pub religion_meta: Option<JsonValue>,
    pub marital_status_id: Option<Uuid>,
    pub marital_status_meta: Option<JsonValue>,
    pub same_as_id_card: bool,
    pub status: String,
    pub id_card_province_id: Option<Uuid>,
    pub id_card_province_meta: Option<JsonValue>,
    pub id_card_city_id: Option<Uuid>,
    pub id_card_city_meta: Option<JsonValue>,
    pub id_card_postal_code: Option<String>,
    pub id_card_address_detail_encrypted: Option<String>,
    pub current_province_id: Option<Uuid>,
    pub current_province_meta: Option<JsonValue>,
    pub current_city_id: Option<Uuid>,
    pub current_city_meta: Option<JsonValue>,
    pub current_postal_code: Option<String>,
    pub current_address_detail_encrypted: Option<String>,
    pub employee_type: String,
}

pub struct IngestionEmployeeSource {
    pub pool: Arc<HrisClient>,
}

impl BuildTableTrait<HrisIngestionTable> for IngestionEmployeeSource {
    fn table(&self) -> HrisIngestionTable {
        HrisIngestionTable::Employee
    }

    fn columns(&self) -> &'static [&'static str] {
        &[
            "id",
            "created_at",
            "updated_at",
            "deleted_at",
            "tenant_id",
            "group_id",
            "first_name",
            "last_name",
            "full_name",
            "email",
            "phone_number",
            "birth_place",
            "birth_date",
            "profile_photo_id",
            "profile_photo",
            "face_recognition_id",
            "face_recognition",
            "identity_id",
            "identity_meta",
            "nationality_id",
            "nationality_meta",
            "gender_id",
            "gender_meta",
            "religion_id",
            "religion_meta",
            "marital_status_id",
            "marital_status_meta",
            "same_as_id_card",
            "status::text AS status",
            "id_card_province_id",
            "id_card_province_meta",
            "id_card_city_id",
            "id_card_city_meta",
            "id_card_postal_code",
            "id_card_address_detail_encrypted",
            "current_province_id",
            "current_province_meta",
            "current_city_id",
            "current_city_meta",
            "current_postal_code",
            "current_address_detail_encrypted",
            "employee_type::text AS employee_type",
        ]
    }
}

#[async_trait::async_trait]
impl DataFetcherJson for IngestionEmployeeSource {
    #[instrument(skip(self), fields(types = "employee data fetcher"))]
    async fn fetch(
        &self,
        filters: Option<Vec<WhereField>>,
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        let filters = normalize_hris_filters(filters);

        let sql = format!(
            " SELECT {} from {} ",
            self.columns().join(", "),
            self.table().table_name()
        );

        let mut qb = QueryBuilder::new(&sql);

        let parse_result =
            fetch_data_json::<IngestionEmployee>(&mut qb, filters, &self.pool.pool, None).await;

        match parse_result {
            Ok(result) => {
                tracing::debug!("employee source {:?}", result.clone());
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
impl SourceIncrementalExtract for IngestionEmployeeSource {
    async fn extract(
        &self,
        cursor: Option<Uuid>,
        limit: i64,
        _strategy_enum: &LoadEnum,
    ) -> anyhow::Result<(Vec<serde_json::Value>, Option<Uuid>)> {
        let sql_query = format!(
            "SELECT {} FROM {}",
            self.columns().join(", "),
            self.table().table_name()
        );

        let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(sql_query);

        if let Some(c) = cursor {
            qb.push(" WHERE id::uuid > ").push_bind(c);
        }
        qb.push(" ORDER BY id::uuid ASC LIMIT ").push_bind(limit);

        let result = qb
            .build_query_as::<IngestionEmployee>()
            .fetch_all(&self.pool.pool)
            .await?;

        let last_cursor_string = result.last().map(|last| last.id.clone());
        let last_cursor: Option<Uuid> = last_cursor_string.and_then(|id| id.parse().ok());

        let parse_result = result
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<serde_json::Value>, _>>()?;

        tracing::debug!("candidate source {:?}", parse_result.clone());
        Ok((parse_result, last_cursor))
    }
}
