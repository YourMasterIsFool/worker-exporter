use std::{collections::HashMap, sync::Arc};

use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::QueryBuilder;
use uuid::Uuid;

use crate::{
    application::{
        fetch_source_regitry::DataFetcherJsonRegistry,
        ingestion::source_registry::SourceRegistry,
    },
    core::{
        traits::{
            dataset_trait::Dataset,
            extractor::source_incremental_extract::SourceIncrementalExtract,
            load::LoadEnum,
        },
        types::{ExportRow, WhereField},
    },
    infrastructure::db::ats_client::AtsClient,
    utils::{
        custom_field::normalize_custom_field_key, export_value_to_string,
        resolved_custom_field_value, unique_string_field_values, uuid_values_filter,
    },
};

/// Display names for the export configuration. `No` is added automatically by
/// `ExportRunner`, so it is intentionally not included here.
pub const REFERENCE_CHECK_HEADERS: &[(&str, &str)] = &[
    ("Check", "check"),
    ("Candidate ID", "candidate_id"),
    ("Candidate Email", "candidate_email"),
    ("Candidate Name", "candidate_name"),
    ("RR Number", "rr_number"),
    ("Last Position", "last_position"),
    ("Referral Email", "referral_email"),
    ("Referral Name", "referral_name"),
    ("Company Name", "company_name"),
    ("Position", "position"),
    ("Relation", "relation"),
    ("Phone Number", "phone_number"),
    ("Candidate Position", "candidate_position"),
    ("Candidate Superordinate", "candidate_superordinate"),
    ("Candidate Subordinate", "candidate_subordinate"),
    ("Candidate Join Date", "candidate_join_date"),
    ("Candidate End Date", "candidate_end_date"),
    ("Relation Period", "relation_period"),
    ("Candidate Responsibility", "candidate_responsibility"),
    ("Candidate Strength", "candidate_strength"),
    ("Candidate Achievement", "candidate_achievement"),
    ("candidate Weakness", "candidate_weakness"),
    ("Internal Organization", "internal_organization"),
    ("Internal Organization Desc", "internal_organization_desc"),
    ("External Organization", "external_organization"),
    ("External Organization Desc", "external_organization_desc"),
    ("Internal Case", "internal_case"),
    ("Internal Case Desc", "internal_case_desc"),
    ("External Case", "external_case"),
    ("External Case Desc", "external_case_desc"),
    ("Quit Reason", "quit_reason"),
    (
        "Current Position Recommendation",
        "current_position_recommendation",
    ),
    ("Lead Position Recommendation", "lead_position_recommendation"),
    (
        "Collaborate with Others Recommendation",
        "collaborate_with_others_recommendation",
    ),
    ("Recomendation Reason", "recomendation_reason"),
    (
        "Chance to Work with the Company Before",
        "chance_to_work_with_the_company_before",
    ),
    ("Other", "other"),
];

#[derive(Debug, Serialize, Deserialize)]
pub struct LsiReferenceCheckDatasetRow {
    pub check: String,
    pub candidate_id: String,
    pub candidate_email: String,
    pub candidate_name: String,
    pub rr_number: String,
    pub last_position: String,
    pub referral_email: String,
    pub referral_name: String,
    pub company_name: String,
    pub position: String,
    pub relation: String,
    pub phone_number: String,
    pub candidate_position: String,
    pub candidate_superordinate: String,
    pub candidate_subordinate: String,
    pub candidate_join_date: String,
    pub candidate_end_date: String,
    pub relation_period: String,
    pub candidate_responsibility: String,
    pub candidate_strength: String,
    pub candidate_achievement: String,
    pub candidate_weakness: String,
    pub internal_organization: String,
    pub internal_organization_desc: String,
    pub external_organization: String,
    pub external_organization_desc: String,
    pub internal_case: String,
    pub internal_case_desc: String,
    pub external_case: String,
    pub external_case_desc: String,
    pub quit_reason: String,
    pub current_position_recommendation: String,
    pub lead_position_recommendation: String,
    pub collaborate_with_others_recommendation: String,
    pub recomendation_reason: String,
    pub chance_to_work_with_the_company_before: String,
    pub other: String,
}

type CustomFields = HashMap<String, String>;

pub struct LsiReferenceCheckDataset {
    pub client: Arc<AtsClient>,
    pub source: Arc<SourceRegistry>,
    pub data_fetcher: Arc<DataFetcherJsonRegistry>,
}

impl LsiReferenceCheckDataset {
    fn pg_text_array_literal(values: Vec<String>) -> String {
        let values = values
            .into_iter()
            .map(|value| {
                format!(
                    "\"{}\"",
                    value.replace('\\', "\\\\").replace('"', "\\\"")
                )
            })
            .collect::<Vec<_>>();
        format!("{{{}}}", values.join(","))
    }

    fn normalize_filters(filters: Option<Vec<WhereField>>) -> Option<Vec<WhereField>> {
        filters.map(|filters| {
            filters
                .into_iter()
                .map(|mut filter| {
                    if filter
                        .field
                        .split('.')
                        .last()
                        .unwrap_or(&filter.field)
                        .trim()
                        == "tenant_id"
                    {
                        let tenant_ids = filter
                            .values
                            .clone()
                            .or_else(|| filter.value.clone().map(|value| vec![value]))
                            .unwrap_or_default();
                        filter.field = "tenant_pool_keys".into();
                        filter.operator = "&&".into();
                        filter.value = Some(Self::pg_text_array_literal(tenant_ids));
                        filter.values = None;
                        filter.casting = Some(crate::core::types::CastingEnum::Enum(
                            "text[]".into(),
                        ));
                    }
                    filter
                })
                .collect()
        })
    }

    fn custom_fields(rows: &[Value]) -> HashMap<String, CustomFields> {
        let mut result = HashMap::new();
        for row in rows {
            let (Some(parent_id), Some(field_key)) = (
                row.get("parent_id").and_then(Value::as_str),
                row.get("field_key").and_then(Value::as_str),
            ) else {
                continue;
            };

            let value = resolved_custom_field_value(row)
                .cloned()
                .or_else(|| row.get("meta").cloned())
                .map(export_value_to_string)
                .unwrap_or_else(|| "-".into());

            result
                .entry(parent_id.to_string())
                .or_insert_with(HashMap::new)
                .insert(normalize_custom_field_key(field_key), value);
        }
        result
    }

    fn custom_value(fields: Option<&CustomFields>, key: &str) -> String {
        fields
            .and_then(|fields| fields.get(&normalize_custom_field_key(key)))
            .cloned()
            .unwrap_or_else(|| "-".into())
    }

    fn json_value(row: &Value, key: &str) -> String {
        row.get(key)
            .map(|value| export_value_to_string(value.clone()))
            .unwrap_or_else(|| "-".into())
    }

    fn custom_or_json(fields: Option<&CustomFields>, row: &Value, key: &str) -> String {
        let custom = Self::custom_value(fields, key);
        if custom == "-" {
            Self::json_value(row, key)
        } else {
            custom
        }
    }
}

#[async_trait::async_trait]
impl Dataset for LsiReferenceCheckDataset {
    async fn get_min_cursor(&self, filters: Option<Vec<WhereField>>) -> anyhow::Result<String> {
        let mut query = QueryBuilder::new("SELECT id FROM candidate.candidate");
        if let Some(filters) = Self::normalize_filters(filters) {
            query.push(" WHERE 1 = 1");
            for filter in filters {
                query
                    .push(" AND ")
                    .push(filter.field)
                    .push(" ")
                    .push(filter.operator)
                    .push(" ")
                    .push_bind(filter.value);
                if let Some(casting) = filter.casting {
                    query.push("::").push(casting.as_str());
                }
            }
        }
        query.push(" ORDER BY id ASC LIMIT 1");

        Ok(query
            .build_query_scalar::<Uuid>()
            .fetch_one(&self.client.pool)
            .await?
            .to_string())
    }

    async fn fetch_batch(
        &self,
        strategy_enum: &LoadEnum,
        cursor: Option<String>,
        limit: i64,
    ) -> anyhow::Result<(Vec<ExportRow>, Option<String>)> {
        let (filters, columns) = match strategy_enum {
            LoadEnum::Select(filters, columns) => (filters.clone(), columns.clone().unwrap_or_default()),
            _ => (None, Vec::new()),
        };
        let source = self.source.get("oltp_ats_candidate")?;
        let strategy = LoadEnum::Select(Self::normalize_filters(filters), None);
        let cursor = cursor
            .map(|cursor| Uuid::parse_str(&cursor).map_err(|error| anyhow!("cursor invalid: {error}")))
            .transpose()?;
        let (candidates, next_cursor) = source.extract(cursor, limit, &strategy).await?;
        let candidate_ids = unique_string_field_values(&candidates, "id");
        if candidate_ids.is_empty() {
            return Ok((Vec::new(), next_cursor.map(|cursor| cursor.to_string())));
        }

        let candidate_fields = self
            .data_fetcher
            .get("ats_candidate_custom_field_fetcher")?
            .fetch(Some(vec![uuid_values_filter(
                "parent_id",
                candidate_ids.clone(),
            )]))
            .await?;
        let experiences = self
            .data_fetcher
            .get("ats_experience_fetcher")?
            .fetch(Some(vec![uuid_values_filter("candidate_id", candidate_ids)]))
            .await?;
        let custom_fields = Self::custom_fields(&candidate_fields);
        let experiences = experiences
            .iter()
            .map(|row| (Self::json_value(row, "candidate_id"), row))
            .collect::<HashMap<_, _>>();

        let rows = candidates
            .iter()
            .map(|candidate| {
                let candidate_id = Self::json_value(candidate, "id");
                let fields = custom_fields.get(&candidate_id);
                let experience = experiences.get(&candidate_id).copied().unwrap_or(&Value::Null);
                LsiReferenceCheckDatasetRow {
                    check: Self::custom_value(fields, "check"),
                    candidate_id: candidate_id.clone(),
                    candidate_email: Self::json_value(candidate, "email"),
                    candidate_name: Self::json_value(candidate, "name"),
                    rr_number: Self::custom_value(fields, "rr_number"),
                    last_position: Self::custom_or_json(fields, experience, "job_title"),
                    referral_email: Self::custom_value(fields, "referral_email"),
                    referral_name: Self::custom_value(fields, "referral_name"),
                    company_name: Self::custom_or_json(fields, experience, "company_name"),
                    position: Self::custom_or_json(fields, experience, "job_title"),
                    relation: Self::custom_value(fields, "relation"),
                    phone_number: Self::custom_or_json(fields, candidate, "phone_number"),
                    candidate_position: Self::custom_value(fields, "candidate_position"),
                    candidate_superordinate: Self::custom_value(fields, "candidate_superordinate"),
                    candidate_subordinate: Self::custom_value(fields, "candidate_subordinate"),
                    candidate_join_date: Self::custom_value(fields, "candidate_join_date"),
                    candidate_end_date: Self::custom_value(fields, "candidate_end_date"),
                    relation_period: Self::custom_value(fields, "relation_period"),
                    candidate_responsibility: Self::custom_value(fields, "candidate_responsibility"),
                    candidate_strength: Self::custom_value(fields, "candidate_strength"),
                    candidate_achievement: Self::custom_value(fields, "candidate_achievement"),
                    candidate_weakness: Self::custom_value(fields, "candidate_weakness"),
                    internal_organization: Self::custom_value(fields, "internal_organization"),
                    internal_organization_desc: Self::custom_value(fields, "internal_organization_desc"),
                    external_organization: Self::custom_value(fields, "external_organization"),
                    external_organization_desc: Self::custom_value(fields, "external_organization_desc"),
                    internal_case: Self::custom_value(fields, "internal_case"),
                    internal_case_desc: Self::custom_value(fields, "internal_case_desc"),
                    external_case: Self::custom_value(fields, "external_case"),
                    external_case_desc: Self::custom_value(fields, "external_case_desc"),
                    quit_reason: Self::custom_value(fields, "quit_reason"),
                    current_position_recommendation: Self::custom_value(fields, "current_position_recommendation"),
                    lead_position_recommendation: Self::custom_value(fields, "lead_position_recommendation"),
                    collaborate_with_others_recommendation: Self::custom_value(
                        fields,
                        "collaborate_with_others_recommendation",
                    ),
                    recomendation_reason: Self::custom_value(fields, "recomendation_reason"),
                    chance_to_work_with_the_company_before: Self::custom_value(
                        fields,
                        "chance_to_work_with_the_company_before",
                    ),
                    other: Self::custom_value(fields, "other"),
                }
            })
            .map(|row| ExportRow::validate_json(row, &columns))
            .collect::<anyhow::Result<Vec<_>>>()?;

        Ok((rows, next_cursor.map(|cursor| cursor.to_string())))
    }
}

#[cfg(test)]
mod tests {
    use super::{normalize_custom_field_key, REFERENCE_CHECK_HEADERS};

    #[test]
    fn headers_match_reference_check_contract() {
        assert_eq!(REFERENCE_CHECK_HEADERS.len(), 37);
        assert_eq!(REFERENCE_CHECK_HEADERS[0], ("Check", "check"));
        assert_eq!(
            normalize_custom_field_key("candidateWeakness"),
            normalize_custom_field_key("candidate_weakness")
        );
        assert_eq!(REFERENCE_CHECK_HEADERS[36].0, "Other");
    }
}
