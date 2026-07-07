use std::{collections::HashMap, sync::Arc};

use crate::{
    application::{
        fetch_source_regitry::DataFetcherJsonRegistry, ingestion::source_registry::SourceRegistry,
    },
    core::{
        traits::{dataset_trait::SyncDataset, load::LoadEnum},
        types::{
            CastingEnum, SourceMappingTable, SourceSyncConfig, SycnTableResult, SyncPayloadDataset,
            WhereField,
        },
    },
};

pub struct LsiVacancySyncDataset {
    pub source: Arc<SourceRegistry>,
    pub fetcher: Arc<DataFetcherJsonRegistry>,
}

impl LsiVacancySyncDataset {
    const VACANCY_TABLE: &'static str = "warehouse.ats_vacancy_temporary";
    const CUSTOM_FIELD_TABLE: &'static str = "warehouse.ats_vacancy_custom_field";
    const STAGE_TABLE: &'static str = "warehouse.ats_vacancy_stage";
    const REQUIREMENT_TABLE: &'static str = "warehouse.ats_vacancy_requirement";
    const PIC_TABLE: &'static str = "warehouse.ats_vacancy_pic";

    const VACANCY_COLUMNS: &'static [&'static str] = &[
        "id",
        "updated_at",
        "created_at",
        "deleted_at",
        "tenant_id",
        "group_id",
        "name",
        "description",
        "requirement_stage_template_id",
        "recruitment_form_template_id",
        "pre_onboarding_template_id",
        "snapshot",
        "active_publish_keys",
        "created_by",
        "permission_keys",
        "job_functions",
        "expired_date",
        "is_confidential",
        "headcount_quota",
        "salary_min",
        "salary_max",
        "is_hide_salary",
        "verified_status",
        "status",
        "is_locked",
        "publish_at",
        "job_position_id",
        "job_position_meta",
        "organization_id",
        "organization_meta",
        "employment_status_id",
        "employment_status_meta",
        "work_placement_type_id",
        "work_placement_type_meta",
        "work_location_id",
        "work_location_meta",
        "total_hired",
        "keywords",
        "filterable_attribute",
        "sla",
        "province_id",
        "province_meta",
        "city_id",
        "city_meta",
        "completion_status",
        "current_hired",
    ];
    const CUSTOM_FIELD_COLUMNS: &'static [&'static str] = &[
        "id",
        "updated_at",
        "created_at",
        "deleted_at",
        "tenant_id",
        "group_id",
        "parent_id",
        "field_key",
        "field_type",
        "value_text",
        "value_number",
        "value_id",
        "meta",
        "reference_module",
        "value_bool",
        "value_date",
        "loggin_id",
    ];
    const STAGE_COLUMNS: &'static [&'static str] = &[
        "id",
        "updated_at",
        "created_at",
        "deleted_at",
        "vacancy_id",
        "stage_name",
        "stage_type",
        "is_default",
        "stage_email_template_id",
        "stage_email_template_reject",
        "sequence",
        "is_locked",
        "form_template_id",
        "share_access",
        "logging_id",
    ];
    const REQUIREMENT_COLUMNS: &'static [&'static str] = &[
        "id",
        "updated_at",
        "created_at",
        "deleted_at",
        "vacancy_id",
        "key_field",
        "field_type",
        "field_purpose",
        "tooltip",
        "multiple_choice_option",
        "allowed_file_type",
        "maximum_file",
        "maximum_file_size_in_kb",
        "is_default_question",
        "is_range",
        "compare_operator",
        "pivot",
        "min_pivot",
        "max_pivot",
        "criteria",
        "is_mandatory",
        "master_data",
        "data_mapping",
        "logging_id",
    ];
    const PIC_COLUMNS: &'static [&'static str] = &[
        "id",
        "vacancy_id",
        "employee_id",
        "employee_meta",
        "is_verified",
        "created_at",
        "logging_id",
    ];

    fn mapping(columns: &[&str], values: Vec<serde_json::Value>) -> SourceMappingTable {
        SourceMappingTable {
            columns: columns.iter().map(|column| column.to_string()).collect(),
            values,
        }
    }

    fn vacancy_ids(rows: &[serde_json::Value]) -> Vec<String> {
        rows.iter()
            .filter_map(|row| row.get("id").and_then(|value| value.as_str()))
            .map(ToString::to_string)
            .collect()
    }

    async fn fetch_by_ids(
        &self,
        fetcher_name: &str,
        field: &str,
        ids: &[String],
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        if ids.is_empty() {
            return Ok(vec![]);
        }

        let fetcher = self.fetcher.get(fetcher_name)?;
        fetcher
            .fetch(Some(vec![WhereField {
                field: field.to_string(),
                operator: "=".to_string(),
                value: None,
                values: Some(ids.to_vec()),
                casting: Some(CastingEnum::Uuid),
            }]))
            .await
    }

    async fn vacancy_rows(
        &self,
        payload: &SyncPayloadDataset,
    ) -> anyhow::Result<Vec<serde_json::Value>> {
        let aggregate = payload.config.aggregate.trim();
        if !aggregate.is_empty() {
            return self
                .fetch_by_ids(
                    "WAREHOUSE_ATS_VACANCY_TEMPORARY",
                    "id",
                    &[aggregate.to_string()],
                )
                .await;
        }

        let limit = if payload.config.chunk > 0 {
            payload.config.chunk
        } else {
            100
        };
        let source = self.source.get("warehouse_ats_vacancy_temporary")?;
        let (rows, _) = source
            .extract(None, limit, &LoadEnum::Select(None, None))
            .await?;

        Ok(rows)
    }
}

#[async_trait::async_trait]
impl SyncDataset for LsiVacancySyncDataset {
    async fn format(&self, payload: &SyncPayloadDataset) -> anyhow::Result<SycnTableResult> {
        let vacancy_rows = self.vacancy_rows(payload).await?;
        let vacancy_ids = Self::vacancy_ids(&vacancy_rows);

        let custom_field_rows = self
            .fetch_by_ids(
                "WAREHOUSE_ATS_VACANCY_CUSTOM_FIELD",
                "parent_id",
                &vacancy_ids,
            )
            .await?;
        let stage_rows = self
            .fetch_by_ids("WAREHOUSE_ATS_VACANCY_STAGE", "vacancy_id", &vacancy_ids)
            .await?;
        let requirement_rows = self
            .fetch_by_ids(
                "WAREHOUSE_ATS_VACANCY_REQUIREMENT",
                "vacancy_id",
                &vacancy_ids,
            )
            .await?;
        let pic_rows = self
            .fetch_by_ids("WAREHOUSE_ATS_VACANCY_PIC", "vacancy_id", &vacancy_ids)
            .await?;

        let mut sources = HashMap::new();
        sources.insert(
            Self::VACANCY_TABLE.to_string(),
            Self::mapping(Self::VACANCY_COLUMNS, vacancy_rows),
        );
        sources.insert(
            Self::CUSTOM_FIELD_TABLE.to_string(),
            Self::mapping(Self::CUSTOM_FIELD_COLUMNS, custom_field_rows),
        );
        sources.insert(
            Self::STAGE_TABLE.to_string(),
            Self::mapping(Self::STAGE_COLUMNS, stage_rows),
        );
        sources.insert(
            Self::REQUIREMENT_TABLE.to_string(),
            Self::mapping(Self::REQUIREMENT_COLUMNS, requirement_rows),
        );
        sources.insert(
            Self::PIC_TABLE.to_string(),
            Self::mapping(Self::PIC_COLUMNS, pic_rows),
        );

        Ok(SycnTableResult {
            config: payload.config.clone(),
            sources: SourceSyncConfig { sources },
        })
    }
}
