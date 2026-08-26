use serde_json::Value;

pub struct VacancySyncTable {
    pub table: &'static str,
    pub columns: &'static [&'static str],
}

pub struct VacancyChildSyncFetcher {
    pub fetcher: &'static str,
    pub filter_field: &'static str,
    pub table: VacancySyncTable,
}

pub struct VacancySyncFetcher;

impl VacancySyncFetcher {
    pub const MASTER_FETCHER: &'static str = "WAREHOUSE_ATS_VACANCY_TEMPORARY";
    pub const MASTER_SOURCE: &'static str = "warehouse_ats_vacancy_temporary";

    pub const VACANCY_COLUMNS: &'static [&'static str] = &[
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
        "job_function_ids",
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

    pub const CUSTOM_FIELD_COLUMNS: &'static [&'static str] = &[
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
        "logging_id",
    ];

    pub const STAGE_COLUMNS: &'static [&'static str] = &[
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

    pub const REQUIREMENT_COLUMNS: &'static [&'static str] = &[
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

    pub const PIC_COLUMNS: &'static [&'static str] = &[
        "id",
        "vacancy_id",
        "employee_id",
        "employee_meta",
        "is_verified",
        "created_at",
        "logging_id",
    ];

    pub const PUBLISH_ON_COLUMNS: &'static [&'static str] = &[
        "id",
        "updated_at",
        "created_at",
        "deleted_at",
        "vacancy_id",
        "platform",
        "status",
        "careersite_tenant_id",
    ];

    pub fn master_table() -> VacancySyncTable {
        VacancySyncTable {
            table: "warehouse.ats_vacancy_temporary",
            columns: Self::VACANCY_COLUMNS,
        }
    }

    pub fn child_fetchers() -> Vec<VacancyChildSyncFetcher> {
        vec![
            VacancyChildSyncFetcher {
                fetcher: "WAREHOUSE_ATS_VACANCY_CUSTOM_FIELD",
                filter_field: "parent_id",
                table: VacancySyncTable {
                    table: "warehouse.ats_vacancy_custom_field",
                    columns: Self::CUSTOM_FIELD_COLUMNS,
                },
            },
            VacancyChildSyncFetcher {
                fetcher: "WAREHOUSE_ATS_VACANCY_STAGE",
                filter_field: "vacancy_id",
                table: VacancySyncTable {
                    table: "warehouse.ats_vacancy_stage",
                    columns: Self::STAGE_COLUMNS,
                },
            },
            VacancyChildSyncFetcher {
                fetcher: "WAREHOUSE_ATS_VACANCY_REQUIREMENT",
                filter_field: "vacancy_id",
                table: VacancySyncTable {
                    table: "warehouse.ats_vacancy_requirement",
                    columns: Self::REQUIREMENT_COLUMNS,
                },
            },
            VacancyChildSyncFetcher {
                fetcher: "WAREHOUSE_ATS_VACANCY_PIC",
                filter_field: "vacancy_id",
                table: VacancySyncTable {
                    table: "warehouse.ats_vacancy_pic",
                    columns: Self::PIC_COLUMNS,
                },
            },
            VacancyChildSyncFetcher {
                fetcher: "WAREHOUSE_ATS_VACANCY_PUBLISH_ON",
                filter_field: "vacancy_id",
                table: VacancySyncTable {
                    table: "warehouse.vacancy_publish_on",
                    columns: Self::PUBLISH_ON_COLUMNS,
                },
            },
        ]
    }

    pub fn master_ids(rows: &[Value]) -> Vec<String> {
        rows.iter()
            .filter_map(|row| row.get("id").and_then(|value| value.as_str()))
            .map(ToString::to_string)
            .collect()
    }
}
