use crate::core::traits::load::LoaderPersitence;

pub struct AtsEntityMapping {
    pub entity: &'static str,
    pub table: &'static str,
    pub columns: &'static [&'static str],
    pub conflict_columns: &'static [&'static str],
    pub identity_columns: &'static [&'static str],
    pub parent_column: Option<&'static str>,
}

impl LoaderPersitence for AtsEntityMapping {
    fn table(&self) -> &'static str {
        self.table
    }

    fn columns(&self) -> &'static [&'static str] {
        self.columns
    }

    fn conflicted_columns(&self) -> &'static [&'static str] {
        self.conflict_columns
    }
}

pub const VACANCY: AtsEntityMapping = AtsEntityMapping {
    entity: "vacancy",
    table: "vacancy.vacancy",
    columns: &[
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
        "pre_onboarding_template_meta",
        "snapshot",
        "active_publish_keys",
        "created_by",
        "permission_keys",
        "job_functions",
        "job_function_ids",
        "job_position_id",
        "job_position_meta",
        "branch_id",
        "branch_meta",
        "organization_id",
        "organization_meta",
        "expired_date",
        "employment_status_id",
        "employment_status_meta",
        "work_placement_type_id",
        "work_placement_type_meta",
        "work_location_id",
        "work_location_meta",
        "province_id",
        "province_meta",
        "city_id",
        "city_meta",
        "is_confidential",
        "headcount_quota",
        "salary_min",
        "salary_max",
        "is_hide_salary",
        "currency_code",
        "is_no_expired_date",
        "version",
        "verified_status",
        "status",
        "completion_status",
        "currentHired",
        "is_locked",
        "total_hired",
        "publish_at",
        "sla",
        "logging_id",
        "keywords",
        "filterable_attribute",
    ],
    conflict_columns: &["id"],
    identity_columns: &["id"],
    parent_column: None,
};

pub const VACANCY_CUSTOM_FIELD: AtsEntityMapping = AtsEntityMapping {
    entity: "vacancy_custom_field",
    table: "vacancy.vacancy_custom_field",
    columns: &[
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
        "value_date",
        "value_number",
        "value_id",
        "meta",
        "reference_module",
        "value_bool",
        "logging_id",
    ],
    conflict_columns: &["id"],
    identity_columns: &["id"],
    parent_column: Some("parent_id"),
};

pub const VACANCY_FILTER: AtsEntityMapping = AtsEntityMapping {
    entity: "vacancy_filter",
    table: "vacancy.vacancy_filter",
    columns: &[
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
        "value_bool",
        "value_date",
    ],
    conflict_columns: &["id"],
    identity_columns: &["id"],
    parent_column: Some("parent_id"),
};

pub const VACANCY_FORM_SNAPSHOT: AtsEntityMapping = AtsEntityMapping {
    entity: "vacancy_form_snapshot",
    table: "vacancy.vacancy_form_snapshot",
    columns: &[
        "id",
        "updated_at",
        "created_at",
        "deleted_at",
        "tenant_id",
        "group_id",
        "vacancy_id",
        "original_form_id",
        "field_name",
        "field_type",
        "is_required",
        "sequence",
        "settings",
    ],
    conflict_columns: &["id"],
    identity_columns: &["id"],
    parent_column: Some("vacancy_id"),
};

pub const VACANCY_FORM_OPTION_SNAPSHOT: AtsEntityMapping = AtsEntityMapping {
    entity: "vacancy_form_option_snapshot",
    table: "vacancy.vacancy_form_option_snapshot",
    columns: &[
        "id",
        "vacancy_form_snapshot_id",
        "sequence",
        "option_value",
        "is_correct_answer",
    ],
    conflict_columns: &["id"],
    identity_columns: &["id"],
    parent_column: Some("vacancy_form_snapshot_id"),
};

pub const VACANCY_INVITATION: AtsEntityMapping = AtsEntityMapping {
    entity: "vacancy_invitation",
    table: "vacancy.vacancy_invitation",
    columns: &[
        "id",
        "updated_at",
        "created_at",
        "deleted_at",
        "vacancy_id",
        "candidate_id",
        "invited_by_employee_id",
        "invited_by_employee_meta",
        "expired_date",
        "is_no_expired_date",
    ],
    conflict_columns: &["id"],
    identity_columns: &["id"],
    parent_column: Some("vacancy_id"),
};

pub const VACANCY_PIC: AtsEntityMapping = AtsEntityMapping {
    entity: "vacancy_pic",
    table: "vacancy.vacancy_pic",
    columns: &[
        "vacancy_id",
        "employee_id",
        "employee_meta",
        "is_verified",
        "logging_id",
        "created_at",
    ],
    conflict_columns: &["vacancy_id", "employee_id"],
    identity_columns: &["vacancy_id", "employee_id"],
    parent_column: Some("vacancy_id"),
};

pub const VACANCY_PUBLISH_ON: AtsEntityMapping = AtsEntityMapping {
    entity: "vacancy_publish_on",
    table: "vacancy.vacancy_publish_on",
    columns: &[
        "id",
        "updated_at",
        "created_at",
        "deleted_at",
        "vacancy_id",
        "platform",
        "status",
        "careersite_tenant_id",
        "logging_id",
    ],
    conflict_columns: &["id"],
    identity_columns: &["id"],
    parent_column: Some("vacancy_id"),
};

pub const VACANCY_REQUIREMENT: AtsEntityMapping = AtsEntityMapping {
    entity: "vacancy_requirement",
    table: "vacancy.vacancy_requirement",
    columns: &[
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
        "is_mandatory",
        "is_range",
        "compare_operator",
        "pivot",
        "min_pivot",
        "max_pivot",
        "criteria",
        "data_mapping",
        "master_data",
        "logging_id",
    ],
    conflict_columns: &["id"],
    identity_columns: &["id"],
    parent_column: Some("vacancy_id"),
};

pub const VACANCY_SHARE_ACCESS: AtsEntityMapping = AtsEntityMapping {
    entity: "vacancy_share_access",
    table: "vacancy.vacancy_share_access",
    columns: &[
        "id",
        "updated_at",
        "created_at",
        "deleted_at",
        "tenant_id",
        "group_id",
        "vacancy_id",
        "vacancy_stage_id",
        "employee_id",
        "employee_name",
        "employee_meta",
    ],
    conflict_columns: &["id"],
    identity_columns: &["id"],
    parent_column: Some("vacancy_id"),
};

pub const VACANCY_STAGE: AtsEntityMapping = AtsEntityMapping {
    entity: "vacancy_stage",
    table: "vacancy.vacancy_stage",
    columns: &[
        "id",
        "updated_at",
        "created_at",
        "deleted_at",
        "vacancy_id",
        "tenant_id",
        "group_id",
        "original_stage_id",
        "interviewer_form_id",
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
    ],
    conflict_columns: &["id"],
    identity_columns: &["id"],
    parent_column: Some("vacancy_id"),
};

pub const VACANCY_ENTITIES: &[&AtsEntityMapping] = &[
    &VACANCY,
    &VACANCY_CUSTOM_FIELD,
    &VACANCY_FILTER,
    &VACANCY_FORM_SNAPSHOT,
    &VACANCY_FORM_OPTION_SNAPSHOT,
    &VACANCY_INVITATION,
    &VACANCY_PIC,
    &VACANCY_PUBLISH_ON,
    &VACANCY_REQUIREMENT,
    &VACANCY_SHARE_ACCESS,
    &VACANCY_STAGE,
];

#[cfg(test)]
mod tests {
    use super::VACANCY_ENTITIES;

    #[test]
    fn maps_each_vacancy_entity_to_its_oltp_table() {
        assert_eq!(VACANCY_ENTITIES.len(), 11);
        assert!(VACANCY_ENTITIES
            .iter()
            .all(|entity| entity.table.starts_with("vacancy.")));
    }
}
