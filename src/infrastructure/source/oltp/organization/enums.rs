use crate::core::traits::extractor::build_query_trait::TableEnum;

pub enum OrganizationTableEnum {
    JobPosition,
    JobPositionCustomField,
    Tenant,
    OrganizationUnit,
    JobGrade,
    JobLevel,
    OnboardingItems,
    OnboardingTemplate,
}

impl TableEnum for OrganizationTableEnum {
    fn table_name(&self) -> &str {
        match self {
            OrganizationTableEnum::JobPosition => "core.job_positions",
            OrganizationTableEnum::JobPositionCustomField => "core.job_position_custom_fields",
            OrganizationTableEnum::Tenant => "core.tenants",
            OrganizationTableEnum::OrganizationUnit => "core.organization_units",
            OrganizationTableEnum::JobLevel => "core.job_levels",
            OrganizationTableEnum::JobGrade => "core.job_grades",
            OrganizationTableEnum::OnboardingItems => "document.onboarding_template_items",
            OrganizationTableEnum::OnboardingTemplate => "document.onboarding_templates",
        }
    }
}
