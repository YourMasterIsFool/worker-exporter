use crate::core::traits::extractor::build_query_trait::TableEnum;

pub enum OrganizationTableEnum {
    JobPosition,
    Tenant,
    OrganizationUnit,
    JobGrade,
    JobLevel,
}

impl TableEnum for OrganizationTableEnum {
    fn table_name(&self) -> &str {
        match self {
            OrganizationTableEnum::JobPosition => "core.job_positions",
            OrganizationTableEnum::Tenant => "core.tenants",
            OrganizationTableEnum::OrganizationUnit => "core.organization_units",
            OrganizationTableEnum::JobLevel => "core.job_levels",
            OrganizationTableEnum::JobGrade => "core.job_grades",
        }
    }
}
