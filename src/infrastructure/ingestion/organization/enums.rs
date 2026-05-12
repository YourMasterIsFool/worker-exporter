pub enum OrganizationTableEnum {
    Tenant,
}

impl OrganizationTableEnum {
    pub fn postgres(&self) -> &'static str {
        "core.tenants"
    }
}
