pub enum HrisIngestionTable {
    AttedanceLocations,
    EmploymentStatuses,
    Employee,
}

impl HrisIngestionTable {
    pub fn table_name(&self) -> &str {
        match self {
            HrisIngestionTable::AttedanceLocations => "attendances.attendance_locations",
            HrisIngestionTable::EmploymentStatuses => "employees.employment_statuses",
            HrisIngestionTable::Employee => "employees.employees",
        }
    }
}
