pub enum HrisIngestionTable {
    AttedanceLocations,
}

impl HrisIngestionTable {
    pub fn table_name(&self) -> &str {
        match self {
            HrisIngestionTable::AttedanceLocations => "attendances.attendance_locations",
        }
    }
}
