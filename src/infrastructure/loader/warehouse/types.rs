pub enum WarehouseTemporaryTable {
    AtsVacancyTemporary,
}

impl WarehouseTemporaryTable {
    pub fn table_name(&self) -> &str {
        match self {
            WarehouseTemporaryTable::AtsVacancyTemporary => "warehouse.ats_vacancy_temporary",
        }
    }
}
