pub enum WarehouseTemporaryTable {
    AtsVacancyTemporary,
    AtsVacancyStage,
    AtsVacancyRequirement,
    AtsVacancyCustomField,
    AtsVacancyPic,
    AtsVacancyPublishOn,
}

impl WarehouseTemporaryTable {
    pub fn table_name(&self) -> &str {
        match self {
            WarehouseTemporaryTable::AtsVacancyTemporary => "warehouse.ats_vacancy_temporary",
            WarehouseTemporaryTable::AtsVacancyStage => "warehouse.ats_vacancy_stage",
            WarehouseTemporaryTable::AtsVacancyRequirement => "warehouse.ats_vacancy_requirement",
            WarehouseTemporaryTable::AtsVacancyCustomField => "warehouse.ats_vacancy_custom_field",
            WarehouseTemporaryTable::AtsVacancyPic => "warehouse.ats_vacancy_pic",
            WarehouseTemporaryTable::AtsVacancyPublishOn => "warehouse.ats_vacancy_publish_on",
        }
    }
}
