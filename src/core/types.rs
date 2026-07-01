use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, serde::Serialize)]
pub struct WhereField {
    pub field: String,
    pub operator: String,
    pub value: Option<String>,
    pub values: Option<Vec<String>>,
    pub casting: Option<CastingEnum>,
}

#[derive(Debug, Clone, Deserialize, serde::Serialize)]
pub enum CastingEnum {
    Uuid,
    Varchar,
    Enum(String),
}

impl CastingEnum {
    pub fn as_str(&self) -> &str {
        match self {
            CastingEnum::Uuid => "uuid",
            CastingEnum::Varchar => "varchar",
            CastingEnum::Enum(enum_str) => enum_str.as_str(),
        }
    }
}
