use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, serde::Serialize)]
pub struct WhereField {
    pub field: String,
    pub operator: String,
    pub value: Option<String>,
    pub values: Option<Vec<String>>,
}
