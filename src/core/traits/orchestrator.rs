use chrono::NaiveDate;
use serde::{Deserialize, Serialize};

use crate::core::types::ValidationConfig;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct CustomJsonKey {
    pub column: String,
    pub value: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub options_relation: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ExporterColumn {
    pub name: String,
    #[serde(default)]
    pub column: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fetcher: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fetcher_field_filter: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fetcher_field_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fetcher_key: Option<String>,
    #[serde(default)]
    pub is_cache: bool,
    #[serde(default)]
    pub relation: Option<String>,
    #[serde(default)]
    pub custom_key: Option<String>,
    #[serde(default)]
    pub transform: Option<String>,
    #[serde(default, rename = "default")]
    pub default_value: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_json_key: Option<CustomJsonKey>,
}
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ExporterConfig {
    pub fields: Vec<ExporterColumn>,
    pub dataset: String,
    pub format: String,
    pub template_code: String,
    pub structure_template_code: String,
    pub redirected_url: String,
    pub chunk: i64,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DynamicExportCursor {
    pub column: String,
    #[serde(default = "default_cursor_direction")]
    pub direction: String,
}

fn default_cursor_direction() -> String {
    "asc".into()
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DynamicExportSource {
    pub database: String,
    pub schema: String,
    pub table: String,
    #[serde(default)]
    pub cursor: Option<DynamicExportCursor>,
    #[serde(default)]
    pub tenant_field: Option<String>,
    #[serde(default)]
    pub tenant_operator: Option<String>,
    #[serde(default)]
    pub where_fields: Vec<crate::core::types::WhereField>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DynamicExportRelation {
    pub name: String,
    #[serde(default)]
    pub depends_on: Option<String>,
    #[serde(default)]
    pub filter_source: bool,
    #[serde(default)]
    pub filter_fields: Vec<String>,
    pub source: DynamicExportSource,
    pub local_key: String,
    pub foreign_key: String,
    pub cardinality: String,
    #[serde(default)]
    pub is_custom: bool,
    #[serde(default)]
    pub is_cache: bool,
    #[serde(default)]
    pub field_key_column: Option<String>,
    #[serde(default)]
    pub field_type_column: Option<String>,
    #[serde(default)]
    pub value_columns: Option<std::collections::HashMap<String, String>>,
    #[serde(default)]
    pub order_by: Option<DynamicExportCursor>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DynamicExportConfig {
    pub dataset: String,
    pub format: String,
    pub fields: Vec<ExporterColumn>,
    pub source: DynamicExportSource,
    #[serde(default)]
    pub relations: Vec<DynamicExportRelation>,
    pub template_code: String,
    pub structure_template_code: String,
    pub redirected_url: String,
    pub chunk: i64,
}

impl DynamicExportConfig {
    pub fn export_config(&self) -> ExporterConfig {
        ExporterConfig {
            fields: self.fields.clone(),
            dataset: self.dataset.clone(),
            format: self.format.clone(),
            template_code: self.template_code.clone(),
            structure_template_code: self.structure_template_code.clone(),
            redirected_url: self.redirected_url.clone(),
            chunk: self.chunk,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct IngestionConfig {
    #[serde(default)]
    pub dataset: Option<String>,
    pub source: String,
    pub loader: String,
    #[serde(default = "default_ingestion_pipeline")]
    pub pipeline: String,
    #[serde(default = "default_ingestion_chunk")]
    pub chunk: i64,
}

fn default_ingestion_pipeline() -> String {
    "ingestion".to_string()
}

fn default_ingestion_chunk() -> i64 {
    500
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct SyncConfig {
    #[serde(default = "default_sync_aggregate")]
    pub sync_aggregate: String,
    pub logging_id: Option<String>,
    #[serde(default)]
    pub chunk: i64,
    /// Required by `CONFIG_SYNC_PIPELINE`; ignored by legacy sync aggregators.
    #[serde(default)]
    pub source: Option<String>,
    /// Required by `CONFIG_SYNC_PIPELINE`; ignored by legacy sync aggregators.
    #[serde(default)]
    pub dataset: Option<String>,
    /// Bulk-loader registry keys, executed in order by `CONFIG_SYNC_PIPELINE`.
    #[serde(default)]
    pub loaders: Vec<String>,
}

fn default_sync_aggregate() -> String {
    "LSI_VACANCY_SYNC".to_string()
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RollbackConfig {
    #[serde(default = "default_rollback_pipeline")]
    pub pipeline: String,
    pub logging_id: String,
    /// Rollback-loader registry keys, executed in order in one transaction.
    #[serde(default)]
    pub loaders: Vec<String>,
}

fn default_rollback_pipeline() -> String {
    "CONFIG_ROLLBACK_PIPELINE".to_string()
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct AggregateConfig {
    #[serde(default = "default_aggregate_pipeline")]
    pub pipeline: String,
    #[serde(default = "default_candidate_aggregate_code")]
    pub aggregate_code: String,
    /// `GLOBAL` when omitted; use a tenant ID to scope the result explicitly.
    #[serde(default)]
    pub scope_key: Option<String>,
    /// Inclusive lower bound for source.created_at.
    #[serde(default)]
    pub from: Option<NaiveDate>,
    /// Exclusive upper bound for source.created_at.
    #[serde(default)]
    pub to: Option<NaiveDate>,
}

fn default_aggregate_pipeline() -> String {
    "CANDIDATE_AGGREGATE".to_string()
}

fn default_candidate_aggregate_code() -> String {
    "CANDIDATE_TOTAL".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TypeDataEnum {
    #[serde(rename = "TEXT")]
    Text,
    #[serde(rename = "UUID")]
    Uuid,
    #[serde(rename = "DATE")]
    Date,
    #[serde(rename = "NUMBER")]
    Number,
    #[serde(rename = "Boolean")]
    Boolean,
}

impl From<TypeDataEnum> for String {
    fn from(value: TypeDataEnum) -> Self {
        match value {
            TypeDataEnum::Boolean => "BOOLEAN".into(),
            TypeDataEnum::Uuid => "UUID".into(),
            TypeDataEnum::Date => "DATE".into(),
            TypeDataEnum::Number => "NUMBER".into(),
            TypeDataEnum::Boolean => "Boolean".into(),
            TypeDataEnum::Text => "TEXT".into(),
        }
    }
}
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ImporterColumn {
    pub header: String,
    pub column: String,
    #[serde(default)]
    pub select: Option<String>,
    pub fetcher: Option<String>,
    #[serde(default, alias = "isCache")]
    pub is_cache: bool,
    #[serde(default)]
    pub template: Option<serde_json::Value>,
    pub type_data: Option<TypeDataEnum>,
    pub is_custom: bool,
    pub fetcher_field_filter: Option<String>,
    pub fetcher_field_key: Option<String>,
    pub fetcher_key: Option<String>,
    pub casting: Option<crate::core::types::CastingEnum>,
    #[serde(
        default,
        alias = "validations",
        alias = "validation_config",
        alias = "validationConfig"
    )]
    pub validation_rules: Option<Vec<ValidationConfig>>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct SequentialTable {
    pub(crate) table: String,
    pub is_custom: bool,
    pub fk_column: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Filterable {
    pub main_column: String,
    pub import_column: String,
    #[serde(default, alias = "isArray")]
    pub is_array: bool,
    #[serde(default, alias = "sourceColumn")]
    pub source_column: Option<String>,
    #[serde(
        default,
        alias = "fieldKey",
        alias = "customFieldKey",
        alias = "custom_field_key"
    )]
    pub field_key: Option<String>,
    #[serde(default, alias = "valueColumn")]
    pub value_column: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BackfieldCofig {
    pub stream: String,
    pub payload: BackfieldPayload,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BackfieldPayload {
    #[serde(alias = "tenantId")]
    pub tenant_id: String,

    #[serde(alias = "configType")]
    pub config_type: String,

    #[serde(alias = "tableName")]
    pub table_name: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ImporterConfig {
    pub fields: Vec<ImporterColumn>,
    pub chunk: i32,
    pub tenant_id: String,
    pub group_id: String,
    pub dataset: String,
    pub auto_sync: bool,
    pub main_filterable: Option<Vec<Filterable>>,
    pub main_searchable: Option<Vec<String>>,
    pub key: String,
    pub data_source_type: String,
    #[serde(default = "default_import_pipeline")]
    pub pipeline: String,
    /// Legacy single-loader form. Use `loaders` for a composed pipeline.
    #[serde(default)]
    pub loader: String,
    /// Import-loader registry keys, executed in order by `CONFIG_IMPORT_PIPELINE`.
    #[serde(default)]
    pub loaders: Vec<String>,
    #[serde(alias = "main_table")]
    pub master_table: String,
    pub primary_column: String,
    pub sequential_table: Option<Vec<SequentialTable>>,
    pub fetcher_caching: Option<Vec<String>>,
    pub backfield_config: Option<BackfieldCofig>,

    // start row for reading excel
    // example start from row 1
    pub start_row: Option<u64>,
    pub permission_key: Option<Vec<String>>,
}

fn default_import_pipeline() -> String {
    "CONFIG_IMPORT_PIPELINE".to_string()
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct OrchestratorJob {
    pub(crate) tenant_id: String,
    pub(crate) title: Option<String>,
    pub(crate) email: String,
    pub(crate) actor_id: String,
    pub(crate) permissions_key: Option<Vec<String>>,

    pub(crate) orchestrator_name: String,
    pub(crate) export_config: Option<ExporterConfig>,
    pub(crate) dynamic_export_config: Option<DynamicExportConfig>,
    pub(crate) ingestion_config: Option<IngestionConfig>,
    pub(crate) import_config: Option<ImporterConfig>,
    pub(crate) sync_config: Option<SyncConfig>,
    pub(crate) rollback_config: Option<RollbackConfig>,
    pub(crate) aggregate_config: Option<AggregateConfig>,
    pub(crate) filters: Option<Vec<crate::core::types::WhereField>>, // pub(crate) fields: Vec<ExportFields>,
}
#[async_trait::async_trait]
pub trait Orchestrator: Send + Sync {
    async fn run(&self, job: &OrchestratorJob, msg_id: &str) -> anyhow::Result<()>;
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::OrchestratorJob;

    #[test]
    fn parses_config_driven_import_sync_and_rollback() {
        let job: OrchestratorJob = serde_json::from_value(json!({
            "tenant_id": "tenant-id",
            "email": "user@example.com",
            "actor_id": "actor-id",
            "orchestrator_name": "IMPORT",
            "import_config": {
                "fields": [{
                    "header": "Requirement Template",
                    "column": "warehouse.ats_vacancy_temporary:recruitment_form_template_id|warehouse.ats_vacancy_custom_field:recruitment_form_template_id",
                    "select": "warehouse.ats_vacancy_custom_field:recruitment_form_template_id:meta:name",
                    "fetcher": "REQUIREMENT_TEMPLATE",
                    "type_data": "UUID",
                    "is_custom": true,
                    "fetcher_field_filter": null,
                    "fetcher_field_key": "name",
                    "fetcher_key": "id",
                    "casting": "Varchar"
                }],
                "chunk": 100,
                "tenant_id": "tenant-id",
                "group_id": "group-id",
                "dataset": "DYNAMIC_IMPORT_DATASET",
                "auto_sync": false,
                "key": "file-key",
                "data_source_type": "stream",
                "master_table": "warehouse.ats_vacancy_temporary",
                "primary_column": "id",
                "loaders": ["WAREHOUSE_DYNAMIC_TEMPORARY_LOADER"]
            },
            "sync_config": {
                "sync_aggregate": "CONFIG_SYNC_PIPELINE",
                "logging_id": "logging-id",
                "source": "warehouse_ats_vacancy_temporary",
                "dataset": "LSI_VACANCY_SYNC_DATASET",
                "loaders": ["ATS_VACANCY"]
            },
            "rollback_config": {
                "logging_id": "logging-id",
                "loaders": ["ATS_VACANCY_ROLLBACK"]
            },
            "aggregate_config": {}
        }))
        .expect("config-driven job must deserialize");

        assert_eq!(
            job.import_config.as_ref().unwrap().pipeline,
            "CONFIG_IMPORT_PIPELINE"
        );
        assert_eq!(
            job.rollback_config.as_ref().unwrap().pipeline,
            "CONFIG_ROLLBACK_PIPELINE"
        );
        assert_eq!(
            serde_json::to_value(&job).unwrap()["import_config"]["fields"][0]["select"],
            "warehouse.ats_vacancy_custom_field:recruitment_form_template_id:meta:name"
        );
        let aggregate_config = job.aggregate_config.unwrap();
        assert_eq!(aggregate_config.pipeline, "CANDIDATE_AGGREGATE");
        assert_eq!(aggregate_config.aggregate_code, "CANDIDATE_TOTAL");
    }
}
