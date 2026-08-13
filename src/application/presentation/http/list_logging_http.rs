use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    Json,
};
use serde::{Deserialize, Serialize};

use crate::{
    app_state::AppState,
    application::types::AuthContextPayload,
    core::pagination::{ApiResponse, Paginated, PaginationQuery},
    domain::logging::{
        entity::{LoggingEntity, LoggingFilter, LoggingSort, LoggingSortField, SortDirection},
        repo::ListLoggingRequest,
    },
    errors::AppError,
    infrastructure::loader::warehouse::temporary::warehouse_dynamic_temporary_loader::WarehouseDynamicTemporaryLoader,
};

#[derive(Debug, Default, Deserialize)]
pub struct PayloadLogging {
    pub _auth_context: AuthContextPayload,
    #[serde(default, alias = "filters")]
    pub filter: LoggingFilterPayload,
    #[serde(default, alias = "sort_by", alias = "sortBy")]
    pub sort: LoggingSortPayload,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LoggingFilterPayload {
    pub id: Option<String>,
    #[serde(alias = "actor_id")]
    pub actor_id: Option<String>,
    pub email: Option<String>,
    #[serde(alias = "tenant_id")]
    pub tenant_id: Option<String>,
    #[serde(alias = "logging_type")]
    pub logging_type: Option<LoggingTypeFilter>,
    pub status: Option<String>,
    #[serde(alias = "stream_id")]
    pub stream_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum LoggingTypeFilter {
    Export,
    Import,
}

#[derive(Debug, Serialize)]
pub struct ListLoggingItem {
    pub id: Option<String>,
    pub logging_type: String,
    pub email: String,
    pub total_chunk: i32,
    pub current_chunk: i32,
    pub processed_count: i32,
    pub total_count: i64,
    pub completed_at: Option<chrono::NaiveDateTime>,
    pub download_url: Option<String>,
}

impl From<LoggingEntity> for ListLoggingItem {
    fn from(logging: LoggingEntity) -> Self {
        Self {
            id: logging.id,
            email: logging.email,
            total_chunk: logging.total_chunk,
            current_chunk: logging.current_chunk,
            processed_count: logging.processed_count,
            total_count: logging.total_count,
            completed_at: logging.completed_at,
            download_url: logging.download_url,
            logging_type: logging.logging_type,
        }
    }
}

impl LoggingTypeFilter {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Export => "EXPORT",
            Self::Import => "IMPORT",
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LoggingSortPayload {
    pub id: Option<SortDirection>,
    #[serde(alias = "actor_id")]
    pub actor_id: Option<SortDirection>,
    pub email: Option<SortDirection>,
    #[serde(alias = "tenant_id")]
    pub tenant_id: Option<SortDirection>,
    #[serde(alias = "logging_type")]
    pub logging_type: Option<SortDirection>,
    pub status: Option<SortDirection>,
    #[serde(alias = "stream_id")]
    pub stream_id: Option<SortDirection>,
    #[serde(alias = "created_at")]
    pub created_at: Option<SortDirection>,
    #[serde(alias = "completed_at")]
    pub completed_at: Option<SortDirection>,
}

impl From<LoggingFilterPayload> for LoggingFilter {
    fn from(value: LoggingFilterPayload) -> Self {
        Self {
            id: value.id,
            actor_id: value.actor_id,
            email: value.email,
            tenant_id: value.tenant_id,
            logging_type: value
                .logging_type
                .map(|logging_type| logging_type.as_str().to_string()),
            status: value.status,
            stream_id: value.stream_id,
        }
    }
}

impl LoggingSortPayload {
    fn into_sorts(self) -> Vec<LoggingSort> {
        let mut sorts = Vec::new();

        macro_rules! add_sort {
            ($field:expr, $direction:expr) => {
                if let Some(direction) = $direction {
                    sorts.push(LoggingSort {
                        field: $field,
                        direction,
                    });
                }
            };
        }

        add_sort!(LoggingSortField::Id, self.id);
        add_sort!(LoggingSortField::ActorId, self.actor_id);
        add_sort!(LoggingSortField::Email, self.email);
        add_sort!(LoggingSortField::TenantId, self.tenant_id);
        add_sort!(LoggingSortField::LoggingType, self.logging_type);
        add_sort!(LoggingSortField::Status, self.status);
        add_sort!(LoggingSortField::StreamId, self.stream_id);
        add_sort!(LoggingSortField::CreatedAt, self.created_at);
        add_sort!(LoggingSortField::CompletedAt, self.completed_at);

        sorts
    }
}

pub async fn list_log_by_tenant_id_http(
    State(state): State<Arc<AppState>>,
    Query(pagination): Query<PaginationQuery>,
    Json(payload): Json<PayloadLogging>,
) -> Result<Json<ApiResponse<Paginated<ListLoggingItem>>>, AppError> {
    let usecases = state.usecases.clone();
    let usecase = usecases.list_log_by_tenant_id_usceases.clone();
    let auth_context = payload._auth_context;
    let mut payload_filter = payload.filter;

    payload_filter.tenant_id = auth_context.tenant_id;
    let request = ListLoggingRequest {
        pagination,
        filter: payload_filter.into(),
        sort: payload.sort.into_sorts(),
    };
    let log_entries = usecase.execute(&request).await;

    match log_entries {
        Ok(log_entries) => {
            let response = Paginated::new(
                log_entries
                    .content
                    .into_iter()
                    .map(ListLoggingItem::from)
                    .collect(),
                log_entries.total_record,
                pagination,
            );
            Ok(Json(ApiResponse::success(response)))
        }
        Err(err) => Err(AppError::InternalServerError(err.to_string())),
    }
}

pub async fn get_import_logging_detail_http(
    State(state): State<Arc<AppState>>,
    Path(logging_id): Path<String>,
) -> Result<
    Json<
        ApiResponse<
            crate::application::usecases::get_import_logging_detail_usecase::ImportLoggingDetail,
        >,
    >,
    AppError,
> {
    let detail = state
        .usecases
        .get_import_logging_detail_usecase
        .execute(&logging_id)
        .await?;

    Ok(Json(ApiResponse::success(detail)))
}

#[cfg(test)]
mod tests {
    use crate::domain::logging::entity::LoggingFilter;

    use super::{ListLoggingItem, PayloadLogging};

    #[test]
    fn accepts_filter_and_sort_aliases_without_a_view_field() {
        let payload: PayloadLogging = serde_json::from_value(serde_json::json!({
            "filters": { "tenantId": "tenant-1" },
            "sort_by": { "createdAt": "DESC" }
        }))
        .unwrap();

        assert_eq!(payload.filter.tenant_id.as_deref(), Some("tenant-1"));
        assert_eq!(payload.sort.into_sorts().len(), 1);
    }

    #[test]
    fn rejects_an_unsupported_view_field() {
        let payload = serde_json::from_value::<PayloadLogging>(serde_json::json!({
            "filter": {},
            "view": ["createdAt"],
            "sort": { "createdAt": "DESC" }
        }));

        assert!(payload.is_err());
    }

    #[test]
    fn accepts_only_export_or_import_logging_type_filter() {
        let payload: PayloadLogging = serde_json::from_value(serde_json::json!({
            "filter": { "loggingType": "IMPORT" }
        }))
        .unwrap();
        let filter: LoggingFilter = payload.filter.into();

        assert_eq!(filter.logging_type.as_deref(), Some("IMPORT"));
        assert!(serde_json::from_value::<PayloadLogging>(serde_json::json!({
            "filter": { "loggingType": "SYNC" }
        }))
        .is_err());
    }

    #[test]
    fn list_item_serializes_only_summary_fields() {
        let item = ListLoggingItem {
            id: Some("logging-1".to_string()),
            logging_type: "EXPORT".to_string(),
            email: "user@example.test".to_string(),
            total_chunk: 4,
            current_chunk: 2,
            processed_count: 20,
            total_count: 40,
            completed_at: None,
            download_url: None,
        };

        assert_eq!(
            serde_json::to_value(item).unwrap(),
            serde_json::json!({
                "id": "logging-1",
                "logging_type": "EXPORT",
                "email": "user@example.test",
                "total_chunk": 4,
                "current_chunk": 2,
                "processed_count": 20,
                "total_count": 40,
                "completed_at": null,
                "download_url": null
            })
        );
    }
}
