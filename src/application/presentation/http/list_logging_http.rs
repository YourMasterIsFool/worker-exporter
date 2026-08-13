use std::{env, sync::Arc};

use axum::{
    extract::{Path, Query, State},
    http::HeaderMap,
    Json,
};
use chrono::{DateTime, NaiveDateTime};
use chrono_tz::Tz;
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
#[serde(deny_unknown_fields)]
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
<<<<<<< HEAD
    pub actor_id: String,
=======
    pub created_at: Option<DateTime<Tz>>,
>>>>>>> 97a56dc (feat✨(WORKER:TECHS-1001): adding timezone resolve and adding chunk resolve)
    pub logging_type: String,
    pub status: String,
    pub email: String,
    pub tenant_id: String,
    pub total_chunk: i32,
    pub current_chunk: i32,
    pub processed_count: i32,
<<<<<<< HEAD
    pub completed_at: Option<chrono::NaiveDateTime>,
=======
    pub total_count: i64,
    pub completed_at: Option<DateTime<Tz>>,
>>>>>>> 97a56dc (feat✨(WORKER:TECHS-1001): adding timezone resolve and adding chunk resolve)
    pub download_url: Option<String>,
    pub status: String,
    // pub error: Option<serde_json::Value>,
    // pub additional_info: Option<serde_json::Value>,
    pub cursor: Option<String>,
    pub stream_id: String,
    pub created_at: Option<chrono::NaiveDateTime>,
}

impl ListLoggingItem {
    fn from_logging(logging: LoggingEntity, timezone: Tz) -> Self {
        Self {
            id: logging.id,
<<<<<<< HEAD
            actor_id: logging.actor_id,
=======
            created_at: resolve_timestamp(logging.created_at, timezone),
>>>>>>> 97a56dc (feat✨(WORKER:TECHS-1001): adding timezone resolve and adding chunk resolve)
            email: logging.email,
            tenant_id: logging.tenant_id,
            total_chunk: logging.total_chunk,
            current_chunk: logging.current_chunk,
            processed_count: logging.processed_count,
<<<<<<< HEAD
            completed_at: logging.completed_at,
=======
            total_count: logging.total_count,
            completed_at: resolve_timestamp(logging.completed_at, timezone),
>>>>>>> 97a56dc (feat✨(WORKER:TECHS-1001): adding timezone resolve and adding chunk resolve)
            download_url: logging.download_url,
            status: logging.status,
            // error: logging.error,
            // additional_info: logging.additional_info,
            cursor: logging.cursor,
            stream_id: logging.stream_id,
            logging_type: logging.logging_type,
<<<<<<< HEAD
            created_at: logging.created_at,
=======
            status: logging.status,
>>>>>>> 97a56dc (feat✨(WORKER:TECHS-1001): adding timezone resolve and adding chunk resolve)
        }
    }
}

fn resolve_timestamp(timestamp: Option<NaiveDateTime>, timezone: Tz) -> Option<DateTime<Tz>> {
    timestamp.map(|timestamp| timestamp.and_utc().with_timezone(&timezone))
}

fn request_timezone(headers: &HeaderMap) -> Result<Tz, AppError> {
    let timezone = headers
        .get("X-TIMEZONE")
        .map(|value| value.to_str())
        .transpose()
        .map_err(|_| AppError::ValidationError("X-TIMEZONE must be valid text".to_string()))?
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .or_else(|| env::var("TIMEZONE").ok())
        .unwrap_or_else(|| "UTC".to_string());

    timezone.parse().map_err(|_| {
        AppError::ValidationError(format!(
            "invalid timezone {timezone}; use an IANA name such as Asia/Makassar"
        ))
    })
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
    headers: HeaderMap,
    Query(pagination): Query<PaginationQuery>,
    Json(payload): Json<PayloadLogging>,
) -> Result<Json<ApiResponse<Paginated<ListLoggingItem>>>, AppError> {
    let timezone = request_timezone(&headers)?;
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
                    .map(|logging| ListLoggingItem::from_logging(logging, timezone))
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
    use axum::http::{HeaderMap, HeaderValue};
    use chrono::NaiveDate;
    use chrono_tz::Asia::Makassar;

    use crate::domain::logging::entity::LoggingFilter;

    use super::{request_timezone, resolve_timestamp, ListLoggingItem, PayloadLogging};

    #[test]
    fn accepts_filter_and_sort_aliases_without_a_view_field() {
        let payload: PayloadLogging = serde_json::from_value(serde_json::json!({
            "_auth_context": {},
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
            "_auth_context": {},
            "filter": {},
            "view": ["createdAt"],
            "sort": { "createdAt": "DESC" }
        }));

        assert!(payload.is_err());
    }

    #[test]
    fn accepts_only_export_or_import_logging_type_filter() {
        let payload: PayloadLogging = serde_json::from_value(serde_json::json!({
            "_auth_context": {},
            "filter": { "loggingType": "IMPORT" }
        }))
        .unwrap();
        let filter: LoggingFilter = payload.filter.into();

        assert_eq!(filter.logging_type.as_deref(), Some("IMPORT"));
        assert!(serde_json::from_value::<PayloadLogging>(serde_json::json!({
            "_auth_context": {},
            "filter": { "loggingType": "SYNC" }
        }))
        .is_err());
    }

    #[test]
    fn list_item_serializes_all_fields_except_config() {
        let item = ListLoggingItem {
            id: Some("logging-1".to_string()),
<<<<<<< HEAD
            actor_id: "actor-1".to_string(),
=======
            created_at: None,
            logging_type: "EXPORT".to_string(),
            status: "COMPLETED".to_string(),
>>>>>>> 97a56dc (feat✨(WORKER:TECHS-1001): adding timezone resolve and adding chunk resolve)
            email: "user@example.test".to_string(),
            tenant_id: "tenant-1".to_string(),
            total_chunk: 4,
            current_chunk: 2,
            processed_count: 20,
            completed_at: None,
            download_url: None,
            status: "COMPLETED".to_string(),
            cursor: Some("cursor-1".to_string()),
            stream_id: "stream-1".to_string(),
            logging_type: "EXPORT".to_string(),
            created_at: None,
        };

        assert_eq!(
            serde_json::to_value(item).unwrap(),
            serde_json::json!({
                "id": "logging-1",
<<<<<<< HEAD
                "actor_id": "actor-1",
=======
                "created_at": null,
                "logging_type": "EXPORT",
                "status": "COMPLETED",
>>>>>>> 97a56dc (feat✨(WORKER:TECHS-1001): adding timezone resolve and adding chunk resolve)
                "email": "user@example.test",
                "tenant_id": "tenant-1",
                "total_chunk": 4,
                "current_chunk": 2,
                "processed_count": 20,
                "completed_at": null,
                "download_url": null,
                "status": "COMPLETED",

                "cursor": "cursor-1",
                "stream_id": "stream-1",
                "logging_type": "EXPORT",
                "created_at": null
            })
        );
    }

    #[test]
    fn timestamps_use_x_timezone() {
        let mut headers = HeaderMap::new();
        headers.insert("X-TIMEZONE", HeaderValue::from_static("Asia/Makassar"));

        let resolved = request_timezone(&headers).unwrap();
        let timestamp = NaiveDate::from_ymd_opt(2026, 8, 13)
            .unwrap()
            .and_hms_opt(8, 0, 0)
            .unwrap();

        assert_eq!(resolved, Makassar);
        assert_eq!(
            resolve_timestamp(Some(timestamp), resolved)
                .unwrap()
                .to_rfc3339(),
            "2026-08-13T16:00:00+08:00"
        );
    }
}
