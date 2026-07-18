use serde::{Deserialize, Serialize};

pub const DEFAULT_PAGE: u32 = 1;
pub const DEFAULT_LIMIT: u32 = 10;
pub const MAX_LIMIT: u32 = 100;

/// Pagination parameters shared by list endpoints.
#[derive(Clone, Copy, Debug, Deserialize)]
pub struct PaginationQuery {
    #[serde(default = "default_page")]
    page: u32,
    #[serde(default = "default_limit")]
    limit: u32,
}

impl Default for PaginationQuery {
    fn default() -> Self {
        Self {
            page: DEFAULT_PAGE,
            limit: DEFAULT_LIMIT,
        }
    }
}

impl PaginationQuery {
    pub fn page(&self) -> u32 {
        self.page.max(DEFAULT_PAGE)
    }

    pub fn limit(&self) -> u32 {
        self.limit.clamp(1, MAX_LIMIT)
    }

    pub fn offset(&self) -> i64 {
        i64::from(self.page().saturating_sub(1)) * i64::from(self.limit())
    }
}

/// Standard paginated payload used inside API responses.
#[derive(Debug, Serialize)]
pub struct Paginated<T> {
    pub content: Vec<T>,
    #[serde(rename = "totalRecord")]
    pub total_record: i64,
    pub page: String,
    #[serde(rename = "pageSize")]
    pub page_size: String,
}

impl<T> Paginated<T> {
    pub fn new(content: Vec<T>, total_record: i64, pagination: PaginationQuery) -> Self {
        Self {
            content,
            total_record,
            page: pagination.page().to_string(),
            page_size: pagination.limit().to_string(),
        }
    }
}

/// Standard successful HTTP response envelope for endpoints returning data.
#[derive(Debug, Serialize)]
pub struct ApiResponse<T> {
    pub data: T,
    pub error: Option<serde_json::Value>,
    pub meta: ResponseMeta,
}

#[derive(Debug, Serialize)]
pub struct ResponseMeta {
    pub success: bool,
    #[serde(rename = "statusCode")]
    pub status_code: u16,
}

impl<T> ApiResponse<T> {
    pub fn success(data: T) -> Self {
        Self {
            data,
            error: None,
            meta: ResponseMeta {
                success: true,
                status_code: 200,
            },
        }
    }
}

const fn default_page() -> u32 {
    DEFAULT_PAGE
}

const fn default_limit() -> u32 {
    DEFAULT_LIMIT
}

#[cfg(test)]
mod tests {
    use super::{ApiResponse, Paginated, PaginationQuery, DEFAULT_LIMIT, DEFAULT_PAGE, MAX_LIMIT};

    #[test]
    fn normalizes_invalid_pagination_values() {
        let pagination = PaginationQuery { page: 0, limit: 0 };

        assert_eq!(pagination.page(), DEFAULT_PAGE);
        assert_eq!(pagination.limit(), 1);
        assert_eq!(pagination.offset(), 0);
    }

    #[test]
    fn caps_the_page_size_and_serializes_it_as_a_string() {
        let pagination = PaginationQuery {
            page: 2,
            limit: MAX_LIMIT + 1,
        };
        let response = Paginated::new(vec!["item"], 33, pagination);

        assert_eq!(response.page, "2");
        assert_eq!(response.page_size, MAX_LIMIT.to_string());
        assert_eq!(DEFAULT_LIMIT, 10);
    }

    #[test]
    fn serializes_the_standard_api_envelope() {
        let response = ApiResponse::success(Paginated::new(
            Vec::<String>::new(),
            33,
            PaginationQuery::default(),
        ));

        assert_eq!(
            serde_json::to_value(response).unwrap(),
            serde_json::json!({
                "data": {
                    "content": [],
                    "totalRecord": 33,
                    "page": "1",
                    "pageSize": "10"
                },
                "error": null,
                "meta": {
                    "success": true,
                    "statusCode": 200
                }
            })
        );
    }
}
