use axum::{
    extract::State, // Hapus Path karena tidak dipakai di POST ini
    response::{IntoResponse, Redirect},
    Json,
};
use serde::Deserialize;
use std::sync::Arc;
use tracing::debug;
// 1. Tambahkan import ini

use crate::{app_state::AppState, errors::AppError};

#[derive(Deserialize)]
pub struct GetDetailFilePayload {
    pub keys: String,
}

pub async fn post_detail_file_http(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<GetDetailFilePayload>,
) -> impl IntoResponse {
    let keys = payload.keys.clone();

    let download_url_handler = state
        .grpc_service
        .s3_facilitator_grpc
        .get_download_url(vec![keys.clone()])
        .await;

    match download_url_handler {
        Ok(grpc_response) => {
            let response_inner = grpc_response.into_inner();

            if let Some(item) = response_inner.items.first() {
                let aws_presigned_url = &item.url;
                debug!("Redirecting user to S3 for file ID: {}", keys);

                Redirect::to(aws_presigned_url).into_response()
            } else {
                debug!(
                    "gRPC return success but items list is empty for ID: {}",
                    keys
                );
                AppError::NotFound(format!("download keys not found {}", keys)).into_response()
            }
        }
        Err(grpc_status) => {
            debug!(
                "gRPC Error or download keys {} not found. Status: {}",
                keys,
                grpc_status.message()
            );

            AppError::NotFound(format!("download keys not found {}", keys)).into_response()
        }
    }
}
