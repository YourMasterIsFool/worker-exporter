use std::sync::Arc;

use axum::extract::State;
use axum::response::{IntoResponse, Redirect};
use axum::routing::{get, post};
use axum::{Json, Router};

use export_rust::application::presentation::http::list_logging_http::list_log_by_tenant_id_http;
use export_rust::application::presentation::http::post_detail_file_http::post_detail_file_http;
use export_rust::errors::AppError;
use export_rust::{
    app_state::{self, AppState},
    application::grpcs::facilitator::{self, S3FacilitatorGrpc},
    boostrap,
    core::config::config::Config,
};

use serde::Deserialize;
use tracing::info;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

pub fn init_logger() {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,sqlx=debug"));

    // 2. Gabungkan format logger terminal dengan TracyLayer di sini
    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer().with_target(false))
        .with(tracing_tracy::TracyLayer::default())
        .try_init();
}

// 2. WAJIB TAMBAHKAN MACRO DESERIALIZE DI SINI

fn create_app(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/detail-file", post(post_detail_file_http))
        .route("/loggings", post(list_log_by_tenant_id_http))
        .with_state(state) // Axum sekarang membagikan Arc<AppState> yang cocok dengan handler
}

#[tokio::main]
async fn main() {
    init_logger(); // Aktifkan logger Anda

    if let Ok(bootstrap_data) = boostrap::bootstrap().await {
        // 2. PERBAIKAN UTAMA: Bungkus objek bootstrap_data ke dalam Arc di sini!
        // Ini memastikan state yang dikloning dan dibagikan bertipe Arc<AppState>
        let state = bootstrap_data;
        let app = create_app(state.clone());
        let config = Config::new();

        let host = format!("{}:{}", config.host, config.port);
        let listener = tokio::net::TcpListener::bind(&host)
            .await
            .expect("http failed to bind");

        info!("axum server running into {}", &host);

        axum::serve(listener, app)
            .await
            .expect("http failed to serve");
    } else {
        eprintln!("failed to bootstrap");
        return;
    }
}
