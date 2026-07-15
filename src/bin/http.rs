use std::sync::Arc;

use axum::routing::post;
use axum::Router;

use worker_exporter::application::presentation::http::list_logging_http::list_log_by_tenant_id_http;
use worker_exporter::application::presentation::http::healthz_http::healthz_http;
use worker_exporter::application::presentation::http::payload_generated_import_template::generated_import_template_http;
use worker_exporter::application::presentation::http::post_detail_file_http::post_detail_file_http;

use worker_exporter::application::presentation::http::test_import_http::test_import_http;
use worker_exporter::{app_state::AppState, boostrap, core::config::config::Config};

use tracing::debug;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

pub fn init_logger() {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,sqlx=debug"));

    // 2. Gabungkan format logger terminal dengan TracyLayer di sini
    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer().with_target(false))
        .try_init();
}

// 2. WAJIB TAMBAHKAN MACRO DESERIALIZE DI SINI

fn create_app(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/healthz", axum::routing::get(healthz_http))
        .route("/detail-file", post(post_detail_file_http))
        .route("/loggings", post(list_log_by_tenant_id_http))
        .route("/generated-template", post(generated_import_template_http))
        .route("/test-import", post(test_import_http))
        .with_state(state) // Axum sekarang membagikan Arc<AppState> yang cocok dengan handler
}

#[tokio::main]
async fn main() {
    init_logger(); // Aktifkan logger Anda

    match boostrap::bootstrap().await {
        Ok(bootstrap_data) => {
            // 2. PERBAIKAN UTAMA: Bungkus objek bootstrap_data ke dalam Arc di sini!
            // Ini memastikan state yang dikloning dan dibagikan bertipe Arc<AppState>
            let state = bootstrap_data;
            let app = create_app(state.clone());
            let config = Config::new();

            let host = format!("{}:{}", config.host, config.port);
            let listener = tokio::net::TcpListener::bind(&host)
                .await
                .expect("http failed to bind");

            debug!("axum server running into {}", &host);

            axum::serve(listener, app)
                .await
                .expect("http failed to serve");
        }
        Err(e) => {
            eprintln!("failed to bootstrap: {}", e);
            return;
        }
    }
}
