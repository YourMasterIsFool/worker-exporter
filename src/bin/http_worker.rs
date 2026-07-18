use std::sync::Arc;

use anyhow::Result;
use axum::routing::{get, post};
use axum::Router;
use tracing::debug;

use worker_exporter::app_state::AppState;
use worker_exporter::application::handler::exporter_handler::ExportHandler;
use worker_exporter::application::handler::import_handler::ImportHandler;
use worker_exporter::application::handler::ingestion_handler::IngestionHandler;
use worker_exporter::application::handler::sync_handler::SyncHandler;
use worker_exporter::application::presentation::http::healthz_http::healthz_http;
use worker_exporter::application::presentation::http::list_logging_http::{
    get_import_logging_detail_http, list_log_by_tenant_id_http,
};
use worker_exporter::application::presentation::http::payload_generated_import_template::generated_import_template_http;
use worker_exporter::application::presentation::http::post_detail_file_http::post_detail_file_http;
use worker_exporter::application::presentation::http::test_import_http::test_import_http;
use worker_exporter::boostrap::bootstrap;
use worker_exporter::core::config::config::Config;
use worker_exporter::core::traits::consumer::{Consumer, ConsumerMessage};
use worker_exporter::core::traits::handler::JobHandler;
use worker_exporter::infrastructure::redis::RedisStreamConsumer;

fn create_app(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/healthz", axum::routing::get(healthz_http))
        .route("/detail-file", post(post_detail_file_http))
        .route("/loggings", post(list_log_by_tenant_id_http))
        .route(
            "/loggings/:logging_id/import-detail",
            get(get_import_logging_detail_http),
        )
        .route("/generated-template", post(generated_import_template_http))
        .route("/test-import", post(test_import_http))
        .with_state(state)
}

async fn setup_stream_groups(state: &Arc<AppState>) -> Result<()> {
    let mut conn = state.manager.as_ref().clone();
    let streams = [
        ("stream:ingestion", "ingestion-group"),
        ("stream:export", "export-group"),
        ("stream:import", "import-group"),
        ("stream:sync", "sync-group"),
    ];

    for (stream, group) in streams {
        let result: redis::RedisResult<()> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(stream)
            .arg(group)
            .arg("$")
            .arg("MKSTREAM")
            .query_async(&mut conn)
            .await;

        match result {
            Ok(()) => debug!(%group, "redis consumer group created"),
            Err(error) if error.to_string().contains("BUSYGROUP") => {
                debug!(%group, "redis consumer group already exists")
            }
            Err(error) => return Err(error.into()),
        }
    }

    Ok(())
}

async fn start_workers(state: Arc<AppState>) -> Result<()> {
    setup_stream_groups(&state).await?;
    let manager = state.manager.clone();

    let ingestion_consumer: Arc<dyn Consumer<Message = ConsumerMessage> + Send + Sync> =
        Arc::new(RedisStreamConsumer {
            client: manager.clone(),
            stream_key: "stream:ingestion".to_string(),
            group: "ingestion-group".to_string(),
            consumer_name: "ingestion-worker-1".to_string(),
        });
    let ingestion_handler = IngestionHandler {
        app_state: state.clone(),
        consumer: ingestion_consumer,
    };
    tokio::spawn(async move {
        if let Err(error) = ingestion_handler.handler().await {
            tracing::error!(?error, "ingestion worker crashed");
        }
    });

    for worker_number in 1..=3 {
        let export_consumer: Arc<dyn Consumer<Message = ConsumerMessage> + Send + Sync> =
            Arc::new(RedisStreamConsumer {
                client: manager.clone(),
                stream_key: "stream:export".to_string(),
                group: "export-group".to_string(),
                consumer_name: format!("export-worker-{worker_number}"),
            });
        let import_consumer: Arc<dyn Consumer<Message = ConsumerMessage> + Send + Sync> =
            Arc::new(RedisStreamConsumer {
                client: manager.clone(),
                stream_key: "stream:import".to_string(),
                group: "import-group".to_string(),
                consumer_name: format!("import-worker-{worker_number}"),
            });
        let sync_consumer: Arc<dyn Consumer<Message = ConsumerMessage> + Send + Sync> =
            Arc::new(RedisStreamConsumer {
                client: manager.clone(),
                stream_key: "stream:sync".to_string(),
                group: "sync-group".to_string(),
                consumer_name: format!("sync-worker-{worker_number}"),
            });

        let export_handler = ExportHandler {
            app_state: state.clone(),
            consumer: export_consumer,
        };
        let import_handler = ImportHandler {
            app_state: state.clone(),
            consumer: import_consumer,
        };
        let sync_handler = SyncHandler {
            app_state: state.clone(),
            consumer: sync_consumer,
        };

        tokio::spawn(async move {
            if let Err(error) = export_handler.handler().await {
                tracing::error!(?error, "export worker crashed");
            }
        });
        tokio::spawn(async move {
            if let Err(error) = import_handler.handler().await {
                tracing::error!(?error, "import worker crashed");
            }
        });
        tokio::spawn(async move {
            if let Err(error) = sync_handler.handler().await {
                tracing::error!(?error, "sync worker crashed");
            }
        });
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let state = bootstrap().await?;
    let config = Config::new();
    let host = format!("{}:{}", config.host, config.port);
    let listener = tokio::net::TcpListener::bind(&host).await?;
    let app = create_app(state.clone());

    start_workers(state).await?;
    tracing::info!(%host, "http server and workers are running");

    tokio::select! {
        result = axum::serve(listener, app) => result?,
        result = tokio::signal::ctrl_c() => result?,
    }

    debug!("shutdown signal received");
    Ok(())
}
