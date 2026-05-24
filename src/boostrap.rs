use tracing::{error, info};
use tracing_subscriber::{fmt, EnvFilter};

use crate::{
    app_state::AppState,
    application::{
        handler::ingestion_handler::IngestionHandler, load::build_load_registry,
        orchestrator::orchestrator_registry::build_orchestrator_registry,
        pipelines::pipeline_registry::build_pipeline_registry,
        source::source_registry::build_source_registry, writter::build_writter_registry,
    },
    core::{
        config::{config::Config, database_manager::DatabaseManager},
        traits::{
            consumer::{Consumer, ConsumerMessage},
            handler::JobHandler,
            orchestrator,
        },
    },
    infrastructure::{
        ingestion::organization::organization_postgres_client::OrganizatinPostgresClient,
        redis::RedisStreamConsumer,
        warehouse::{logging_warehouse::LoggingWarehouse, warehouse_client::WarehouseClient},
    },
    repo::WarehouseRepo,
};
use redis::{aio::ConnectionManager, Client};
use std::sync::Arc;

pub fn init_logger() {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,sqlx=debug"));

    fmt().with_env_filter(filter).with_target(false).init();
}

pub async fn bootstrap() -> anyhow::Result<()> {
    init_logger();
    // 1. config
    let config = Config::new();

    // 2. database
    let database_manager = Arc::new(
        DatabaseManager::new(&config)
            .await
            .expect("DATABASE MANAGER ERROR"),
    );

    // 3. clients
    let warehouse_client = Arc::new(
        WarehouseClient::new(database_manager.warehouse.clone())
            .expect("failed to create warehouse client"),
    );

    let organization_client = Arc::new(OrganizatinPostgresClient::new(
        database_manager.organization.clone(),
    ));

    let incremental_load_registry = Arc::new(build_load_registry(warehouse_client.clone()));

    // 4. source registry
    let source_registry = Arc::new(build_source_registry(organization_client.clone()));
    let writter_registry = Arc::new(build_writter_registry());
    let pipeline_registry = Arc::new(build_pipeline_registry(
        incremental_load_registry.clone(),
        source_registry.clone(),
    ));

    let warehouse_logging = Arc::new(LoggingWarehouse {
        client: warehouse_client.clone(),
    });
    let orchestrator = Arc::new(build_orchestrator_registry(
        writter_registry.clone(),
        pipeline_registry.clone(),
        incremental_load_registry.clone(),
        warehouse_logging.clone(),
    ));

    // repo
    let warehouse_repo = WarehouseRepo::new(warehouse_client.clone());

    // 5. app state
    let state = Arc::new(AppState {
        warehouse_client,
        organization_db: organization_client,
        source_registry: source_registry,
        incremental_load_registry: incremental_load_registry,
        warehouse_repo: Arc::new(warehouse_repo),
        orchestrator_registry: orchestrator,
    });

    // 6. redis
    let redis_client =
        Client::open(std::env::var("REDIS_URL").unwrap_or("redis://127.0.0.1:6379".to_string()))?;
    let manager = Arc::new(ConnectionManager::new(redis_client).await?);

    // 7. setup stream groups
    setup_stream_groups(manager.clone()).await?;

    let ingestion_consumer: Arc<dyn Consumer<Message = ConsumerMessage> + Send + Sync> =
        Arc::new(RedisStreamConsumer {
            client: manager.clone(),
            stream_key: "stream:ingestion".to_string(),
            group: "ingestion-group".to_string(),
            consumer_name: "consumer-1".to_string(),
        });
    // 8. handlers
    let ingestion_handler = IngestionHandler {
        app_state: state.clone(),
        consumer: ingestion_consumer,
    };

    // 9. jalankan — tambah handler baru tinggal tambah di sini

    tokio::spawn(async move {
        if let Err(e) = ingestion_handler.handler().await {
            error!(error = ?e, "ingestion handler crashed");
        }
    });

    // ⛔ THIS WAS MISSING
    tokio::signal::ctrl_c().await?;
    info!("shutdown signal received");
    Ok(())
}

async fn setup_stream_groups(manager: Arc<ConnectionManager>) -> anyhow::Result<()> {
    let mut conn = manager.as_ref().clone();

    let streams = vec![
        ("stream:ingestion", "ingestion-group"),
        ("stream:export", "export-group"),
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
            Ok(_) => println!("group {} created", group),
            Err(e) if e.to_string().contains("BUSYGROUP") => {
                println!("group {} already exists, skip", group)
            }
            Err(e) => return Err(anyhow::anyhow!(e)),
        }
    }

    Ok(())
}
