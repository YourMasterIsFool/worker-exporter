use tokio::net::unix::pipe;
use tracing::error;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use crate::application::aggregate_pipeline_registry::build_aggregate_pipeline_registry;
use crate::application::bulk_loader_registry::build_bulk_loader_registry;
use crate::application::dataset::{
    build_importer_dataset_registry, build_importer_dataset_registry_v1,
    build_sync_dataset_registry,
};
use crate::application::fetch_source_regitry;
use crate::application::import::import_registry::{
    build_import_loader_registry, ImportLoaderRegistry,
};
use crate::application::loader_registry::{self, build_loader_registry};
use crate::application::pipelines::pipeline_import_registry::{
    self, build_pipeline_import_registry,
};
use crate::application::pipelines::rollback_pipeline_registry::build_rollback_pipeline_registry;
use crate::application::rollback_loader_registry::build_rollback_loader_registry;
use crate::application::sync_aggregator_registry::build_sync_aggregator_registry;
use crate::infrastructure::db::warehouse_client::WarehouseClient;
use crate::infrastructure::loader;
use crate::infrastructure::loader::warehouse::logging_warehouse::LoggingWarehouse;
use crate::infrastructure::loader::warehouse::warehouse_logging_import::WarehouseLoggingImport;
use crate::{
    app_state::AppState,
    application::{
        dataset::build_dataset_registry, fetch_source_regitry::build_data_fetcher_json_registry,
        grpcs::gprc::GrpcService, ingestion::source_registry::build_source_registry,
        orchestrator::orchestrator_registry::build_orchestrator_registry,
        pipelines::pipeline_registry::build_pipeline_registry, repositories::Repositories,
        usecases::usecases::Usecases, writter::build_writter_registry,
    },
    core::config::{config::Config, database_manager::DatabaseManager},
    infrastructure::{
        db::{
            ats_client::AtsClient, db_manager::DatabaseInfraManager, master_client::MasterClient,
        },
        redis::RedisPublisher,
        source::oltp::organization::organization_postgres_client::OrganizatinPostgresClient,
    },
    repo::WarehouseRepo,
};
use redis::{aio::ConnectionManager, Client};
use std::sync::Arc;

pub fn init_logger() {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,sqlx=debug"));

    // 2. Gabungkan format logger terminal dengan TracyLayer di sini
    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer().with_target(false))
        .try_init();
}

pub async fn bootstrap() -> anyhow::Result<Arc<AppState>> {
    init_logger();

    // 1. config
    let config = Config::new();

    let grpc_service = Arc::new(GrpcService::new(&config));

    let redis_client = Client::open(config.redis_url.as_str())?;
    let manager = Arc::new(ConnectionManager::new(redis_client).await.map_err(|err| {
        error!("REDIS CONNECTION {err}");
        anyhow::anyhow!(err)
    })?);

    let redis_publisher = Arc::new(RedisPublisher {
        client: manager.clone(),
    });

    // defining pool in database manager
    let database_manager = Arc::new(
        DatabaseManager::new(&config)
            .await
            .expect("DATABASE MANAGER ERROR"),
    );

    // defining database infra manager
    let database_infra_manager = Arc::new(DatabaseInfraManager::new(database_manager.clone()));

    // defining repositories

    // 3. clients
    let warehouse_client = Arc::new(
        WarehouseClient::new(database_manager.warehouse.clone())
            .expect("failed to create warehouse client"),
    );

    let ats_client = Arc::new(AtsClient::new(database_manager.ats.clone()));
    let master_client = Arc::new(MasterClient::new(database_manager.master.clone()));

    let organization_client = Arc::new(OrganizatinPostgresClient::new(
        database_manager.organization.clone(),
    ));

    let loader_registry = Arc::new(build_loader_registry(database_infra_manager.clone()));
    let bulk_loader_registry = Arc::new(build_bulk_loader_registry(ats_client.clone()));
    let rollback_loader_registry = Arc::new(build_rollback_loader_registry());
    let rollback_pipeline_registry = Arc::new(build_rollback_pipeline_registry(
        rollback_loader_registry,
        ats_client.clone(),
    ));
    let aggregate_pipeline_registry =
        Arc::new(build_aggregate_pipeline_registry(ats_client.clone()));
    // 4. source registry
    let source_registry = Arc::new(build_source_registry(
        organization_client.clone(),
        ats_client.clone(),
        warehouse_client.clone(),
        master_client.clone(),
    ));
    let writter_registry = Arc::new(build_writter_registry());
    let pipeline_registry = Arc::new(build_pipeline_registry(
        // incremental_load_registry.clone(),
        source_registry.clone(),
        redis_publisher.clone(),
    ));
    let data_fetcher_registry = Arc::new(build_data_fetcher_json_registry(
        database_infra_manager.clone(),
    ));

    let importer_registry = Arc::new(build_importer_dataset_registry(
        data_fetcher_registry.clone(),
    ));

    let import_dataset_v1_registry = Arc::new(build_importer_dataset_registry_v1(
        data_fetcher_registry.clone(),
    ));
    let warehouse_logging = Arc::new(LoggingWarehouse {
        client: warehouse_client.clone(),
    });
    let import_logging = Arc::new(WarehouseLoggingImport::new(warehouse_client.clone()));
    let sync_dataset_registry = Arc::new(build_sync_dataset_registry(
        data_fetcher_registry.clone(),
        source_registry.clone(),
        import_logging.clone(),
    ));
    let dataset_registry = Arc::new(build_dataset_registry(
        database_manager.clone(),
        warehouse_client.clone(),
        ats_client.clone(),
        data_fetcher_registry.clone(),
        source_registry.clone(),
        grpc_service.clone(),
    ));

    let sync_aggregator_registry = Arc::new(build_sync_aggregator_registry(
        source_registry.clone(),
        sync_dataset_registry.clone(),
        bulk_loader_registry.clone(),
        ats_client.clone(),
        warehouse_logging.clone(),
        import_logging,
    ));

    let importer_load_registry =
        Arc::new(build_import_loader_registry(database_infra_manager.clone()));
    let pipeline_import_registry = Arc::new(build_pipeline_import_registry(importer_load_registry));
    let orchestrator = Arc::new(build_orchestrator_registry(
        writter_registry.clone(),
        pipeline_registry.clone(),
        loader_registry.clone(),
        warehouse_logging.clone(),
        dataset_registry.clone(),
        grpc_service.clone(),
        importer_registry.clone(),
        pipeline_import_registry.clone(),
        redis_publisher.clone(),
        import_dataset_v1_registry.clone(),
        data_fetcher_registry.clone(),
        sync_aggregator_registry.clone(),
        rollback_pipeline_registry,
        aggregate_pipeline_registry,
    ));

    // repo
    let _warehouse_repo = WarehouseRepo::new(warehouse_client.clone());
    let repositories = Arc::new(Repositories::new(database_infra_manager.clone()));
    let usecases = Arc::new(Usecases::new(
        repositories.clone(),
        data_fetcher_registry.clone(),
    ));
    // 5. app state
    let state = Arc::new(AppState {
        // warehouse_client,
        // organization_db: organization_client,
        // source_registry,
        // incremental_load_registry,
        // warehouse_repo: Arc::new(warehouse_repo),
        orchestrator_registry: orchestrator,
        grpc_service,
        usecases,
        publisher: redis_publisher.clone(),
        manager: manager.clone(),
    });

    Ok(state.clone())
}

pub async fn boostrap_worker(state: &AppState) -> anyhow::Result<()> {
    let manager = state.manager.clone();

    setup_stream_groups(manager.clone()).await?;
    Ok(())
}

pub async fn boostrap_http(state: &AppState) -> anyhow::Result<()> {
    Ok(())
}

async fn setup_stream_groups(manager: Arc<ConnectionManager>) -> anyhow::Result<()> {
    let mut conn = manager.as_ref().clone();

    let streams = vec![
        ("stream:ingestion", "ingestion-group"),
        ("stream:export", "export-group"),
        ("stream:import", "import-group"),
        ("stream:sync", "sync-group"),
        ("stream:rollback", "rollback-group"),
        ("stream:aggregate", "aggregate-group"),
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
