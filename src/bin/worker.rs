use std::sync::Arc;

use anyhow::Result;

use worker_exporter::application::handler::exporter_handler::ExportHandler;
use worker_exporter::application::handler::import_handler::ImportHandler;
use worker_exporter::application::handler::ingestion_handler::IngestionHandler;
use worker_exporter::application::handler::sync_handler::SyncHandler;
use worker_exporter::boostrap::bootstrap;
use worker_exporter::core::traits::consumer::{Consumer, ConsumerMessage};
use worker_exporter::core::traits::handler::JobHandler;
use worker_exporter::infrastructure::redis::RedisStreamConsumer;
#[cfg(all(feature = "jemalloc", not(feature = "dhat-heap")))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;
#[tokio::main]
async fn main() -> Result<()> {
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();
    // 1. bootstrap → sudah include init_logger + setup_stream_groups
    let state = bootstrap().await?;
    let manager = state.manager.clone();

    // 2. ingestion — 1 worker (pindah dari bootstrap.rs ke sini)
    let mut conn = manager.as_ref().clone();

    let streams = vec![
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
            Ok(_) => println!("group {} created", group),
            Err(e) if e.to_string().contains("BUSYGROUP") => {
                println!("group {} already exists, skip", group)
            }
            Err(e) => return Err(anyhow::anyhow!(e)),
        }
    }

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
        if let Err(err) = ingestion_handler.handler().await {
            tracing::error!(?err, "ingestion handler crashed");
        }
    });

    // 3. export — 3 worker
    for i in 1..=3 {
        let export_consumer: Arc<dyn Consumer<Message = ConsumerMessage> + Send + Sync> =
            Arc::new(RedisStreamConsumer {
                client: manager.clone(),
                stream_key: "stream:export".to_string(),
                group: "export-group".to_string(),
                consumer_name: format!("export-worker-{}", i),
            });

        let import_consumer: Arc<dyn Consumer<Message = ConsumerMessage> + Send + Sync> =
            Arc::new(RedisStreamConsumer {
                client: manager.clone(),
                stream_key: "stream:import".to_string(),
                group: "import-group".to_string(),
                consumer_name: format!("import-worker-{}", i),
            });

        let sync_consumer: Arc<dyn Consumer<Message = ConsumerMessage> + Send + Sync> =
            Arc::new(RedisStreamConsumer {
                client: manager.clone(),
                stream_key: "stream:sync".to_string(),
                group: "sync-group".to_string(),
                consumer_name: format!("sync-worker-{}", i),
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
            if let Err(err) = export_handler.handler().await {
                tracing::error!("exporter  handler crashed: {:?}", err);
            }
        });

        tokio::spawn(async move {
            if let Err(err) = import_handler.handler().await {
                tracing::error!("import handler crashed: {:?}", err);
            }
        });

        tokio::spawn(async move {
            if let Err(err) = sync_handler.handler().await {
                tracing::error!("sync handler crashed: {:?}", err);
            }
        });
    }

    // 4. keep alive
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::debug!("shutdown signal received");
        }
    }

    Ok(())
}
