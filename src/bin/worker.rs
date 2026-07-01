use std::sync::Arc;

use anyhow::Result;
use redis::aio::ConnectionManager;
use worker_exporter::application::handler::exporter_handler::ExportHandler;
use worker_exporter::application::handler::ingestion_handler::IngestionHandler;
use worker_exporter::boostrap::bootstrap;
use worker_exporter::core::traits::consumer::{Consumer, ConsumerMessage};
use worker_exporter::core::traits::handler::JobHandler;
use worker_exporter::infrastructure::redis::RedisStreamConsumer;
// Tambahkan import ini untuk mengaktifkan try_init

#[global_allocator]
static GLOBAL: tracing_tracy::client::ProfiledAllocator<std::alloc::System> =
    tracing_tracy::client::ProfiledAllocator::new(std::alloc::System, 100);

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

#[tokio::main]
async fn main() -> Result<()> {
    // 1. bootstrap → sudah include init_logger + setup_stream_groups
    let state = bootstrap().await?;
    let manager = state.manager.clone();

    // 2. ingestion — 1 worker (pindah dari bootstrap.rs ke sini)
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
        if let Err(e) = ingestion_handler.handler().await {
            tracing::error!("ingestion handler crashed: {:?}", e);
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

        let export_handler = ExportHandler {
            app_state: state.clone(),
            consumer: export_consumer,
        };

        tokio::spawn(async move {
            if let Err(err) = export_handler.handler().await {
                tracing::error!("exporter  handler crashed: {:?}", err);
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
