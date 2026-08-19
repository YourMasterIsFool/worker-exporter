use std::sync::Arc;

use crate::{
    application::rollback_loader_registry::RollbackLoaderRegistry,
    core::traits::pipeline::pipeline_trait::RollbackPipeline,
    infrastructure::db::ats_client::AtsClient,
};

/// Executes configured delete loaders atomically. Configure child tables
/// before their parent table to satisfy foreign keys.
pub struct ConfigRollbackPipeline {
    pub loaders: Arc<RollbackLoaderRegistry>,
    pub ats_client: Arc<AtsClient>,
}

#[async_trait::async_trait]
impl RollbackPipeline for ConfigRollbackPipeline {
    async fn rollback(&self, logging_id: &str, loaders: &[String]) -> anyhow::Result<i64> {
        if loaders.is_empty() {
            anyhow::bail!("rollback_config.loaders wajib diisi");
        }

        let mut tx = self.ats_client.pool.begin().await?;
        let mut deleted = 0;
        for loader_name in loaders {
            deleted += self
                .loaders
                .get(loader_name)?
                .rollback(&mut tx, logging_id)
                .await?;
        }
        tx.commit().await?;
        Ok(deleted)
    }
}
