#[derive(Debug, Clone)]
pub struct OrchestratorJob {
    pub(crate) tenant_id: String,
    pub(crate) limit: i64,
    pub(crate) source: String,
    pub(crate) orchestrator_type: String, // pub(crate) fields: Vec<ExportFields>,
}
#[async_trait::async_trait]
pub trait Orchestrator: Send + Sync {
    async fn run(&self, job: &OrchestratorJob, msg_id: &str) -> anyhow::Result<()>;
}
