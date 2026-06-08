use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Clone)]
pub struct JobStruct {
    pub pipeline: String,
    pub tenant: Option<String>,
    pub job_type: String,
    pub source: String,
}

#[async_trait::async_trait]
pub trait JobHandler {
    async fn handler(&self) -> Result<()>;
}
