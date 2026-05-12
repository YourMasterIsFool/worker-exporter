use anyhow::Result;

 
#[async_trait::async_trait]
 pub trait TenantEnricher<T> {
 	async fn enrich(&self, rows: &mut Vec<T>) -> Result<()>;
 }