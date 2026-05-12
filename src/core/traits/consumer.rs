use anyhow::Result;

#[derive(Debug)]
pub struct ConsumerMessage {
    pub id: String,
    pub payload: String,
}

#[async_trait::async_trait]
pub trait Consumer: Send + Sync {
    type Message;
    async fn consume(&self) -> Result<Vec<Self::Message>>;
    async fn ack(&self, message_id: &str) -> Result<()>;
}
