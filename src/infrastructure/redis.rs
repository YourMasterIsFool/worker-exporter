use crate::core::traits::{
    consumer::{Consumer, ConsumerMessage},
    publisher::Publisher,
};
use redis::{aio::ConnectionManager, AsyncCommands};
use std::sync::Arc;
use tracing::debug;

pub struct RedisStreamConsumer {
    pub client: Arc<ConnectionManager>,
    pub stream_key: String,
    pub group: String,
    pub consumer_name: String,
}

#[async_trait::async_trait]
impl Consumer for RedisStreamConsumer {
    type Message = ConsumerMessage;

    async fn consume(&self) -> anyhow::Result<Vec<Self::Message>> {
        let mut conn = self.client.as_ref().clone(); // ← self.client bukan self.conn

        let opts = redis::streams::StreamReadOptions::default()
            .group(&self.group, &self.consumer_name)
            .count(10)
            .block(5000);

        let results: redis::streams::StreamReadReply = conn
            .xread_options(&[&self.stream_key], &[">"], &opts)
            .await?;

        let mut messages = vec![];

        for stream in results.keys {
            for entry in stream.ids {
                let payload: String = entry
                    .map
                    .get("payload")
                    .and_then(|v| match v {
                        redis::Value::Data(b) => String::from_utf8(b.clone()).ok(),
                        _ => None,
                    })
                    .ok_or_else(|| anyhow::anyhow!("missing payload field"))?;

                messages.push(ConsumerMessage {
                    id: entry.id,
                    payload,
                });
            }
        }

        Ok(messages)
    }

    async fn ack(&self, msg_id: &str) -> anyhow::Result<()> {
        let mut conn = self.client.as_ref().clone(); // ← self.client bukan self.conn
        let _: i64 = conn.xack(&self.stream_key, &self.group, &[msg_id]).await?;
        Ok(())
    }
}

pub struct RedisPublisher {
    pub client: Arc<ConnectionManager>,
}

#[async_trait::async_trait]
impl Publisher for RedisPublisher {
    async fn publish(&self, stream_key: &str, payload: serde_json::Value) -> anyhow::Result<()> {
        let mut conn = self.client.as_ref().clone();
        let serialize_payload = serde_json::to_string(&payload)?;
        let _: String = conn
            .xadd(stream_key, "*", &[("payload", serialize_payload)])
            .await?;

        debug!("send publish {:?}", payload);
        Ok(())
    }
}
