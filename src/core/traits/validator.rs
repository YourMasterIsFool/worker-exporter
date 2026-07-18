use std::any::Any;

pub type ValidatorState = Box<dyn Any + Send + Sync>;

#[async_trait::async_trait]
pub trait Validator: Send + Sync {
    async fn prepare_batch(
        &self,
        values: &[serde_json::Value],
        options: &serde_json::Value,
    ) -> anyhow::Result<ValidatorState>;

    fn validate(
        &self,
        value: &serde_json::Value,
        options: &serde_json::Value,
        state: &mut dyn Any,
    ) -> anyhow::Result<()>;

    fn validator_name(&self) -> &'static str;
}
