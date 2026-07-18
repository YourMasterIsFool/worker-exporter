use serde_json::Value;

use crate::core::traits::validator::{Validator, ValidatorState};

pub struct RequiredValidator;

#[async_trait::async_trait]
impl Validator for RequiredValidator {
    async fn prepare_batch(
        &self,
        _values: &[Value],
        _options: &Value,
    ) -> anyhow::Result<ValidatorState> {
        Ok(Box::new(()))
    }

    fn validate(
        &self,
        value: &Value,
        _options: &Value,
        _state: &mut dyn std::any::Any,
    ) -> anyhow::Result<()> {
        let invalid = match value {
            Value::Null => true,
            Value::String(value) => value.trim().is_empty(),
            Value::Array(values) => values.is_empty(),
            _ => false,
        };

        if invalid {
            anyhow::bail!("Required");
        }

        Ok(())
    }

    fn validator_name(&self) -> &'static str {
        "REQUIRED"
    }
}
