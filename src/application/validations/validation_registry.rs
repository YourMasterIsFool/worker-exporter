use std::sync::Arc;

use serde_json::Value;

use crate::core::{
    traits::validator::{Validator, ValidatorState},
    types::ValidationConfig,
};

pub struct ValidationRegistry {
    validators: std::collections::HashMap<String, Arc<dyn Validator>>,
}

pub struct PreparedValidation {
    pub config: ValidationConfig,
    pub validator: Arc<dyn Validator>,
    pub state: ValidatorState,
}

impl ValidationRegistry {
    pub fn new() -> Self {
        Self {
            validators: std::collections::HashMap::new(),
        }
    }

    pub fn register<V>(&mut self, name: impl Into<String>, validator: V)
    where
        V: Validator + 'static,
    {
        self.validators
            .insert(name.into().trim().to_uppercase(), Arc::new(validator));
    }

    pub fn get(&self, name: &str) -> anyhow::Result<Arc<dyn Validator>> {
        self.validators
            .get(&name.trim().to_uppercase())
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("validator '{}' is not registered", name))
    }
}

impl Default for ValidationRegistry {
    fn default() -> Self {
        Self::new()
    }
}

pub async fn prepare_validations(
    registry: &ValidationRegistry,
    values: &[Value],
    configs: &[ValidationConfig],
) -> anyhow::Result<Vec<PreparedValidation>> {
    let mut prepared = Vec::with_capacity(configs.len());

    for config in configs {
        let validator = registry.get(&config.validator)?;
        let state = validator.prepare_batch(values, &config.options).await?;

        prepared.push(PreparedValidation {
            config: config.clone(),
            validator,
            state,
        });
    }

    Ok(prepared)
}

pub fn validate_value(value: &Value, prepared: &mut [PreparedValidation]) -> Vec<anyhow::Error> {
    prepared
        .iter_mut()
        .filter_map(|item| {
            item.validator
                .validate(value, &item.config.options, item.state.as_mut())
                .err()
        })
        .collect()
}
