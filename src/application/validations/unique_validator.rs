use std::{any::Any, collections::HashSet, sync::Arc};

use serde::Deserialize;
use serde_json::Value;

use crate::{
    application::fetch_source_regitry::DataFetcherJsonRegistry,
    core::{
        traits::validator::{Validator, ValidatorState},
        types::{CastingEnum, WhereField},
    },
    utils::custom_field_value,
};

#[derive(Debug, Clone, Deserialize)]
struct UniqueOptions {
    fetcher: String,
    key: String,
    result_key: String,
    #[serde(default)]
    case_insensitive: bool,
    #[serde(default)]
    is_custom: bool,
    #[serde(default)]
    filters: Vec<WhereField>,
}

#[derive(Debug, Default)]
struct UniqueValidationState {
    existing_values: HashSet<String>,
    seen_values: HashSet<String>,
    case_insensitive: bool,
}

pub struct UniqueValidator {
    fetcher_registry: Arc<DataFetcherJsonRegistry>,
}

impl UniqueValidator {
    pub fn new(fetcher_registry: Arc<DataFetcherJsonRegistry>) -> Self {
        Self { fetcher_registry }
    }

    fn value_tokens(value: &Value) -> Vec<String> {
        match value {
            Value::Null => Vec::new(),
            Value::String(value) if value.trim().is_empty() => Vec::new(),
            Value::String(value) => vec![value.trim().to_string()],
            Value::Array(values) => values.iter().flat_map(Self::value_tokens).collect(),
            Value::Bool(value) => vec![value.to_string()],
            Value::Number(value) => vec![value.to_string()],
            value => vec![value.to_string()],
        }
    }

    fn normalize(value: &str, case_insensitive: bool) -> String {
        let value = value.trim();
        if case_insensitive {
            value.to_lowercase()
        } else {
            value.to_string()
        }
    }

    fn parse_options(options: &Value) -> anyhow::Result<UniqueOptions> {
        let options: UniqueOptions = serde_json::from_value(options.clone())?;

        if options.fetcher.trim().is_empty()
            || options.key.trim().is_empty()
            || options.result_key.trim().is_empty()
        {
            anyhow::bail!("unique validator options fetcher, key, and result_key are required");
        }

        Ok(options)
    }
}

#[async_trait::async_trait]
impl Validator for UniqueValidator {
    async fn prepare_batch(
        &self,
        values: &[Value],
        options: &Value,
    ) -> anyhow::Result<ValidatorState> {
        let options = Self::parse_options(options)?;
        let values = values
            .iter()
            .flat_map(Self::value_tokens)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();

        if values.is_empty() {
            return Ok(Box::new(UniqueValidationState {
                case_insensitive: options.case_insensitive,
                ..Default::default()
            }));
        }

        let mut filters = options.filters;
        let (field, values) = if options.is_custom {
            ("field_key".to_string(), vec![options.key.clone()])
        } else {
            (options.key.clone(), values)
        };
        filters.push(WhereField {
            field,
            operator: if options.case_insensitive {
                "ILIKE"
            } else {
                "="
            }
            .to_string(),
            value: None,
            values: Some(values),
            casting: Some(CastingEnum::Varchar),
        });

        let fetcher = self.fetcher_registry.get(options.fetcher.trim())?;
        let rows = fetcher.fetch(Some(filters)).await?;

        let existing_values = rows
            .iter()
            .filter_map(|row| {
                if options.is_custom {
                    // `key` sudah menjadi filter `field_key`; nilai pembandingnya
                    // selalu berasal dari kolom `value_*` yang sesuai dengan tipe.
                    custom_field_value(row)
                } else {
                    row.get(&options.result_key)
                }
            })
            .flat_map(Self::value_tokens)
            .map(|value| Self::normalize(&value, options.case_insensitive))
            .collect();

        Ok(Box::new(UniqueValidationState {
            existing_values,
            seen_values: HashSet::new(),
            case_insensitive: options.case_insensitive,
        }))
    }

    fn validate(&self, value: &Value, _options: &Value, state: &mut dyn Any) -> anyhow::Result<()> {
        let state = state
            .downcast_mut::<UniqueValidationState>()
            .ok_or_else(|| anyhow::anyhow!("invalid unique validator state"))?;

        for value in Self::value_tokens(value) {
            let value = Self::normalize(&value, state.case_insensitive);

            if state.existing_values.contains(&value) || !state.seen_values.insert(value) {
                anyhow::bail!("must unique");
            }
        }

        Ok(())
    }

    fn validator_name(&self) -> &'static str {
        "UNIQUE"
    }
}
