# Validation Registry Architecture

This document describes the validation architecture used by the ETL/import pipeline.

## Overview

The import pipeline processes data in batches, maps each row into a dataset, validates each configured field, writes valid data through a loader, and stores validation or sync state in `import_entity_states`.

```text
Input file
    ↓
Parse rows
    ↓
Process batch
    ↓
Map fields
    ↓
Prepare batch validators
    ↓
Validate each row
    ↓
Build dataset + entity states
    ↓
Transaction
    ├── Load warehouse data
    └── Upsert import entity states
    ↓
Sync to OLTP
```

## Suggested Project Structure

```text
src/
├── application/
│   └── import/
│       ├── dataset/
│       │   ├── mod.rs
│       │   ├── registry.rs
│       │   └── vacancy.rs
│       └── validation/
│           ├── mod.rs
│           ├── registry.rs
│           ├── runner.rs
│           └── validators/
│               ├── required.rs
│               ├── unique.rs
│               └── exists.rs
│
├── core/
│   ├── config/
│   │   └── validation.rs
│   ├── traits/
│   │   ├── validator.rs
│   │   └── data_fetcher.rs
│   └── types/
│       ├── validation.rs
│       └── import_entity_state.rs
│
├── infrastructure/
│   ├── fetcher/
│   ├── loader/
│   ├── source/
│   └── logging/
│
└── bootstrap.rs
```

Responsibilities:

- `application/import/dataset`: field mapping, casting, validation execution, dataset construction.
- `application/import/validation`: validator registry, runner, and validator implementations.
- `core/traits`: reusable contracts without SQLx, PostgreSQL, Redis, or HTTP dependencies.
- `infrastructure/fetcher`: database-backed fetcher implementations.
- `infrastructure/loader`: bulk insert, upsert, and transaction implementation.
- `infrastructure/logging`: persistence for `import_entity_states`.
- `bootstrap.rs`: dependency composition and registry initialization.

## Tenant Configuration

Each import field can declare validation rules.

```json
{
  "column": "warehouse.ats_vacancy_custom_field:noRR",
  "header": "No RR*",
  "validations": [
    {
      "validator": "required",
      "options": {}
    },
    {
      "validator": "unique",
      "options": {
        "fetcher": "VACANCY",
        "field": "no_rr",
        "case_insensitive": true
      }
    }
  ]
}
```

```rust
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Clone, Deserialize)]
pub struct ValidationConfig {
    pub validator: String,

    #[serde(default)]
    pub options: Value,
}
```

Every validator receives dynamic `options` as `serde_json::Value` and converts them into its own typed Rust struct using `serde_json::from_value`.

```rust
#[derive(Debug, Clone, Deserialize)]
pub struct UniqueOptions {
    pub fetcher: String,
    pub field: String,

    #[serde(default)]
    pub case_insensitive: bool,
}

let options: UniqueOptions =
    serde_json::from_value(config.options.clone())?;
```

## Validator Trait

Some validators only inspect one value, while others need batch preparation.

- `required`: row-level validation only.
- `unique`: batch preparation with `WHERE IN`, followed by row-level lookup.
- `exists`: batch preparation with `WHERE IN`, followed by row-level lookup.

```rust
use std::any::Any;

use async_trait::async_trait;
use serde_json::Value;

pub type ValidatorState = Box<dyn Any + Send + Sync>;

#[async_trait]
pub trait Validator: Send + Sync {
    async fn prepare_batch(
        &self,
        values: &[Value],
        options: &Value,
    ) -> anyhow::Result<ValidatorState>;

    fn validate(
        &self,
        value: &Value,
        options: &Value,
        state: &mut dyn Any,
    ) -> anyhow::Result<()>;

    fn name(&self) -> &'static str;
}
```

`ValidatorState` allows each validator to store a different prepared state:

```text
required → ()
unique   → UniqueValidationState
exists   → ExistsValidationState
regex    → compiled Regex
```

The validator object itself is shared and should remain stateless across requests. Mutable batch data belongs inside `ValidatorState`.

## Required Validator

```rust
use std::any::Any;

use anyhow::bail;
use async_trait::async_trait;
use serde_json::Value;

pub struct RequiredValidator;

#[async_trait]
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
        _state: &mut dyn Any,
    ) -> anyhow::Result<()> {
        let invalid = match value {
            Value::Null => true,
            Value::String(value) => value.trim().is_empty(),
            Value::Array(values) => values.is_empty(),
            _ => false,
        };

        if invalid {
            bail!("is required");
        }

        Ok(())
    }

    fn name(&self) -> &'static str {
        "required"
    }
}
```

The validator returns only the reason. The dataset adds the header and field key.

```rust
errors
    .entry(field_key.to_string())
    .or_default()
    .push(format!("{} {}", header, error));
```

Example:

```json
{
  "noRR": [
    "No RR* is required"
  ]
}
```

## Unique Validator

For a batch of 100 rows:

```text
Collect field values
    ↓
Remove duplicates
    ↓
Fetch existing values using WHERE IN / ANY($1)
    ↓
Store existing database values in HashSet
    ↓
Validate each row using HashSet::contains
```

```rust
use std::collections::HashSet;

#[derive(Debug, Default)]
pub struct UniqueValidationState {
    pub existing_values: HashSet<String>,
    pub seen_values: HashSet<String>,
}
```

- `existing_values`: values already present in the database.
- `seen_values`: values already seen inside the current import batch.

```rust
let state = state
    .downcast_mut::<UniqueValidationState>()
    .ok_or_else(|| anyhow::anyhow!("invalid unique validator state"))?;

if state.existing_values.contains(&normalized_value) {
    anyhow::bail!("has already existed");
}

if !state.seen_values.insert(normalized_value) {
    anyhow::bail!("is duplicated within the import batch");
}
```

## Validation Registry

Use owned string keys and `Arc<dyn Validator>`.

```rust
use std::{
    collections::HashMap,
    sync::Arc,
};

pub struct ValidationRegistry {
    validators: HashMap<String, Arc<dyn Validator>>,
}

impl ValidationRegistry {
    pub fn new() -> Self {
        Self {
            validators: HashMap::new(),
        }
    }

    pub fn register<V>(
        &mut self,
        name: impl Into<String>,
        validator: V,
    )
    where
        V: Validator + 'static,
    {
        self.validators.insert(
            name.into().to_uppercase(),
            Arc::new(validator),
        );
    }

    pub fn get(
        &self,
        name: &str,
    ) -> anyhow::Result<Arc<dyn Validator>> {
        self.validators
            .get(&name.to_uppercase())
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Validator '{}' is not registered",
                    name
                )
            })
    }
}

impl Default for ValidationRegistry {
    fn default() -> Self {
        Self::new()
    }
}
```

No explicit lifetime is required. The application stores the registry as:

```rust
Arc<ValidationRegistry>
```

As long as one `Arc` exists, the registry and validators remain alive.

## Registry Initialization

```rust
let mut validation_registry = ValidationRegistry::new();

validation_registry.register(
    "REQUIRED",
    RequiredValidator,
);

validation_registry.register(
    "UNIQUE",
    UniqueValidator::new(
        data_fetcher_registry.clone(),
    ),
);

let validation_registry = Arc::new(validation_registry);
```

Inject the same registry into any dataset that needs it:

```rust
let vacancy_dataset = VacancyDataset::new(
    validation_registry.clone(),
);

let employee_dataset = EmployeeDataset::new(
    validation_registry.clone(),
);
```

## Validation Runner

```rust
pub struct PreparedValidation {
    pub validator: Arc<dyn Validator>,
    pub config: ValidationConfig,
    pub state: ValidatorState,
}
```

Preparation:

```rust
pub async fn prepare_validations(
    registry: &ValidationRegistry,
    values: &[Value],
    configs: &[ValidationConfig],
) -> anyhow::Result<Vec<PreparedValidation>> {
    let mut prepared = Vec::with_capacity(configs.len());

    for config in configs {
        let validator = registry.get(&config.validator)?;

        let state = validator
            .prepare_batch(values, &config.options)
            .await?;

        prepared.push(PreparedValidation {
            validator,
            config: config.clone(),
            state,
        });
    }

    Ok(prepared)
}
```

Validation:

```rust
pub fn validate_value(
    value: &Value,
    prepared: &mut [PreparedValidation],
) -> Vec<anyhow::Error> {
    let mut errors = Vec::new();

    for item in prepared {
        if let Err(error) = item.validator.validate(
            value,
            &item.config.options,
            item.state.as_mut(),
        ) {
            errors.push(error);
        }
    }

    errors
}
```

## Dataset Responsibility

The dataset knows:

- row number;
- field key;
- header;
- entity ID;
- target table;
- field mapping;
- validation configuration;
- how errors are grouped;
- how `ImportEntityState` is created.

The validator does not need `ValidationContext` for the current design.

## Validation Result

```rust
use std::collections::HashMap;

pub type FieldValidationErrors =
    HashMap<String, Vec<String>>;
```

Example:

```json
{
  "noRR": [
    "No RR* has already existed"
  ],
  "positionName": [
    "Position Name* is required"
  ]
}
```

One row can contain multiple errors. Do not write one database record for every field error.

## Import Entity State

One state record represents:

```text
logging_id + entity_table + entity_id
```

```rust
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{FromRow, Type};
use uuid::Uuid;

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Type,
)]
#[sqlx(
    type_name = "import_validation_status",
    rename_all = "SCREAMING_SNAKE_CASE"
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ImportValidationStatus {
    Pending,
    Invalid,
    Valid,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Type,
)]
#[sqlx(
    type_name = "import_sync_status",
    rename_all = "SCREAMING_SNAKE_CASE"
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ImportSyncStatus {
    Pending,
    Syncing,
    Synced,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct ImportEntityState {
    pub id: Uuid,
    pub logging_id: Uuid,
    pub entity_table: String,
    pub entity_id: Uuid,
    pub validation_status: ImportValidationStatus,
    pub sync_status: ImportSyncStatus,
    pub errors: Value,
    pub sync_error: Option<String>,
    pub metadata: Value,
    pub validated_at: Option<DateTime<Utc>>,
    pub synced_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
```

## Persistence

Use bulk upsert with this conflict key:

```sql
UNIQUE (logging_id, entity_table, entity_id)
```

```sql
INSERT INTO public.import_entity_states (...)
VALUES (...)
ON CONFLICT (
    logging_id,
    entity_table,
    entity_id
)
DO UPDATE SET
    validation_status = EXCLUDED.validation_status,
    sync_status = EXCLUDED.sync_status,
    errors = EXCLUDED.errors,
    sync_error = EXCLUDED.sync_error,
    metadata = EXCLUDED.metadata,
    validated_at = EXCLUDED.validated_at,
    synced_at = EXCLUDED.synced_at,
    updated_at = NOW();
```

## Transaction Boundary

Warehouse data and import entity states should be persisted in the same transaction when they must remain consistent.

```rust
let mut tx = warehouse_client.pool.begin().await?;

warehouse_loader
    .bulk_insert_with_tx(
        &mut tx,
        &warehouse_rows,
    )
    .await?;

warehouse_logging
    .bulk_upsert_with_tx(
        &mut tx,
        &entity_states,
    )
    .await?;

tx.commit().await?;
```

## Final Processing Flow

```text
Read input file
    ↓
Split into batches
    ↓
Map and cast fields
    ↓
Prepare validators
    ├── required → no-op state
    ├── unique   → WHERE IN + HashSet
    └── exists   → WHERE IN + HashSet
    ↓
Validate every row
    ↓
Collect errors by field key
    ↓
Build warehouse dataset
    ↓
Build import entity states
    ↓
Open transaction
    ├── bulk insert/upsert warehouse rows
    └── bulk upsert import entity states
    ↓
Commit
    ↓
Sync valid entities
    ↓
Upsert SYNCED or FAILED state
```

## Design Summary

```text
Validator
- Knows how to validate a value.
- Converts dynamic options into typed Rust structs.
- Can prepare batch state.
- Returns a concise validation reason.

Dataset
- Knows row, field, header, entity, and mapping.
- Executes configured validators.
- Groups errors.
- Creates VALID or INVALID entity states.

Registry
- Owns reusable validators.
- Uses String keys.
- Returns Arc<dyn Validator>.
- Lives as long as the application state.

Fetcher
- Provides existing values from external or database sources.
- Implemented in infrastructure.

Loader
- Persists warehouse datasets and entity states.
- Owns SQLx, transactions, bulk insert, and upsert.
```
