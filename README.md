# Worker Exporter

`worker-exporter` is an asynchronous Rust worker for database ingestion and large-file exports. It consumes jobs from Redis Streams, reads data in cursor-based batches, produces CSV or XLSX files, uploads the result through the configured file service, and publishes a completion notification.

The project is designed around replaceable sources, datasets, writers, and orchestration layers so that new export workflows can be added without changing the worker runtime.

## Features

- Redis Streams and consumer groups for asynchronous jobs.
- Cursor-based batch processing for large datasets.
- CSV output with buffered writes and XLSX output with constant-memory mode.
- Configuration-driven dynamic exports across approved PostgreSQL databases.
- Relation loading, filtering, value transforms, and optional cached lookups.
- Export progress and output metadata persisted through the logging repository.
- File upload and email notification integrations through gRPC and Redis.
- Optional HTTP server with a health endpoint and operational endpoints.

## Architecture

```mermaid
flowchart LR
    Producer[Job Producer] --> Redis[(Redis Streams)]
    Redis --> Worker[Worker Exporter]
    Worker --> Dataset[Dataset / Dynamic Query]
    Dataset --> Database[(PostgreSQL)]
    Worker --> Writer[CSV or XLSX Writer]
    Writer --> File[Export File]
    File --> Storage[File Service]
    Worker --> Email[Email Stream]
```

An export follows this sequence:

1. A producer adds a JSON job to `stream:export`.
2. A worker resolves the export orchestrator and dataset.
3. The dataset fetches rows in cursor-based batches.
4. The selected writer appends each batch to a CSV or XLSX file.
5. The completed file is uploaded and its download URL is stored with the job log.
6. A notification is published to `stream:email`.

## Requirements

- Rust stable toolchain
- PostgreSQL instances for the configured data sources
- Redis 6.2+ with Streams support
- Reachable Facilitator and ATS gRPC services

## Configuration

Create a `.env` file for local development. Do not commit credentials or service URLs to source control.

```dotenv
HOST=0.0.0.0
PORT=8080

REDIS_URL=redis://127.0.0.1:6379
WAREHOUSE_DATABASE_URL=postgres://user:password@localhost/warehouse
ORGANIZATION_DATABASE_URL=postgres://user:password@localhost/organization
MASTER_DATABASE_URL=postgres://user:password@localhost/master
ATS_DATABASE_URL=postgres://user:password@localhost/ats
HRIS_DATABASE_URL=postgres://user:password@localhost/hris

FACILITATOR_GRPC_URL=http://127.0.0.1:50051
ATS_GRPC_URL=http://127.0.0.1:50052
```

Set `RUST_LOG` to control logging, for example `RUST_LOG=info` or `RUST_LOG=worker_exporter=debug,sqlx=warn`.

## Run locally

```bash
cargo check
cargo test
cargo run --bin worker
```

The worker creates the required Redis consumer groups for these streams:

| Stream | Purpose |
| --- | --- |
| `stream:export` | Export jobs |
| `stream:ingestion` | Incremental ingestion jobs |
| `stream:import` | Import jobs |
| `stream:sync` | Synchronization jobs |
| `stream:rollback` | Rollback jobs |
| `stream:aggregate` | Aggregate jobs |

To run the combined HTTP and worker process:

```bash
cargo run --bin http_worker
```

## Dynamic export job

Jobs are stored in Redis as a `payload` field containing JSON. The following example exports a configured PostgreSQL source to CSV:

```json
{
  "tenant_id": "TENANT_UUID",
  "title": "Candidate export",
  "email": "user@example.com",
  "actor_id": "ACTOR_UUID",
  "orchestrator_name": "EXPORT",
  "filters": [
    {
      "field": "created_at",
      "operator": ">=",
      "value": "2026-01-01 00:00:00"
    }
  ],
  "dynamic_export_config": {
    "dataset": "DYNAMIC_EXPORT_DATASET",
    "format": "csv",
    "chunk": 1000,
    "template_code": "EXPORT_READY",
    "structure_template_code": "DEFAULT",
    "redirected_url": "https://example.com/download/{{url}}",
    "source": {
      "database": "ats",
      "schema": "process",
      "table": "candidates",
      "cursor": { "column": "id", "direction": "asc" },
      "tenant_field": "tenant_id",
      "tenant_operator": "=",
      "where_fields": [
        { "field": "deleted_at", "operator": "IS NULL" }
      ]
    },
    "fields": [
      { "name": "Candidate ID", "column": "id" },
      { "name": "Email", "column": "email" },
      { "name": "Created At", "column": "created_at" }
    ],
    "relations": []
  }
}
```

Publish it to Redis with a client of your choice. The stream entry must use `payload` as its field name.

```text
XADD stream:export * payload '<JSON job above>'
```

### Dynamic export configuration

| Field | Description |
| --- | --- |
| `dataset` | Dataset registry key. Use `DYNAMIC_EXPORT_DATASET` for config-driven exports. |
| `format` | Output format: `csv`, `excel`, or `excel_rs`. |
| `chunk` | Maximum source rows fetched per batch. |
| `source` | Approved database, schema, table, cursor, and base filters. |
| `fields` | Output columns, source paths, transforms, and defaults. |
| `relations` | Optional related data loaded from configured sources. |
| `filters` | Runtime filters applied in addition to the source configuration. |

Identifiers are validated and databases are restricted to the configured `organization`, `ats`, `master`, `warehouse`, and `hris` pools.

## Extending the worker

### Add an export writer

1. Implement `WritterTrait` in `src/infrastructure/exporter`.
2. Register a factory in `build_writter_registry`.
3. Use the registry key as `format` in the export job.

### Add a dataset

1. Implement `Dataset` in `src/application/datasets/export`.
2. Register it in `build_dataset_registry`.
3. Reference its key in `export_config.dataset`.

### Add an ingestion source

1. Implement the relevant extraction contract in `src/infrastructure/ingestion`.
2. Register the source in `build_source_registry`.
3. Submit a job with the matching source and pipeline keys.

## Project layout

```text
src/
├── application/     # Orchestration, datasets, handlers, runners, and registries
├── core/            # Shared contracts, configuration, and types
├── domain/          # Domain entities and repository contracts
├── infrastructure/  # Redis, database, file writer, loader, and source adapters
├── bin/             # Worker and HTTP entry points
└── utils/           # Shared helpers
```

## Development

```bash
cargo fmt --check
cargo check
cargo test
```

For memory profiling in a development environment, enable the optional `dhat-heap` feature:

```bash
cargo run --features dhat-heap --bin worker
```

## Documentation

Additional configuration examples and architecture notes are available in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## License

Add a license file before publishing or distributing the project.
