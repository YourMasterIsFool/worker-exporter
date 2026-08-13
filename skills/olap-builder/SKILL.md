---
name: olap-builder
description: Build or change OLAP, IMPORT, or EXPORT flows in this Rust worker from the supplied repository configuration, schema, and mapping. Use when asked to add OLAP dimensions, facts, snapshots, warehouse upserts, import datasets/loaders, export datasets/writers, or their wiring; enforce the configured scope and avoid unrelated flow changes.
---

# OLAP Builder

Implement the smallest OLAP, IMPORT, or EXPORT change justified by the configuration already supplied by the user and repository. Treat migrations and configured registry keys as the source of truth.

## Read first

1. Read the requested configuration, affected migration, and target table schema.
2. Trace the existing equivalent end to end: source, loader, module exports, registry, caller, and tests.
3. Inspect these integration points only when relevant:
   - `src/core/traits/load.rs` for loader contracts;
   - `src/application/loader_registry.rs` for `IncrementalLoadPersitence` registration;
   - `src/application/ingestion/source_registry.rs` for source registration;
   - `src/application/pipelines/pipeline_registry.rs` for ingestion pipelines; and
   - `src/application/orchestrator/ingestion_orchestrator.rs` for runtime activation.
4. For IMPORT, also inspect `src/application/dataset.rs`, `src/application/import/import_registry.rs`, `src/application/pipelines/pipeline_import_registry.rs`, `src/infrastructure/datasets/import/`, and `ImportOrchestrator`.
5. For EXPORT, also inspect `src/application/dataset.rs`, `src/application/writter.rs`, `src/application/runner/export_runner.rs`, `src/infrastructure/datasets/export/`, and `ExportOrchestrator`.

Do not infer a table name, key, source, conflict columns, mapping, or load mode that is absent from the supplied configuration. Report the missing item rather than inventing it.

## Implement

1. Reuse an existing source, schema, helper, and loader pattern whenever it matches.
2. Place a new OLAP loader in `src/infrastructure/loader/olap/`; create one domain submodule only when it is needed by more than one file.
3. Implement the existing persistence trait, use parameterized SQL, and make rerunning the same batch idempotent with the configured natural/conflict key.
4. Register only the configured key in the existing registry. Keep source key, loader key, and pipeline key exact.
5. Add only the wiring needed by the requested flow. Add migrations only when the configuration requires a missing or changed schema.
6. Leave one focused test that fails on broken mapping or non-idempotent loading. Run the narrowest applicable Cargo check/test and `git diff --check`.

## Runtime boundary

`stream:ingestion` is not currently complete end to end: its pipeline and orchestrator are not registered. Do not publish or document production use of that stream as working. Wire and activate it only when the user explicitly asks to build the ingestion runtime, then complete source, loader, pipeline, orchestrator, registry, handler-key, and tests together.

## IMPORT and EXPORT

For IMPORT, treat `import_config.dataset`, `import_config.loader`, and `import_config.pipeline` as exact registry keys. Add only the requested dataset mapper, temporary loader, fetcher, or import pipeline. Preserve `logging_id`, chunking, validation, and `auto_sync` behavior unless configuration explicitly changes them.

For EXPORT, treat `export_config.dataset`, `export_config.format`, and `export_config.fields` as the contract. Add only the requested dataset or writer, preserve the tenant filter and cursor/batch behavior in `ExportRunner`, and do not change facilitator upload or email-stream behavior unless requested.

## Scope guardrails

Do not change any of the following unless the supplied configuration and request explicitly require it:

- OLTP schemas or source data;
- IMPORT, EXPORT, or SYNC behavior outside the configured flow;
- Redis retry/ACK behavior, handlers, public payloads, or generic registry design;
- unrelated migrations, refactors, dependencies, or formatting-only edits.

Do not use unparameterized SQL, destructive schema operations, or a loader key not present in the configuration. Stop and ask for the specific missing configuration instead of broadening scope.

## Handoff

State the files changed, configured keys registered, idempotency rule, and verification performed. State any unimplemented runtime wiring plainly; do not represent a scaffold as an active OLAP flow.
