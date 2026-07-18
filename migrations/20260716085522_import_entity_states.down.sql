-- Add down migration script here
DROP TABLE IF EXISTS public.import_entity_states CASCADE;

DROP TYPE IF EXISTS import_sync_status CASCADE;
DROP TYPE IF EXISTS import_validation_status CASCADE;