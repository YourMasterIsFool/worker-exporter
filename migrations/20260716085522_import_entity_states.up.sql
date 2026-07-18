-- Add up migration script here
CREATE TYPE import_validation_status AS ENUM (
    'PENDING',
    'INVALID',
    'VALID'
);

CREATE TYPE import_sync_status AS ENUM (
    'PENDING',
    'SYNCING',
    'SYNCED',
    'FAILED'
);

CREATE TABLE public.import_entity_states (
    id UUID PRIMARY KEY,

    logging_id UUID NOT NULL,

    entity_table VARCHAR(255) NOT NULL,
    entity_id UUID NOT NULL,

    validation_status import_validation_status NOT NULL DEFAULT 'PENDING',
    sync_status import_sync_status NOT NULL DEFAULT 'PENDING',

    errors JSONB NOT NULL DEFAULT '{}'::jsonb,

    sync_error TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,

    validated_at TIMESTAMPTZ,
    synced_at TIMESTAMPTZ,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_import_entity_state
        UNIQUE (logging_id, entity_table, entity_id)
);

CREATE INDEX idx_import_entity_states_logging
ON public.import_entity_states (logging_id);

CREATE INDEX idx_import_entity_states_validation
ON public.import_entity_states (
    logging_id,
    validation_status
);

CREATE INDEX idx_import_entity_states_sync
ON public.import_entity_states (
    logging_id,
    sync_status
);