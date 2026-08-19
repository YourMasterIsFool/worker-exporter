CREATE TABLE IF NOT EXISTS aggregate_period (
    id varchar PRIMARY KEY DEFAULT uuidv7(),
    scope_key varchar NOT NULL,
    aggregate_code varchar NOT NULL,
    entity_type varchar NOT NULL,
    period_type varchar NOT NULL CHECK (period_type IN ('DAILY', 'MONTHLY', 'YEARLY')),
    period_start date NOT NULL,
    dimensions jsonb NOT NULL DEFAULT '{}'::jsonb,
    metrics jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (
        scope_key,
        aggregate_code,
        entity_type,
        period_type,
        period_start,
        dimensions
    )
);

CREATE INDEX IF NOT EXISTS aggregate_period_lookup_idx
    ON aggregate_period (
        scope_key,
        aggregate_code,
        entity_type,
        period_type,
        period_start
    );

CREATE INDEX IF NOT EXISTS aggregate_period_dimensions_idx
    ON aggregate_period USING gin (dimensions);
