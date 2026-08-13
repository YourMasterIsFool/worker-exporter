CREATE SCHEMA IF NOT EXISTS olap_ats;

CREATE TABLE IF NOT EXISTS olap_ats.vacancy_dimensions (
    id uuid PRIMARY KEY,
    source_id uuid NOT NULL,
    source_tenant_id uuid NOT NULL,
    source_group_id uuid NOT NULL,
    name text NOT NULL,
    status varchar NOT NULL,
    completion_status varchar,
    source_created_at timestamp NOT NULL,
    source_updated_at timestamp NOT NULL,
    source_deleted_at timestamp,
    source_data jsonb NOT NULL,
    start_from timestamp NOT NULL,
    end_from timestamp,
    is_current boolean NOT NULL DEFAULT true
);

CREATE UNIQUE INDEX IF NOT EXISTS vacancy_dimensions_current_source_id
    ON olap_ats.vacancy_dimensions (source_id) WHERE is_current;

CREATE INDEX IF NOT EXISTS vacancy_dimensions_source_updated_at
    ON olap_ats.vacancy_dimensions (source_updated_at DESC);
