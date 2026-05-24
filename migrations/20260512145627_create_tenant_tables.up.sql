-- Add up migration script here
CREATE TABLE tenant_snapshots (
    id varchar primary key default uuidv7(),
    tenant_code varchar not null,
    tenant_name varchar not null,

    -- Gunakan timestamptz agar konsisten dengan chrono::DateTime<Utc>
    sync_at timestamptz not null default now(),
    additional_info jsonb null,
    tenant_id varchar not null unique
);
