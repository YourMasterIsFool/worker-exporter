-- Add up migration script here
CREATE TABLE loggings (
    id varchar primary key default uuidv7(),
    logging_type varchar not null,
    completed_at timestamp null,
    download_url varchar null,
    status varchar not null,
    error jsonb null,
    stream_id varchar not null,
    additional_info jsonb null,
    processed_count int not null default 0,
    created_at timestamp not null default now(),
    current_chunk int not null default 0,
    total_chunk int not null default 0,
    cursor varchar null
);
