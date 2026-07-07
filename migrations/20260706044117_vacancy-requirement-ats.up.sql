-- Add up migration script here
CREATE TABLE IF NOT EXISTS warehouse.ats_vacancy_requirement
(
    id uuid NOT NULL,
    updated_at timestamp without time zone DEFAULT now(),
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    deleted_at timestamp without time zone,
    vacancy_id uuid NOT NULL,
    key_field text COLLATE pg_catalog."default" NOT NULL,
    field_type varchar NOT NULL,
    field_purpose varchar NOT NULL,
    tooltip text COLLATE pg_catalog."default",
    multiple_choice_option jsonb,
    allowed_file_type jsonb,
    maximum_file integer,
    maximum_file_size_in_kb integer,
    is_default_question boolean NOT NULL DEFAULT false,
    is_range boolean NOT NULL DEFAULT false,
    compare_operator varchar,
    pivot integer,
    min_pivot integer,
    max_pivot integer,
    criteria jsonb,
    is_mandatory boolean NOT NULL DEFAULT false,
    master_data jsonb,
    data_mapping jsonb,
    logging_id varchar

);

-- Opsional: Tambahkan index untuk optimasi query
