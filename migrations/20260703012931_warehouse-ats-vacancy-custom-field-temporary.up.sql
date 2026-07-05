CREATE TABLE IF NOT EXISTS warehouse.vacancy_custom_field
(
    id uuid NOT NULL PRIMARY KEY,
    updated_at timestamp without time zone DEFAULT now(),
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    deleted_at timestamp without time zone,
    tenant_id uuid NOT NULL,
    group_id uuid NOT NULL,
    parent_id uuid,
    field_key text COLLATE pg_catalog."default" NOT NULL,
    field_type text,
    value_text text COLLATE pg_catalog."default",
    value_number numeric,
    value_id uuid,
    meta jsonb,
    reference_module text COLLATE pg_catalog."default",
    value_bool boolean,
    value_date date,
    loggin_id uuid
);
