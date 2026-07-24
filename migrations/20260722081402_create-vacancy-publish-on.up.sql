-- Add up migration script here
CREATE TABLE IF NOT EXISTS warehouse.vacancy_publish_on
(
    id uuid NOT NULL,
    updated_at timestamp without time zone DEFAULT now(),
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    deleted_at timestamp without time zone,
    vacancy_id uuid NOT NULL,
    platform TEXT,
    status TEXT,
    careersite_tenant_id uuid,
    CONSTRAINT vacancy_publish_on_pkey PRIMARY KEY (id)
);
