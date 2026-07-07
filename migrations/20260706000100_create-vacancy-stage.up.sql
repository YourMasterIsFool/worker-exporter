CREATE SCHEMA IF NOT EXISTS vacancy;

CREATE TABLE IF NOT EXISTS warehouse.ats_vacancy_stage
(
    id uuid NOT NULL,
    updated_at timestamp without time zone DEFAULT now(),
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    deleted_at timestamp without time zone,
    vacancy_id uuid NOT NULL,
    stage_name text COLLATE pg_catalog."default" NOT NULL,
    stage_type character varying COLLATE pg_catalog."default" NOT NULL,
    is_default boolean NOT NULL DEFAULT false,
    stage_email_template_id uuid,
    stage_email_template_reject uuid,
    sequence integer NOT NULL,
    is_locked boolean NOT NULL DEFAULT false,
    form_template_id uuid,
    share_access jsonb NOT NULL DEFAULT '[]'::jsonb,
    logging_id varchar,
    CONSTRAINT vacancy_stage_pkey PRIMARY KEY (id)
);
