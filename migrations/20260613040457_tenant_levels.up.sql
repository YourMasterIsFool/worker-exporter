-- Add up migration script here


CREATE TABLE if not exists tenant_level_dimensions (
    id uuid PRIMARY KEY,
    name character varying(255) NOT NULL,
    oltp_id uuid NOT NULL,
    level integer NOT NULL,
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    updated_at timestamp without time zone NOT NULL DEFAULT now()
);
