-- Add up migration script here


CREATE TABLE IF NOT EXISTS master_categories_dimensions
(
    id uuid NOT NULL,
    tenant_id uuid,
    oltp_tenant_id uuid,
    code character varying(64) COLLATE pg_catalog."default" NOT NULL,
    name character varying(200) COLLATE pg_catalog."default" NOT NULL,
    description character varying(255) COLLATE pg_catalog."default",
    is_active boolean NOT NULL DEFAULT true,
    sort_order integer,
    metadata json,
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    updated_at timestamp without time zone NOT NULL DEFAULT now(),
    CONSTRAINT "PK_1febc7cec6abad976b21995a32e" PRIMARY KEY (id)
)
