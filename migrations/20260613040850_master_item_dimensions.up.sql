-- Add up migration script here
CREATE TABLE IF NOT EXISTS public.master_items
(
    id uuid NOT NULL,
    tenant_id uuid,
    oltp_tenant_id uuid,
    category_id uuid NOT NULL,
    oltp_category_id uuid,
    code character varying(64) COLLATE pg_catalog."default" NOT NULL,
    name character varying(200) COLLATE pg_catalog."default" NOT NULL,
    description character varying(255) COLLATE pg_catalog."default",
    is_active boolean NOT NULL DEFAULT true,
    sort_order integer,
    metadata json,
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    updated_at timestamp without time zone NOT NULL DEFAULT now(),
    parent_id uuid,
    multilang jsonb,
    CONSTRAINT "PK_388206be580e4b20829bc1b08dd" PRIMARY KEY (id)
)
