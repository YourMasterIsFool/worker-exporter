CREATE SCHEMA IF NOT EXISTS olap;

CREATE TABLE IF NOT EXISTS olap.master_item_dimensions (
    id uuid PRIMARY KEY,
    source_id uuid NOT NULL,
    source_tenant_id uuid,
    source_category_id uuid NOT NULL,
    source_parent_id uuid,
    code varchar(64) NOT NULL,
    name varchar(200) NOT NULL,
    description varchar(255),
    is_active boolean NOT NULL,
    sort_order integer,
    metadata jsonb,
    multilang jsonb,
    source_created_at timestamp NOT NULL,
    source_updated_at timestamp NOT NULL,
    start_from timestamp NOT NULL,
    end_from timestamp,
    is_current boolean NOT NULL DEFAULT true
);

CREATE UNIQUE INDEX IF NOT EXISTS master_item_dimensions_current_source_id
    ON olap.master_item_dimensions (source_id) WHERE is_current;

CREATE INDEX IF NOT EXISTS master_item_dimensions_category_current
    ON olap.master_item_dimensions (source_category_id, is_current);

CREATE TABLE IF NOT EXISTS olap.geo_dimensions (
    id uuid PRIMARY KEY,
    source_id uuid NOT NULL,
    source_parent_id uuid,
    source_level varchar(32) NOT NULL,
    code varchar(64) NOT NULL,
    name varchar(200) NOT NULL,
    description varchar(255),
    is_active boolean NOT NULL,
    sort_order integer,
    metadata jsonb,
    source_created_at timestamp NOT NULL,
    source_updated_at timestamp NOT NULL,
    start_from timestamp NOT NULL,
    end_from timestamp,
    is_current boolean NOT NULL DEFAULT true
);

CREATE UNIQUE INDEX IF NOT EXISTS geo_dimensions_current_source_id
    ON olap.geo_dimensions (source_id) WHERE is_current;

CREATE INDEX IF NOT EXISTS geo_dimensions_parent_current
    ON olap.geo_dimensions (source_parent_id, is_current);
