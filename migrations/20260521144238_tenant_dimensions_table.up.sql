-- Add up migration script here
CREATE TABLE tenant_dimensions (
	id uuid primary key DEFAULT uuidv7(),
	parent_id uuid NULL,
	tenant_id uuid NOT NULL,
	group_id uuid NULL,
	tenant_code varchar(50) NOT NULL,
	tenant_name varchar(255) NOT NULL,
	tenant_level_id uuid NOT NULL,
	tenant_logo varchar NULL,
	status varchar NULL,
	tenant_description text NULL,
	tenant_vision text NULL,
	tenant_mission text NULL,
	created_at timestamp DEFAULT now() NOT NULL,
	update_at timestamp DEFAULT now() NOT NULL,
	careersite_url varchar NULL,
	is_current boolean NOT NULL DEFAULT false,
	valid_from timestamp NULL,
	valid_end timestamp NULL,
	deleted_at timestamp NULL
);
CREATE INDEX idx_tenant_dim_lookup ON tenant_dimensions (tenant_id, is_current);
