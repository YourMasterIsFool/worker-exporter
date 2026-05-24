-- Add up migration script here
CREATE TABLE tenant_taxes_dimensions (
	id uuid NOT NULL,
	tenant_id uuid NOT NULL,
	group_id uuid NULL,
	tenant_dimension_id uuid not null,
	matching_npwp_parent bool DEFAULT false NOT NULL,
	personal_npwp varchar(255) NULL,
	personal_name varchar(255) NULL,
	personal_position varchar(255) NULL,
	tenant_npwp varchar(255) NULL,
	created_at timestamp DEFAULT now() NOT NULL,
	update_at timestamp DEFAULT now() NOT NULL,
	deleted_at timestamp NULL,
	personal_position_id uuid NULL,
	valid_from timestamp NULL,
	valid_end timestamp NULL,
	deleted_at timestamp NULL
	CONSTRAINT "FK_tenant_information_tenant_dimension" FOREIGN KEY (tenant_dimension_id) REFERENCES tenant_dimensions(id)

);


CREATE INDEX idx_tenant_tax_tenant_dimension ON tenant_taxes_dimensions (tenant_dimension_id, is_current);
