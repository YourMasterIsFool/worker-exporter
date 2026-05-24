-- Add up migration script here
CREATE TABLE tenant_information_dimensions (
	id uuid NOT NULL default uuidv7(),
	group_id uuid NULL,
	tenant_id uuid NOT NULL,
	tenant_dimension_id uuid not null,
	industry_type_id uuid NULL,
	province_id uuid NULL,
	city_id uuid NULL,
	email varchar(100) NULL,
	phone_number varchar(20) NULL,
	address text NULL,
	zip_code varchar(20) NULL,
	tenant_location point NULL,
	radius_recording int4 NULL,
	status string,
	created_at timestamp DEFAULT now() NOT NULL,
	update_at timestamp DEFAULT now() NOT NULL,
	is_current boolean NOT NULL DEFAULT false,
	valid_from timestamp NULL,
	valid_end timestamp NULL,
	deleted_at timestamp NULL
	CONSTRAINT "FK_tenant_information_tenant_dimension" FOREIGN KEY (tenant_dimension_id) REFERENCES tenant_dimensions(id)
);
CREATE INDEX idx_tenant_information_tenant_dimension ON tenant_information_dimensios (tenant_dimension_id, is_current);
