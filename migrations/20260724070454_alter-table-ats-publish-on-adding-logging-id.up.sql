-- Add up migration script here
ALTER TABLE warehouse.ats_vacancy_publish_on
ADD COLUMN logging_id varchar;

ALTER TABLE warehouse.ats_vacancy_publish_on
ADD COLUMN tenant_id varchar;

CREATE INDEX idx_vacancy_publish_on_tenant_logging_id
ON warehouse.ats_vacancy_publish_on (tenant_id, logging_id);