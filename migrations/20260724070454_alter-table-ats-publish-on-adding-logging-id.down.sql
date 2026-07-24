-- Add down migration script here
DROP INDEX idx_vacancy_publish_on_vacancy_id_logging_id;
alter table "warehouse"."ats_vacancy_publish_on" drop column logging_id;
alter table "warehouse"."ats_vacancy_publish_on" drop column tenant_id;

