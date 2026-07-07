-- Add down migration script here
ALTER TABLE warehouse.ats_vacancy_temporary drop column filterable_attribute jsonb;
alter table warehouse.ats_vacancy_temporary drop column keyword jsonb;
