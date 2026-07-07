-- Add up migration script here
ALTER TABLE warehouse.ats_vacancy_temporary add column filterable_attribute jsonb;
alter table warehouse.ats_vacancy_temporary add column keyword jsonb;
