-- Add down migration script here
ALTER TABLE  warehouse.ats_vacancy_custom_field
RENAME TO vacancy_custom_field;
