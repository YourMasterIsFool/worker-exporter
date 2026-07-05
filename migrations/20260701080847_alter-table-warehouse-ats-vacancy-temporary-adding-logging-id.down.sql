-- Add down migration script here
alter table warehouse.ats_vacancy_temporary
    drop column logging_id;
