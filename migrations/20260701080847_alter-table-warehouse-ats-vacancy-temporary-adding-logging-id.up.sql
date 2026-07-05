-- Add up migration script here
alter table warehouse.ats_vacancy_temporary
    add column logging_id varchar not null;
