-- Add up migration script here
CREATE TABLE IF NOT EXISTS warehouse.ats_vacancy_pic
(
    vacancy_id uuid NOT NULL,
    employee_id uuid NOT NULL,
    employee_meta jsonb,
    is_verified boolean NOT NULL DEFAULT false,
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    logging_id varchar,
    id uuid NOT NULL PRIMARY KEY
);
