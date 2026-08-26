ALTER TABLE warehouse.ats_vacancy_temporary
    ADD COLUMN IF NOT EXISTS job_function_ids uuid[];
