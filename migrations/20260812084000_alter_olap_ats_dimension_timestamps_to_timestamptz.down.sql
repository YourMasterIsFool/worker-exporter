ALTER TABLE olap_ats.vacancy_dimensions
    ALTER COLUMN source_created_at TYPE timestamp USING source_created_at AT TIME ZONE 'UTC',
    ALTER COLUMN source_updated_at TYPE timestamp USING source_updated_at AT TIME ZONE 'UTC',
    ALTER COLUMN source_deleted_at TYPE timestamp USING source_deleted_at AT TIME ZONE 'UTC',
    ALTER COLUMN start_from TYPE timestamp USING start_from AT TIME ZONE 'UTC',
    ALTER COLUMN end_from TYPE timestamp USING end_from AT TIME ZONE 'UTC';
