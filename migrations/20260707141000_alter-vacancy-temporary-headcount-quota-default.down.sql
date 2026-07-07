DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'warehouse'
          AND table_name = 'ats_vacancy_temporary'
          AND column_name = 'headcount_quota'
    ) THEN
        ALTER TABLE warehouse.ats_vacancy_temporary
            ALTER COLUMN headcount_quota DROP DEFAULT;
    END IF;
END $$;
