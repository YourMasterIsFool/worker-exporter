DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'warehouse'
          AND table_name = 'ats_vacancy_temporary'
          AND column_name = 'is_locked'
    ) THEN
        ALTER TABLE warehouse.ats_vacancy_temporary
            ALTER COLUMN is_locked SET DEFAULT false;
    END IF;
END $$;
