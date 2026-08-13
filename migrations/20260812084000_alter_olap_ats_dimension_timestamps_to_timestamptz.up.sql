ALTER TABLE olap_ats.vacancy_dimensions
    ALTER COLUMN source_created_at TYPE timestamptz USING source_created_at AT TIME ZONE 'UTC',
    ALTER COLUMN source_updated_at TYPE timestamptz USING source_updated_at AT TIME ZONE 'UTC',
    ALTER COLUMN source_deleted_at TYPE timestamptz USING source_deleted_at AT TIME ZONE 'UTC',
    ALTER COLUMN start_from TYPE timestamptz USING start_from AT TIME ZONE 'UTC',
    ALTER COLUMN end_from TYPE timestamptz USING end_from AT TIME ZONE 'UTC';

DO $$
DECLARE
    entity text;
BEGIN
    FOREACH entity IN ARRAY ARRAY[
        'vacancy_custom_field', 'vacancy_filter', 'vacancy_form_snapshot',
        'vacancy_form_option_snapshot', 'vacancy_invitation', 'vacancy_pic',
        'vacancy_publish_on', 'vacancy_requirement', 'vacancy_share_access', 'vacancy_stage'
    ]
    LOOP
        EXECUTE format(
            'ALTER TABLE olap_ats.%I_dimensions
                ALTER COLUMN source_created_at TYPE timestamptz USING source_created_at AT TIME ZONE ''UTC'',
                ALTER COLUMN source_updated_at TYPE timestamptz USING source_updated_at AT TIME ZONE ''UTC'',
                ALTER COLUMN start_from TYPE timestamptz USING start_from AT TIME ZONE ''UTC'',
                ALTER COLUMN end_from TYPE timestamptz USING end_from AT TIME ZONE ''UTC''',
            entity
        );
    END LOOP;
END $$;
