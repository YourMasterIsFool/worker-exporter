CREATE SCHEMA IF NOT EXISTS olap_ats;

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
            'CREATE TABLE IF NOT EXISTS olap_ats.%I_dimensions (
                id uuid PRIMARY KEY,
                source_key text NOT NULL,
                source_parent_id uuid,
                source_tenant_id uuid,
                source_group_id uuid,
                source_created_at timestamp,
                source_updated_at timestamp NOT NULL,
                source_data jsonb NOT NULL,
                start_from timestamp NOT NULL,
                end_from timestamp,
                is_current boolean NOT NULL DEFAULT true
            )',
            entity
        );
        EXECUTE format(
            'CREATE UNIQUE INDEX IF NOT EXISTS %I ON olap_ats.%I_dimensions (source_key) WHERE is_current',
            entity || '_dimensions_current_source_key', entity
        );
        EXECUTE format(
            'CREATE INDEX IF NOT EXISTS %I ON olap_ats.%I_dimensions (source_updated_at DESC)',
            entity || '_dimensions_source_updated_at', entity
        );
    END LOOP;
END $$;
