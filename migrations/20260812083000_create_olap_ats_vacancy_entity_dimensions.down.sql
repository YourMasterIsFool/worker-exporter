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
        EXECUTE format('DROP TABLE IF EXISTS olap_ats.%I_dimensions', entity);
    END LOOP;
END $$;
