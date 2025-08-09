INSERT INTO site_restoration.t_site_restoration_abandonment_psc_relinquishment_details (
    site_restoration_abandonment_details_id,
    field_name,
    date_of_relinquishment,
    is_active,
    created_by,
    creation_date,
    is_migrated
)
SELECT 
    tgt.site_restoration_abandonment_details_id,
    fsf."FIELD_NAME",
    fsf."DATE_RELINQUISHMENT",
    1,
    tgt.created_by,
    CURRENT_DATE,
    1
FROM dgh_staging.form_site_restoration_field fsf
JOIN site_restoration.t_site_restoration_abandonment_details tgt 
    ON tgt.site_restoration_abandonment_application_number = fsf."REFID"
WHERE fsf."STATUS" = 1 and fsf."IS_ACTIVE" = '1';