INSERT INTO upstream_data_management.t_caliberation_flow_meters_equiptment_details (
    caliberation_flow_meters_witness_custody_details_id,
    meter_type_name_list,
    meter_id,
    last_date_of_caliberation,
    is_active,
    created_by,
    creation_date,
    is_migrated
)
SELECT DISTINCT ON (fm."REFID", fm."SEQ_NO")
    wd.caliberation_flow_meters_witness_custody_details_id,
    CASE 
        WHEN fm."LABEL_VALUE" = 'Orifice_Meter' THEN 'Orifice'
        WHEN fm."LABEL_VALUE" = 'Manual_Tank_Gauging' THEN 'Manual Tank Gauging'
        ELSE 'Other'
    END AS meter_type_name_list,
    CASE 
        WHEN fm."LABEL_VALUE" = 'Orifice_Meter' THEN 1
        WHEN fm."LABEL_VALUE" = 'Manual_Tank_Gauging' THEN 6
        ELSE 9
    END AS meter_id,
    TO_DATE(NULLIF(lcd."LABEL_VALUE", ''), 'DD/MM/YYYY') AS last_date_of_caliberation,
    1 AS is_active,
    mum.user_id,
    TO_TIMESTAMP(NULLIF(fm."CREATED_ON", ''), 'DD/MM/YYYY') AS creation_date,
    1 AS is_migrated
FROM dgh_staging.form_calibration_flow_sec fm
LEFT JOIN dgh_staging.form_calibration_flow_sec lcd 
    ON lcd."REFID" = fm."REFID"
    AND lcd."SEQ_NO" = fm."SEQ_NO" + 2
    AND lcd."LABEL_TEXT" = 'Last_date_of_calibration'
LEFT JOIN upstream_data_management.t_caliberation_flow_meters_witness_custody_details wd 
    ON wd.caliberation_flow_meters_witness_custody_details_application_nu = fm."REFID"
LEFT JOIN user_profile.m_user_master mum 
    ON mum.migrated_user_id = fm."CREATED_BY" AND mum.is_migrated = 1
WHERE 
    fm."LABEL_TEXT" = 'Please_select_type _of_meter'
    AND fm."STATUS" = 1
ORDER BY fm."REFID", fm."SEQ_NO", lcd."SEQ_NO";
