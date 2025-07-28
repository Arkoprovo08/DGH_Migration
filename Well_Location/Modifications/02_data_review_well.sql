INSERT INTO operator_contracts_agreements.t_well_location_well_details (
    well_location_header_id,
    well_type,
    task_type,
    well_name,
    well_location_surface_latitude,
    well_location_surface_longitude,
    well_location_sub_surface_latitude,
    well_location_sub_surface_longitude,
    well_course,
    target_well_depth_offshore,
    intended_well_depth_offshore,
    expected_geological_objective,
    is_ocr_available,
    ocr_unavailable_txt,
    is_active,
    is_migrated
)
SELECT 
    hdr.well_location_header_id,
    CASE 
        WHEN data."WELL_TYPE" = '1' THEN 'Exploratory'
        WHEN data."WELL_TYPE" = '2' THEN 'Appraisal'
        WHEN data."WELL_TYPE" = '3' THEN 'Developtment'
        ELSE NULL
    END AS well_type,
    'Review' AS task_type,
    data."WELL_TYPE_NAME" as well_name,
    data."REW_LOC_SURFACE_LATI" as well_location_surface_latitude,
    data."REW_LOC_SURFACE" as well_location_surface_longitude,
    data."REW_LOC_SUB_SURFACE_LATI" as well_location_sub_surface_latitude,
    data."REW_LOC_SUB_SURFACE" as well_location_sub_surface_longitude,
    CASE 
        WHEN data."REW_WELL_COURSE_TYPE" = '1' THEN 'Vertical'
        ELSE 'Inclined'
    END AS well_course,
    CAST(data."REW_TAG_WELL_DEPTH" AS numeric(30,2)) as target_well_depth_offshore,
    CAST(data."REW_INT_DEPTH" AS NUMERIC(30,2)) as intended_well_depth_offshore,
    data."REW_EXP_GEO_OBJ" as expected_geological_objective,
    CASE 
        WHEN data."OCR_AVAIL" = '1' THEN 1
        ELSE 0
    END AS ocr_available,
    data."OCR_UNAVAIL_TEXT",
    1 as is_active,
    1 as is_migrated
FROM dgh_staging.form_wel_loc_cha_de_data data
JOIN operator_contracts_agreements.t_well_location_header hdr 
    ON hdr.well_location_application_number = data."WEL_LOC_CHA_DEEP_ID"
WHERE data."REW_WELL_COURSE_TYPE" = '1' OR data."REW_WELL_COURSE_TYPE" = '2';