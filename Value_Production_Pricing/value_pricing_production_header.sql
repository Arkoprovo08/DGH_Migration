INSERT INTO financial_mgmt.t_value_of_production_and_pricing_statement_details_header (
    value_production_pricing_statement_details_application_number,
    process_id,
    is_declared,
    name_of_authorised_signatory,
    designation,
    created_by,
    creation_date,
    is_active,
    current_status,
    block_name,
    block_category,
    dos_contract,
    fy,
    month,
    ref_to_psc_article_no,
    is_migrated
)
SELECT 
    src."REFID",
    35, -- Static process ID
    1,  -- is_declared = true
    src."NAME_AUTH_SIG_CONTRA",
    src."DESIGNATION",
    mum.user_id AS created_by,
    src."CREATED_ON",
    1,  -- is_active = true
    'DRAFT', -- current_status
    src."BLOCKNAME",
    src."BLOCKCATEGORY",
    src."DOS_CONTRACT",
    src."FY",
    CASE src."MONTH"
        WHEN '1' THEN 'January'
        WHEN '2' THEN 'February'
        WHEN '3' THEN 'March'
        WHEN '4' THEN 'April'
        WHEN '5' THEN 'May'
        WHEN '6' THEN 'June'
        WHEN '7' THEN 'July'
        WHEN '8' THEN 'August'
        WHEN '9' THEN 'September'
        WHEN '10' THEN 'October'
        WHEN '11' THEN 'November'
        WHEN '12' THEN 'December'
        ELSE NULL
    END AS month,
    src."REF_TOPSC_ARTICALNO",
    1 AS is_migrated
FROM dgh_staging.form_production_val_statement src
LEFT JOIN user_profile.m_user_master mum
    ON mum.migrated_user_id = src."CREATED_BY" AND mum.is_migrated = 1
WHERE src."STATUS" = '1';
