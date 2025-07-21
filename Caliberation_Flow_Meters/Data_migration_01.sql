INSERT INTO upstream_data_management.t_caliberation_flow_meters_witness_custody_details (
    caliberation_flow_meters_witness_custody_details_application_nu,
    block_category,
    block_name,
    contractor_name,
    created_by,
    creation_date,
    current_status,
    date_contract_signature,
    designation,
    is_active,
    declaration_checkbox ,
    name_of_authorised_signatory,
    operator_location,
    ref_to_psc_article_no,
    remarks,
    testing_from,
    testing_to,
    is_migrated 
)
SELECT
    fcf."REFID",
    fcf."BLOCKCATEGORY",
    fcf."BLOCKNAME",
    fcf."CONTRACTNAME",
    mum.user_id,
    fcf."CREATED_ON",
    'DRAFT' AS current_status,
    fcf."DOS_CONTRACT",
    fcf."DESIGNATION",
    1 AS is_active,
    '{}' AS dummy_json,
    fcf."NAME_AUTH_SIG_CONTRA",
    fcf."LOCATION",
    fcf."REF_TOPSC_ARTICALNO",
    fc.comment_data,
    fcf."SCH_DATE_TEST_FROM",
    fcf."SCH_DATE_TEST_TO",
    1
FROM dgh_staging.form_calibratio_flow fcf
LEFT JOIN user_profile.m_user_master mum 
    ON mum.migrated_user_id = fcf."CREATED_BY" AND mum.is_migrated = 1
LEFT JOIN dgh_staging.frm_comments fc 
    ON fc.comment_id = fcf."COMMENTID"
where fcf."STATUS" = '1';
