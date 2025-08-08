INSERT INTO financial_mgmt.t_submission_of_insurance_and_indemnity
(
    insurance_and_indemnity_application_number,
    contractor_name,
    block_name,
    date_of_contract_signature,
    block_category,
    ref_psc_article_no,
    insurance_company_name,
    sum_insured_in_usd,
    sum_insured_in_inr,
    asset_value_covered_in_usd,
    insurance_from,
    insurance_to,
    name_of_authorised_signatory,
    designation,
    created_by,
    creation_date,
    is_active,
    current_status,
    process_id,
    declaration_checkbox,
    is_migrated,
    remarks,
    is_declared
)
SELECT 
    s."REFID", 
    s."CONTRACTNAME", 
    s."BLOCKNAME", 
    s."DOS_CONTRACT", 
    s."BLOCKCATEGORY",
    s."REF_TOPSC_ARTICALNO", 
    s."NAME_OF_INSURANCE_COMPANY", 
    s."SUM_INSURED_USD",
    s."SUM_INSURED", 
    s."ASS_VAL_COVERED", 
    s."TERM_OF_INSURANCE", 
    s."TERM_OF_INSURANCE_TO",
    s."NAME_AUTH_SIG_CONTRA", 
    s."DESIGNATION",
    um.user_id,  -- mapped from migrated_user_id
    s."CREATED_ON",
    1,                      -- is_active
    'DRAFT',                -- current_status
    23,                     -- process_id
    '{"acceptTerm1": true}',-- declaration_checkbox
    1,                      -- is_migrated
    fc.comment_data,
    1
FROM dgh_staging.FORM_SUB_INSURANCE_INDEMNITY s
JOIN dgh_staging.frm_workitem_master_new w 
    ON w.ref_id = s."REFID"
LEFT JOIN LATERAL (
    SELECT comment_data
    FROM dgh_staging.frm_comments
    WHERE comment_id = s."COMMENTID"
    LIMIT 1
) fc ON TRUE
LEFT JOIN user_profile.m_user_master um 
    ON um.is_migrated = 1 
    AND um.migrated_user_id = s."CREATED_BY"
WHERE s."STATUS" = '1' 