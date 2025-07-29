INSERT INTO operator_contracts_agreements.t_appointment_auditor_details (
    appointment_auditor_application_number,
    creation_date,
    created_by,
    contractor_name,
    block_name,
    dos_contract,
    block_category,
    ref_topsc_articalno,
    name_auditor,
    total_fees_payable_inr,
    designation,
    current_status,
    name_auth_sig_contra,
    is_active,
    audit_period_from,
    audit_period_to,
    is_ocr_available,
    ocr_unavailable_txt,
    awarded_under,
    is_migrated,
    process_id,
    remarks,
    declaration_checkbox
)
SELECT
    src."REFID",
    src."CREATED_ON",
    mum.user_id,
    src."CONTRACTNAME",
    src."BLOCKNAME",
    src."DOS_CONTRACT",
    src."BLOCKCATEGORY",
    src."REF_TOPSC_ARTICALNO",
    src."NAME_AUDITOR",
    CAST(src."TOTAL_FEESPAY" AS numeric),
    src."DESIGNATION",
    'DRAFT',
    src."NAME_AUTH_SIG_CONTRA",
    1,
    src."FROM_YEAR",
    src."TO_YEAR",
    CASE 
        WHEN src."OCR_AVAIABLE" ILIKE 'YES' THEN 1
        ELSE 0
    END,
    src."OCR_UNAVAIABLE_TXT",
    src."BID_ROUND",
    1,
    30,
    cm.comment_data,
    '{}'
FROM dgh_staging.FORM_APPOINTMENT_AUDITOR_OPR src
LEFT JOIN user_profile.m_user_master mum
    ON mum.migrated_user_id = src."CREATED_BY" AND mum.is_migrated = 1
LEFT JOIN LATERAL (
    SELECT c.comment_data
    FROM dgh_staging.FRM_COMMENTS c
    WHERE c.comment_id = src."COMMENTID"
    LIMIT 1
) cm ON true
JOIN dgh_staging.frm_workitem_master_new fwmn ON fwmn.ref_id = src."REFID"
WHERE src."STATUS" = '1';