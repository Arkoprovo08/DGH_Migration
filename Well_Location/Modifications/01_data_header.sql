INSERT INTO operator_contracts_agreements.t_well_location_header (
    well_location_application_number,
    contractor_name,
    block_name,
    date_of_contract_signature,
    block_category,
    reference_to_article_policy,
    is_declared,
    name_of_authorised_signatory,
    designation,
    created_by,
    creation_date,
    is_active,
    remarks,
    process_id,
    current_status,
    is_migrated
)
SELECT
    src.REFID AS well_location_application_number,
    src.CONTRACTNAME AS contractor_name,
    src.BLOCKNAME AS block_name,
    src.DOS_CONTRACT AS date_of_contract_signature,
    src.BLOCKCATEGORY AS block_category,
    src.REF_TOPSC_ARTICALNO AS reference_to_article_policy,
    1 AS is_declared,
    src.NAME_AUTH_SIG_CONTRA AS name_of_authorised_signatory,
    src.DESIGNATION AS designation,
    mum.user_id AS created_by,
    src.CREATED_ON AS creation_date,
    1 AS is_active,
    cm.comment_data AS remarks,
    13 AS process_id,
    'DRAFT' AS current_status,
    1 AS is_migrated
FROM dgh_staging.FORM_WEL_LOC_CHA_DEEP src
LEFT JOIN user_profile.m_user_master mum
    ON mum.migrated_user_id = src.CREATED_BY AND mum.is_migrated = 1
LEFT JOIN LATERAL (
    SELECT comment_data
    FROM dgh_staging.frm_comments c
    WHERE c.comment_id = src.COMMENTID
    LIMIT 1
) cm ON true
join dgh_staging.frm_workitem_master_new w on w.ref_id = src.refid
WHERE src.status = '1';