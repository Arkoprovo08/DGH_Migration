insert into financial_mgmt.t_eoy_statement_and_audited_accounts_details 
(
	eoy_statement_and_audited_accounts_details_application_number,
	block_name,
	block_category,
	awarded_under,
	is_ocr_available,
	ocr_unaivalibility_text, 
	declaration_checkbox,
	name_of_authorised_signatory,
	designation,
	is_declared,
	process_id,
	remarks,
	type_of_data_submitted,
	created_by,
	current_status,
	is_migrated
)
SELECT 
    f."REF_ID",
    f."BLOCKNAME",
    f."BLOCKCATEGORY",
    f."BID_ROUND",
    CASE 
        WHEN f."OCR_AVAIABLE" = 'YES' THEN 1 
        ELSE 0 
    END AS is_ocr_available,
    f."OCR_UNAVAIABLE_TXT", 
    '{"acceptTerm1": true}'::JSONB AS declaration_checkbox,
    f."NAME_AUTH_SIG_CONTRA",
    f."DESIGNATION",
    1 AS is_declared,
    8 AS process_id,
    fc.comment_data,
    CASE 
        WHEN f."TYPE_DATA" = 'EDP' THEN 0 
        WHEN f."TYPE_DATA" = 'ES' THEN 1 
        ELSE NULL
    END AS type_of_data_submitted,
    m.user_id as created_by,
    'DRAFT',
    1 as is_migrated
FROM dgh_staging.form_audited_accounts f
JOIN (  SELECT DISTINCT ON (COMMENT_ID) *
	    FROM dgh_staging.frm_comments
	    WHERE is_active = '1'
	    ORDER BY COMMENT_ID
	 ) fc ON fc.COMMENT_ID = f."COMMENT_ID"
JOIN dgh_staging.frm_workitem_master_new c ON f."REF_ID" = c.ref_id
JOIN user_profile.m_user_master m on m.migrated_user_id = f."CREATED_BY"
WHERE  f."STATUS" = '1' AND f."IS_ACTIVE" = '1' AND f."REF_ID" = 'AUA-20210222070200';
