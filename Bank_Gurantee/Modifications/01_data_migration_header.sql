INSERT INTO financial_mgmt.t_bank_gurantee_header (
    bank_gurantee_application_number,
    process_id,
    dos_contract,
    block_name,
    block_category,
    ref_topsc_artical_no,
    name_auth_sig_contra,
    designation,
    created_by,
    creation_date,
    remarks,
    contractor_name,
    is_migrated,
    is_active,
    current_status,
    is_declared,
    declaration_checkbox,
    nodal_update_details
)
SELECT DISTINCT ON (fsbg."REFID")
        fsbg."REFID", 
        18,
        fsbg."DOS_CONTRACT", 
        fsbg."BLOCKNAME", 
        fsbg."BLOCKCATEGORY", 
        fsbg."REF_TOPSC_ARTICALNO", 
        fsbg."NAME_AUTH_SIG_CONTRA", 
        fsbg."DESIGNATION", 
        COALESCE(um.user_id, NULL), 
        fsbg."CREATED_ON", 
        cm.comment_data as remarks, 
--        fsbg."CONTRACTNAME" as contractor_name, 
        (
        SELECT string_agg(
            TRIM(REGEXP_REPLACE(split_part(c, '(', 1), '\s+$', '')) || '>>' || 
            COALESCE(
                NULLIF(TRIM(REPLACE(split_part(split_part(c, '(', 2), ')', 1), '%', '')), ''),
                '100'
            ) || 
            '>>20>>', 
            ',')
	        FROM unnest(string_to_array(fsbg."CONSORTIUM", ',')) as c
    	) AS contractor_name,
        2,
        1,
        'DRAFT',
        1,
        '{}',
        '{}'
FROM 
        dgh_staging.form_sub_bg_legal_renewal fsbg
LEFT JOIN 
        user_profile.m_user_master um 
ON 
        fsbg."CREATED_BY" = um.migrated_user_id 
LEFT JOIN 
    	dgh_staging.frm_comments cm
ON 
    	cm.comment_id = fsbg."COMMENTID"
WHERE 
        fsbg."STATUS" = 1
ORDER BY 
        fsbg."REFID", fsbg."CREATED_ON";
