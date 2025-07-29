WITH pivoted_data AS (
  SELECT
    refid,
    MAX(CASE 
	    WHEN data_id = 'txt_Ex_Phase_I' THEN 
	        CASE 
	            WHEN data_value = 'NA' THEN 'NA'
	            WHEN data_value = 'Na' THEN 'NA'
	            ELSE NULL
	        END
		END) AS txt_ex_phase_i,
	MAX(CASE 
	    WHEN data_id = 'txt_Ex_Phase_II' THEN 
	        CASE 
	            WHEN data_value = 'NA' THEN 'NA'
	            WHEN data_value = 'Na' THEN 'NA'
	            ELSE NULL
	        END
		END) AS txt_ex_phase_ii,
	MAX(CASE 
	    WHEN data_id = 'txt_Ex_Phase_III' THEN 
	        CASE 
	            WHEN data_value = 'NA' THEN 'NA'
	            WHEN data_value = 'Na' THEN 'NA'
	            ELSE NULL
	        END
		END) AS txt_ex_phase_iii,
    MAX(CASE WHEN data_id = 'txt_Brief_corporate' THEN data_value END) AS txt_brief_corporate,
    MAX(CASE WHEN data_id = 'txt_Bank_Guarantee_date' THEN data_value::date END) AS txt_bank_guarantee_date,
    MAX(CASE WHEN data_id = 'txt_Bank_Guarantee_amt' THEN data_value::numeric END) AS txt_bank_guarantee_amt,
    MAX(CASE WHEN data_id = 'txt_Bank_Guarantee_from' THEN data_value::date END) AS txt_bank_guarantee_from,
    MAX(CASE WHEN data_id = 'txt_Bank_Guarantee_to' THEN data_value::date END) AS txt_bank_guarantee_to,
    MAX(CASE 
	    WHEN data_id = 'ddl_Deed_partnership' THEN 
	        CASE 
	            WHEN data_value = 'YES' THEN 'Yes'
	            WHEN data_value = 'NO' THEN 'No'
	            ELSE NULL
	        END
		END) AS ddl_Deed_partnership,    
    MAX(CASE 
	    WHEN data_id = 'ddl_Amendments' THEN 
	        CASE 
	            WHEN data_value = 'YES' THEN 'Yes'
	            WHEN data_value = 'NO' THEN 'No'
	            ELSE NULL
	        END
		END) AS ddl_Amendments,    
    MAX(CASE 
	    WHEN data_id = 'ddl_pending_dues' THEN 
	        CASE 
	            WHEN data_value = 'YES' THEN 'Yes'
	            WHEN data_value = 'NO' THEN 'No'
	            ELSE NULL
	        END
		END) AS ddl_pending_dues,    
    MAX(CASE 
        	WHEN data_id = 'ddl_compliances_psc' THEN 
            CASE WHEN data_value = 'YES' THEN '1' ELSE '0' END 
    END) AS ddl_compliances_psc
  FROM dgh_staging.form_assignment_interest_data
  WHERE data_id IN (
    'txt_Ex_Phase_I', 'txt_Ex_Phase_II', 'txt_Ex_Phase_III',
    'txt_Brief_corporate', 'txt_Bank_Guarantee_date', 'txt_Bank_Guarantee_amt',
    'txt_Bank_Guarantee_from', 'txt_Bank_Guarantee_to',
    'ddl_Deed_partnership', 'ddl_Amendments', 'ddl_pending_dues', 'ddl_compliances_psc'
  )
  GROUP BY refid
),
latest_assignment_interest AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY ref_id ORDER BY seq DESC) AS rn
  FROM dgh_staging.form_assignment_interest
),
comments_data AS (
  SELECT a.ref_id, cm.comment_data
  FROM dgh_staging.frm_comments cm
  JOIN dgh_staging.form_assignment_interest a ON cm.comment_id = a.comment_id
  WHERE a.status = '1'
)

INSERT INTO operator_contracts_agreements.t_assignment_participation_interest_details (
    assignment_participation_interest_application_number,
    process_id,
    block_category,
    block_name,
    consortium,
    existing_operator,
    new_operator,
    status_of_contractors,
    exploration_phase1_dropdown,
    exploration_phase2_dropdown,
    exploration_phase3_dropdown,
    technical_brief_infp_of_assignee,
    bg_submitted_date,
    bg_amount_usd,
    period_from,
    period_to,
    deed_of_assignment,
    draft_amendments_psc,
    pending_dues_cleared,
    is_compliance_checkbox,
    comments,
    is_declared,
    name_authorized_signatory,
    designation,
    is_active,
    created_by,
    creation_date,
    contractor_name,
    ref_psc_article,
    dos_contract,
    awarded_under,
    is_migrated,
    current_status 
)
SELECT
    fai.ref_id,
    11,
    fai.blockcategory,
    mbm.block_name,
    fai.consortium,
    fai.assignor,
    fai.assignee,
    fai.status_contractors,
    pd.txt_ex_phase_i,
    pd.txt_ex_phase_ii,
    pd.txt_ex_phase_iii,
    pd.txt_brief_corporate,
    pd.txt_bank_guarantee_date,
    pd.txt_bank_guarantee_amt,
    pd.txt_bank_guarantee_from,
    pd.txt_bank_guarantee_to,
    pd.ddl_deed_partnership,
    pd.ddl_amendments,
    pd.ddl_pending_dues,
    pd.ddl_compliances_psc::int,
    cd.comment_data,
    1,
    fai.name_auth_sig_contra,
    fai.designation,
    fai.is_active,
    mum.user_id,
    fai.created_on,
    fai.contractname,
    fai.ref_topsc_articalno,
    fai.dos_contract,
    fai.bid_round,
    1,
    'DRAFT'
FROM latest_assignment_interest fai
LEFT JOIN pivoted_data pd ON pd.refid = fai.ref_id
LEFT JOIN user_profile.m_user_master mum ON mum.migrated_user_id = fai.created_by
LEFT JOIN comments_data cd ON cd.ref_id = fai.ref_id
LEFT JOIN exploration_mining_permits.m_block_master mbm
ON mbm.migrated_block_id = fai.blockname::integer
AND mbm.regime = fai.blockcategory
WHERE fai.rn = 1 AND mum.is_migrated = 1;