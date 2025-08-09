INSERT INTO financial_mgmt.t_cost_and_profit_petroleum_calculations (
    quarterly_report_cost_and_profit_petroleum_calculations_applica,
    block_category,
    block_name, 
    contractor_name,
    effective_date,
    psc_date_amt_provisional_profit, 
    psc_1_date, 
    psc_1_amount_usd,
    psc_1_utr_details,
    psc_2_from_date,
    psc_2_amount_usd,
    psc_2_utr_details,
    psc_mc_approved_audited_acc, 
    psc_compliance_of_psc,
    name_authorised_signatory,
    designation,
    created_by,
    creation_date, 
    is_active,
    psc_2_to_date,
    psc_2_date,
    dos_contract,
    awarded_under,
    is_migrated, 
    is_declared,
    process_id,
    current_status,
    psc_period_date_amt_of_interest,
    remarks
)
SELECT
    f."REFID",
    f."BLOCKCATEGORY",
    f."BLOCKNAME",
	  CASE
	    WHEN "CONSORTIUM" NOT LIKE '%,%' AND "CONSORTIUM" NOT LIKE '%(%'
	      THEN "CONSORTIUM" || '>>100>>0'
	    ELSE (
	      SELECT string_agg(
	        trim(regexp_replace(x, '^(.+)\((\d+)\)$', '\1>>\2>>0')),
	        ', '
	      )
	      FROM unnest(string_to_array("CONSORTIUM", ',')) AS x
	    )
	  END AS consortium_transformed,
    f."DATE_EFFECTIVE",
    CASE 
	    WHEN f."PROFIT_PETROLEUM" IS NULL OR f."PROFIT_PETROLEUM" = '0' THEN NULL
	    WHEN UPPER(f."PROFIT_PETROLEUM") = 'NO' THEN 'No'
	    WHEN UPPER(f."PROFIT_PETROLEUM") = 'YES' THEN 'Yes'
	    ELSE f."PROFIT_PETROLEUM"
	end,
    f."PETROLEUM_DATE"::date,
    f."PETROLEUM_AMOUNT",
    f."PETROLEUM_UTR",
    f."FROM_DATE_INTEREST"::date,
    f."AMOUNT_INTEREST",
    f."UTR",
    CASE 
	    WHEN UPPER(f."DOES_MC") = 'YES' THEN 1
	    WHEN UPPER(f."DOES_MC") = 'NO' THEN 0
    ELSE NULL END,
    CASE 
	    WHEN UPPER(f."COMPLIANCE_PSC") = 'YES' THEN 1 
	    WHEN UPPER(f."COMPLIANCE_PSC") = 'NO' THEN 0     
	ELSE NULL END,
    f."NAME_AUTH_SIG_CONTRA",
    f."DESIGNATION",
    COALESCE(um.user_id, 5),
    f."CREATED_ON",
    1,
    f."TO_DATE_INTEREST"::date,
    f."DATE_INTEREST"::date,
    f."DOS_CONTRACT",
    f."BID_ROUND",
    1,
    1, 
    34, 
    'DRAFT',
    CASE 
	    WHEN f."PROFIT_PETROLEUM_DEPOSITED" IS NULL OR f."PROFIT_PETROLEUM_DEPOSITED" = '0' THEN NULL
	    WHEN UPPER(f."PROFIT_PETROLEUM_DEPOSITED") = 'NO' THEN 'No'
	    WHEN UPPER(f."PROFIT_PETROLEUM_DEPOSITED") = 'YES' THEN 'Yes'
	    ELSE f."PROFIT_PETROLEUM_DEPOSITED"
	END,
    fc.comment_data
FROM dgh_staging.FORM_PROFIT_PETROLEUM f
INNER JOIN dgh_staging.frm_workitem_master_new fw ON fw.ref_id = f."REFID"
LEFT JOIN LATERAL (
    SELECT comment_data
    FROM dgh_staging.FRM_COMMENTS c
    WHERE c.COMMENT_ID = f."COMMENTID"
    LIMIT 1
) fc ON TRUE
LEFT JOIN user_profile.m_user_master um
    ON um.is_migrated = 1 AND um.migrated_user_id = f."CREATED_BY"
WHERE f."STATUS" = '1';
