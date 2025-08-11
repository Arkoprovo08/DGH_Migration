INSERT INTO financial_mgmt.t_eoy_statement_and_audited_accounts_details 
(
    eoy_statement_and_audited_accounts_details_application_number,
    goi_pp_as_per_psc_terms,
    finance_mc_approved_re_budget,
    is_annual_expenditure_within_mc,
    sale_purchase_goods_compliance_psc,
    audited_acc_statement,
    compliance_of_psc_terms,
    cost_recoverable_as_per_psc_terms,
    mc_approve_last_audited_acc_eoy_statement,
    outstanding_dues_goi_dgh_paid,
    audited_acc_eoy_disclaimer,
    finance_cost_recoverable_as_per_psc_terms,
    finance_goi_pp_as_per_psc_terms,
    revenue_computed_as_per_psc_terms,
    finance_sale_purchase_in_compliance_psc,
    last_year_audited_mc_approved,
    finance_mc_aoa_submitted,
    finance_mc_approved_budget,
    audited_acc_eoy_oc_approved,
    date_amt_final_profit_petroleum,
    period_date_amt_interest_final_profit_petroleum,
    royalty_cess_statement_paid_year_submitted,
    is_srf99_srg2018,
    variance_analysis_actual_vs_budget,
    mc_aoa_submitted,
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
    a."REFID" AS eoy_statement_and_audited_accounts_details_application_number,
    MAX(CASE WHEN "DATA_ID" = 'ddl_GOI' AND "DATA_VALUE" = 'YES' THEN 1
             WHEN "DATA_ID" = 'ddl_GOI' AND "DATA_VALUE" = 'NO' THEN 0
             WHEN "DATA_ID" = 'ddl_GOI' AND "DATA_VALUE" = 'NA' THEN 2 END) AS goi_pp_as_per_psc_terms,

    MAX(CASE WHEN "DATA_ID" = 'DDL_Annual_Expenditure_is' AND "DATA_VALUE" = 'YES' THEN 1
             WHEN "DATA_ID" = 'DDL_Annual_Expenditure_is' AND "DATA_VALUE" = 'NO' THEN 0
             WHEN "DATA_ID" = 'DDL_Annual_Expenditure_is' AND "DATA_VALUE" = 'NA' THEN 2 END) AS finance_mc_approved_re_budget,

    MAX(CASE WHEN "DATA_ID" = 'DDL_Annual_Expenditure1' AND "DATA_VALUE" = 'YES' THEN 1
             WHEN "DATA_ID" = 'DDL_Annual_Expenditure1' AND "DATA_VALUE" = 'NO' THEN 0
             WHEN "DATA_ID" = 'DDL_Annual_Expenditure1' AND "DATA_VALUE" = 'NA' THEN 2 END) AS is_annual_expenditure_within_mc,

    MAX(CASE WHEN "DATA_ID" = 'ddl_Are_Sale_Purchase' AND "DATA_VALUE" = 'YES' THEN 1
             WHEN "DATA_ID" = 'ddl_Are_Sale_Purchase' AND "DATA_VALUE" = 'NO' THEN 0
             WHEN "DATA_ID" = 'ddl_Are_Sale_Purchase' AND "DATA_VALUE" = 'NA' THEN 2 END) AS sale_purchase_goods_compliance_psc,

    MAX(CASE WHEN "DATA_ID" = 'DDL_Audited_Accounts1' AND "DATA_VALUE" = 'YES' THEN 1
             WHEN "DATA_ID" = 'DDL_Audited_Accounts1' AND "DATA_VALUE" = 'NO' THEN 0
             WHEN "DATA_ID" = 'DDL_Audited_Accounts1' AND "DATA_VALUE" = 'NA' THEN 2 END) AS audited_acc_statement,

    MAX(CASE WHEN "DATA_ID" = 'DDL_Compliance_PSC1' AND "DATA_VALUE" = 'YES' THEN 1
             WHEN "DATA_ID" = 'DDL_Compliance_PSC1' AND "DATA_VALUE" = 'NO' THEN 0
             WHEN "DATA_ID" = 'DDL_Compliance_PSC1' AND "DATA_VALUE" = 'NA' THEN 2 END) AS compliance_of_psc_terms,

    MAX(CASE WHEN "DATA_ID" = 'ddl_Cost_Recoverable' AND "DATA_VALUE" = 'YES' THEN 1
             WHEN "DATA_ID" = 'ddl_Cost_Recoverable' AND "DATA_VALUE" = 'NO' THEN 0
             WHEN "DATA_ID" = 'ddl_Cost_Recoverable' AND "DATA_VALUE" = 'NA' THEN 2 END) AS cost_recoverable_as_per_psc_terms,

    MAX(CASE WHEN "DATA_ID" = 'DDL_Did_MC_approv1' AND "DATA_VALUE" = 'YES' THEN 1
             WHEN "DATA_ID" = 'DDL_Did_MC_approv1' AND "DATA_VALUE" = 'NO' THEN 0
             WHEN "DATA_ID" = 'DDL_Did_MC_approv1' AND "DATA_VALUE" = 'NA' THEN 2 END) AS mc_approve_last_audited_acc_eoy_statement,

    MAX(CASE WHEN "DATA_ID" = 'DDL_Does_all_outstanding1' AND "DATA_VALUE" = 'YES' THEN 1
             WHEN "DATA_ID" = 'DDL_Does_all_outstanding1' AND "DATA_VALUE" = 'NO' THEN 0
             WHEN "DATA_ID" = 'DDL_Does_all_outstanding1' AND "DATA_VALUE" = 'NA' THEN 2 END) AS outstanding_dues_goi_dgh_paid,

    MAX(CASE WHEN "DATA_ID" = 'DDL_Does_Audited1' AND "DATA_VALUE" = 'YES' THEN 1
             WHEN "DATA_ID" = 'DDL_Does_Audited1' AND "DATA_VALUE" = 'NO' THEN 0
             WHEN "DATA_ID" = 'DDL_Does_Audited1' AND "DATA_VALUE" = 'NA' THEN 2 END) AS audited_acc_eoy_disclaimer,

    MAX(CASE WHEN "DATA_ID" = 'DDL_Does_Cost_Recoverable1' AND "DATA_VALUE" = 'YES' THEN 1
             WHEN "DATA_ID" = 'DDL_Does_Cost_Recoverable1' AND "DATA_VALUE" = 'NO' THEN 0
             WHEN "DATA_ID" = 'DDL_Does_Cost_Recoverable1' AND "DATA_VALUE" = 'NA' THEN 2 END) AS finance_cost_recoverable_as_per_psc_terms,

    MAX(CASE WHEN "DATA_ID" = 'DDL_Does_GOI1' AND "DATA_VALUE" = 'YES' THEN 1
             WHEN "DATA_ID" = 'DDL_Does_GOI1' AND "DATA_VALUE" = 'NO' THEN 0
             WHEN "DATA_ID" = 'DDL_Does_GOI1' AND "DATA_VALUE" = 'NA' THEN 2 END) AS finance_goi_pp_as_per_psc_terms,

    MAX(CASE WHEN "DATA_ID" = 'DDL_Does_Revenue_computed1' AND "DATA_VALUE" = 'YES' THEN 1
             WHEN "DATA_ID" = 'DDL_Does_Revenue_computed1' AND "DATA_VALUE" = 'NO' THEN 0
             WHEN "DATA_ID" = 'DDL_Does_Revenue_computed1' AND "DATA_VALUE" = 'NA' THEN 2 END) AS revenue_computed_as_per_psc_terms,

    MAX(CASE WHEN "DATA_ID" = 'DDL_Does_Sale1' AND "DATA_VALUE" = 'YES' THEN 1
             WHEN "DATA_ID" = 'DDL_Does_Sale1' AND "DATA_VALUE" = 'NO' THEN 0
             WHEN "DATA_ID" = 'DDL_Does_Sale1' AND "DATA_VALUE" = 'NA' THEN 2 END) AS finance_sale_purchase_in_compliance_psc,

    MAX(CASE WHEN "DATA_ID" = 'ddl_last_year_Audited' AND "DATA_VALUE" = 'YES' THEN 1
             WHEN "DATA_ID" = 'ddl_last_year_Audited' AND "DATA_VALUE" = 'NO' THEN 0
             WHEN "DATA_ID" = 'ddl_last_year_Audited' AND "DATA_VALUE" = 'NA' THEN 2 END) AS last_year_audited_mc_approved,

    MAX(CASE WHEN "DATA_ID" = 'DDL_MC_approved_Appointment1' AND "DATA_VALUE" = 'YES' THEN 1
             WHEN "DATA_ID" = 'DDL_MC_approved_Appointment1' AND "DATA_VALUE" = 'NO' THEN 0
             WHEN "DATA_ID" = 'DDL_MC_approved_Appointment1' AND "DATA_VALUE" = 'NA' THEN 2 END) AS finance_mc_aoa_submitted,

    MAX(CASE WHEN "DATA_ID" = 'DDL_MC_approved_Budget1' AND "DATA_VALUE" = 'YES' THEN 1
             WHEN "DATA_ID" = 'DDL_MC_approved_Budget1' AND "DATA_VALUE" = 'NO' THEN 0
             WHEN "DATA_ID" = 'DDL_MC_approved_Budget1' AND "DATA_VALUE" = 'NA' THEN 2 END) AS finance_mc_approved_budget,

    MAX(CASE WHEN "DATA_ID" = 'DDL_OC_approved1' AND "DATA_VALUE" = 'YES' THEN 1
             WHEN "DATA_ID" = 'DDL_OC_approved1' AND "DATA_VALUE" = 'NO' THEN 0
             WHEN "DATA_ID" = 'DDL_OC_approved1' AND "DATA_VALUE" = 'NA' THEN 2 END) AS audited_acc_eoy_oc_approved,

    MAX(CASE WHEN "DATA_ID" = 'DDL_Profit_Petroleum1' AND "DATA_VALUE" = 'YES' THEN 1
             WHEN "DATA_ID" = 'DDL_Profit_Petroleum1' AND "DATA_VALUE" = 'NO' THEN 0
             WHEN "DATA_ID" = 'DDL_Profit_Petroleum1' AND "DATA_VALUE" = 'NA' THEN 2 END) AS date_amt_final_profit_petroleum,

    MAX(CASE WHEN "DATA_ID" = 'DDL_Profit_Petroleum_deposited1' AND "DATA_VALUE" = 'YES' THEN 1
             WHEN "DATA_ID" = 'DDL_Profit_Petroleum_deposited1' AND "DATA_VALUE" = 'NO' THEN 0
             WHEN "DATA_ID" = 'DDL_Profit_Petroleum_deposited1' AND "DATA_VALUE" = 'NA' THEN 2 END) AS period_date_amt_interest_final_profit_petroleum,

    MAX(CASE WHEN "DATA_ID" = 'DDL_Royalty_Cess1' AND "DATA_VALUE" = 'YES' THEN 1
             WHEN "DATA_ID" = 'DDL_Royalty_Cess1' AND "DATA_VALUE" = 'NO' THEN 0
             WHEN "DATA_ID" = 'DDL_Royalty_Cess1' AND "DATA_VALUE" = 'NA' THEN 2 END) AS royalty_cess_statement_paid_year_submitted,

    MAX(CASE WHEN "DATA_ID" = 'ddl_SRF_created' AND "DATA_VALUE" = 'YES' THEN 1
             WHEN "DATA_ID" = 'ddl_SRF_created' AND "DATA_VALUE" = 'NO' THEN 0
             WHEN "DATA_ID" = 'ddl_SRF_created' AND "DATA_VALUE" = 'NA' THEN 2 END) AS is_srf99_srg2018,

    MAX(CASE WHEN "DATA_ID" = 'DDL_Variance_Analysis1' AND "DATA_VALUE" = 'YES' THEN 1
             WHEN "DATA_ID" = 'DDL_Variance_Analysis1' AND "DATA_VALUE" = 'NO' THEN 0
             WHEN "DATA_ID" = 'DDL_Variance_Analysis1' AND "DATA_VALUE" = 'NA' THEN 2 END) AS variance_analysis_actual_vs_budget,

    MAX(CASE WHEN "DATA_ID" = 'DDL_MC_approved_Appointment_MCR' AND "DATA_VALUE" = 'YES' THEN 1
             WHEN "DATA_ID" = 'DDL_MC_approved_Appointment_MCR' AND "DATA_VALUE" = 'NO' THEN 0
             WHEN "DATA_ID" = 'DDL_MC_approved_Appointment_MCR' AND "DATA_VALUE" = 'NA' THEN 2 END) AS mc_aoa_submitted,

    -- Second insert's direct mappings
    f."BLOCKNAME" AS block_name,
    f."BLOCKCATEGORY" AS block_category,
    f."BID_ROUND" AS awarded_under,
    CASE WHEN UPPER(COALESCE(f."OCR_AVAIABLE", '')) = 'YES' THEN 1 ELSE 0 END AS is_ocr_available,
    f."OCR_UNAVAIABLE_TXT" AS ocr_unaivalibility_text,
    '{"acceptTerm1": true}'::JSONB AS declaration_checkbox,
    f."NAME_AUTH_SIG_CONTRA" AS name_of_authorised_signatory,
    f."DESIGNATION" AS designation,
    1 AS is_declared,
    8 AS process_id,
    fc.comment_data AS remarks,
    CASE WHEN f."TYPE_DATA" = 'EDP' THEN 0
         WHEN f."TYPE_DATA" = 'ES'  THEN 1 ELSE NULL END AS type_of_data_submitted,
    m.user_id AS created_by,
    'DRAFT' AS current_status,
    1 AS is_migrated

FROM dgh_staging.form_audited_accounts_data a
JOIN dgh_staging.frm_workitem_master_new fwmn 
    ON fwmn.ref_id = a."REFID"
JOIN dgh_staging.form_audited_accounts f 
    ON f."REF_ID" = a."REFID"
LEFT JOIN (
    SELECT COMMENT_ID, MAX(comment_data) AS comment_data
    FROM dgh_staging.frm_comments
    WHERE is_active = '1'
    GROUP BY COMMENT_ID
) fc ON fc.COMMENT_ID = f."COMMENT_ID"
JOIN user_profile.m_user_master m 
    ON m.migrated_user_id = f."CREATED_BY"
WHERE f."STATUS" = '1' AND f."IS_ACTIVE" = '1'
GROUP BY 
    a."REFID", f."BLOCKNAME", f."BLOCKCATEGORY", f."BID_ROUND", f."OCR_AVAIABLE", 
    f."OCR_UNAVAIABLE_TXT", f."NAME_AUTH_SIG_CONTRA", f."DESIGNATION", 
    fc.comment_data, f."TYPE_DATA", m.user_id;
