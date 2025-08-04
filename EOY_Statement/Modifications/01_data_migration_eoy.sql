WITH data_pivot AS (
  SELECT
    A."REFID",
    MAX(CASE WHEN A."DATA_ID" = 'DDL_SRF_created' THEN A."DATA_VALUE" END) AS is_srf99_srg2018,
    MAX(CASE WHEN A."DATA_ID" = 'DDL_MC_approved_Budget_MCR' THEN A."DATA_VALUE" END) AS mc_approved_budget,
    MAX(CASE WHEN A."DATA_ID" = 'DDL_MC_approved_Appointment_MCR' THEN A."DATA_VALUE" END) AS is_mc_aoa_with_mcr,
    MAX(CASE WHEN A."DATA_ID" = 'ddl_Cost_Recoverable' THEN A."DATA_VALUE" END) AS cost_recoverable_as_per_psc_terms,
    MAX(CASE WHEN A."DATA_ID" = 'DDL_GOI' THEN A."DATA_VALUE" END) AS goi_pp_as_per_psc_terms,
    MAX(CASE WHEN A."DATA_ID" = 'DDL_Audited_Accounts1' THEN A."DATA_VALUE" END) AS audited_acc_statement,
    MAX(CASE WHEN A."DATA_ID" = 'DDL_OC_approved1' THEN A."DATA_VALUE" END) AS audited_acc_eoy_oc_approved,
    MAX(CASE WHEN A."DATA_ID" = 'ddl_last_year_Audited' THEN A."DATA_VALUE" END) AS last_year_audited_mc_approved,
    MAX(CASE WHEN A."DATA_ID" = 'DDL_Does_Audited1' THEN A."DATA_VALUE" END) AS audited_acc_eoy_disclaimer_1,
    MAX(CASE WHEN A."DATA_ID" = 'DDL_Annual_Expenditure_Budget' THEN A."DATA_VALUE" END) AS is_annual_expenditure_within_mc,
    MAX(CASE WHEN A."DATA_ID" = 'DDL_Did_MC_approv1' THEN A."DATA_VALUE" END) AS mc_approve_last_audited_acc_eoy_statement,
    MAX(CASE WHEN A."DATA_ID" = 'DDL_MC_approved_Budget1' THEN A."DATA_VALUE" END) AS mc_approved_budget_1,
    MAX(CASE WHEN A."DATA_ID" = 'DDL_MC_approved_Appointment1' THEN A."DATA_VALUE" END) AS mc_aoa_submitted,
    -- You can uncomment and include if needed:
    -- MAX(CASE WHEN A."DATA_ID" = 'DDL_Does_Cost_Recoverable1' THEN A."DATA_VALUE" END) AS cost_recoverable_as_per_psc_terms_alt,
    MAX(CASE WHEN A."DATA_ID" = 'DDL_Does_Sale1' THEN A."DATA_VALUE" END) AS sale_purchase_in_compliance_psc,
    MAX(CASE WHEN A."DATA_ID" = 'DDL_Does_all_outstanding1' THEN A."DATA_VALUE" END) AS outstanding_dues_goi_dgh_paid,
    MAX(CASE WHEN A."DATA_ID" = 'DDL_Compliance_PSC1' THEN A."DATA_VALUE" END) AS compliance_of_psc_terms,
    MAX(CASE WHEN A."DATA_ID" = 'ddl_necessary_disclaimer' THEN A."DATA_VALUE" END) AS audited_acc_eoy_disclaimer_2,
    MAX(CASE WHEN A."DATA_ID" = 'DDL_Profit_Petroleum1' THEN A."DATA_VALUE" END) AS period_date_amt_interest_final_profit_petroleum,
    MAX(CASE WHEN A."DATA_ID" = 'DDL_Profit_Petroleum_deposited1' THEN A."DATA_VALUE" END) AS period,
    MAX(CASE WHEN A."DATA_ID" = 'ddl_Are_Sale_Purchase' THEN A."DATA_VALUE" END) AS finance_sale_purchase_in_compliance_psc
  FROM dgh_staging.form_audited_accounts_data A
  JOIN dgh_staging.frm_workitem_master_new B ON A."REFID" = B.ref_id
  WHERE A."STATUS" = '1'
  GROUP BY A."REFID"
)

INSERT INTO financial_mgmt.t_eoy_statement_and_audited_accounts_details 
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
    is_migrated,

    is_srf99_srg2018,
    mc_approved_budget,
    is_mc_aoa_with_mcr,
    cost_recoverable_as_per_psc_terms,
    goi_pp_as_per_psc_terms,
    audited_acc_statement,
    audited_acc_eoy_oc_approved,
    last_year_audited_mc_approved,
    audited_acc_eoy_disclaimer,
    is_annual_expenditure_within_mc,
    mc_approve_last_audited_acc_eoy_statement,
    mc_aoa_submitted,
    sale_purchase_in_compliance_psc,
    outstanding_dues_goi_dgh_paid,
    compliance_of_psc_terms,
    period_date_amt_interest_final_profit_petroleum,
    period,
    finance_sale_purchase_in_compliance_psc
)
SELECT
    f."REF_ID",
    f."BLOCKNAME",
    f."BLOCKCATEGORY",
    f."BID_ROUND",
    CASE WHEN UPPER(COALESCE(f."OCR_AVAIABLE", '')) = 'YES' THEN 1 ELSE 0 END,
    f."OCR_UNAVAIABLE_TXT",
    '{"acceptTerm1": true}'::JSONB,
    f."NAME_AUTH_SIG_CONTRA",
    f."DESIGNATION",
    1,
    8,
    fc.comment_data,
    CASE WHEN f."TYPE_DATA" = 'EDP' THEN 0 WHEN f."TYPE_DATA" = 'ES' THEN 1 ELSE NULL END,
    m.user_id,
    'DRAFT',
    1,

    CASE 
      WHEN UPPER(data_pivot.is_srf99_srg2018) = 'YES' THEN 1
      WHEN UPPER(data_pivot.is_srf99_srg2018) = 'NO' THEN 0
      WHEN UPPER(data_pivot.is_srf99_srg2018) = 'NA' THEN 2
      ELSE NULL
    END AS is_srf99_srg2018,

    CASE 
      WHEN UPPER(data_pivot.mc_approved_budget) = 'YES' THEN 1
      WHEN UPPER(data_pivot.mc_approved_budget) = 'NO' THEN 0
      WHEN UPPER(data_pivot.mc_approved_budget) = 'NA' THEN 2
      ELSE 
        CASE 
          WHEN UPPER(data_pivot.mc_approved_budget_1) = 'YES' THEN 1
          WHEN UPPER(data_pivot.mc_approved_budget_1) = 'NO' THEN 0
          WHEN UPPER(data_pivot.mc_approved_budget_1) = 'NA' THEN 2
          ELSE NULL
        END
    END AS mc_approved_budget,

    CASE 
      WHEN UPPER(data_pivot.is_mc_aoa_with_mcr) = 'YES' THEN 1
      WHEN UPPER(data_pivot.is_mc_aoa_with_mcr) = 'NO' THEN 0
      WHEN UPPER(data_pivot.is_mc_aoa_with_mcr) = 'NA' THEN 2
      ELSE NULL
    END AS is_mc_aoa_with_mcr,

    CASE 
      WHEN UPPER(data_pivot.cost_recoverable_as_per_psc_terms) = 'YES' THEN 1
      WHEN UPPER(data_pivot.cost_recoverable_as_per_psc_terms) = 'NO' THEN 0
      WHEN UPPER(data_pivot.cost_recoverable_as_per_psc_terms) = 'NA' THEN 2
      ELSE NULL
    END AS cost_recoverable_as_per_psc_terms,

    CASE 
      WHEN UPPER(data_pivot.goi_pp_as_per_psc_terms) = 'YES' THEN 1
      WHEN UPPER(data_pivot.goi_pp_as_per_psc_terms) = 'NO' THEN 0
      WHEN UPPER(data_pivot.goi_pp_as_per_psc_terms) = 'NA' THEN 2
      ELSE NULL
    END AS goi_pp_as_per_psc_terms,

    CASE 
      WHEN UPPER(data_pivot.audited_acc_statement) = 'YES' THEN 1
      WHEN UPPER(data_pivot.audited_acc_statement) = 'NO' THEN 0
      WHEN UPPER(data_pivot.audited_acc_statement) = 'NA' THEN 2
      ELSE NULL
    END AS audited_acc_statement,

    CASE 
      WHEN UPPER(data_pivot.audited_acc_eoy_oc_approved) = 'YES' THEN 1
      WHEN UPPER(data_pivot.audited_acc_eoy_oc_approved) = 'NO' THEN 0
      WHEN UPPER(data_pivot.audited_acc_eoy_oc_approved) = 'NA' THEN 2
      ELSE NULL
    END AS audited_acc_eoy_oc_approved,

    CASE 
      WHEN UPPER(data_pivot.last_year_audited_mc_approved) = 'YES' THEN 1
      WHEN UPPER(data_pivot.last_year_audited_mc_approved) = 'NO' THEN 0
      WHEN UPPER(data_pivot.last_year_audited_mc_approved) = 'NA' THEN 2
      ELSE NULL
    END AS last_year_audited_mc_approved,

    COALESCE(
      CASE 
        WHEN UPPER(data_pivot.audited_acc_eoy_disclaimer_1) = 'YES' THEN 1
        WHEN UPPER(data_pivot.audited_acc_eoy_disclaimer_1) = 'NO' THEN 0
        WHEN UPPER(data_pivot.audited_acc_eoy_disclaimer_1) = 'NA' THEN 2
        ELSE NULL
      END,
      CASE 
        WHEN UPPER(data_pivot.audited_acc_eoy_disclaimer_2) = 'YES' THEN 1
        WHEN UPPER(data_pivot.audited_acc_eoy_disclaimer_2) = 'NO' THEN 0
        WHEN UPPER(data_pivot.audited_acc_eoy_disclaimer_2) = 'NA' THEN 2
        ELSE NULL
      END
    ) AS audited_acc_eoy_disclaimer,

    CASE 
      WHEN UPPER(data_pivot.is_annual_expenditure_within_mc) = 'YES' THEN 1
      WHEN UPPER(data_pivot.is_annual_expenditure_within_mc) = 'NO' THEN 0
      WHEN UPPER(data_pivot.is_annual_expenditure_within_mc) = 'NA' THEN 2
      ELSE NULL
    END AS is_annual_expenditure_within_mc,

    CASE 
      WHEN UPPER(data_pivot.mc_approve_last_audited_acc_eoy_statement) = 'YES' THEN 1
      WHEN UPPER(data_pivot.mc_approve_last_audited_acc_eoy_statement) = 'NO' THEN 0
      WHEN UPPER(data_pivot.mc_approve_last_audited_acc_eoy_statement) = 'NA' THEN 2
      ELSE NULL
    END AS mc_approve_last_audited_acc_eoy_statement,

    CASE 
      WHEN UPPER(data_pivot.mc_aoa_submitted) = 'YES' THEN 1
      WHEN UPPER(data_pivot.mc_aoa_submitted) = 'NO' THEN 0
      WHEN UPPER(data_pivot.mc_aoa_submitted) = 'NA' THEN 2
      ELSE NULL
    END AS mc_aoa_submitted,

    CASE 
      WHEN UPPER(data_pivot.sale_purchase_in_compliance_psc) = 'YES' THEN 1
      WHEN UPPER(data_pivot.sale_purchase_in_compliance_psc) = 'NO' THEN 0
      WHEN UPPER(data_pivot.sale_purchase_in_compliance_psc) = 'NA' THEN 2
      ELSE NULL
    END AS sale_purchase_in_compliance_psc,

    CASE 
      WHEN UPPER(data_pivot.outstanding_dues_goi_dgh_paid) = 'YES' THEN 1
      WHEN UPPER(data_pivot.outstanding_dues_goi_dgh_paid) = 'NO' THEN 0
      WHEN UPPER(data_pivot.outstanding_dues_goi_dgh_paid) = 'NA' THEN 2
      ELSE NULL
    END AS outstanding_dues_goi_dgh_paid,

    CASE 
      WHEN UPPER(data_pivot.compliance_of_psc_terms) = 'YES' THEN 1
      WHEN UPPER(data_pivot.compliance_of_psc_terms) = 'NO' THEN 0
      WHEN UPPER(data_pivot.compliance_of_psc_terms) = 'NA' THEN 2
      ELSE NULL
    END AS compliance_of_psc_terms,

    data_pivot.period_date_amt_interest_final_profit_petroleum,

    CASE 
      WHEN data_pivot.period ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(data_pivot.period, 'DD/MM/YYYY')
      ELSE NULL
    END AS period,

    CASE 
      WHEN UPPER(data_pivot.finance_sale_purchase_in_compliance_psc) = 'YES' THEN 1
      WHEN UPPER(data_pivot.finance_sale_purchase_in_compliance_psc) = 'NO' THEN 0
      WHEN UPPER(data_pivot.finance_sale_purchase_in_compliance_psc) = 'NA' THEN 2
      ELSE NULL
    END AS finance_sale_purchase_in_compliance_psc

FROM dgh_staging.form_audited_accounts f
LEFT JOIN (
  SELECT COMMENT_ID, MAX(comment_data) AS comment_data
  FROM dgh_staging.frm_comments
  WHERE is_active = '1'
  GROUP BY COMMENT_ID
) fc ON fc.COMMENT_ID = f."COMMENT_ID"
JOIN dgh_staging.frm_workitem_master_new c 
    ON f."REF_ID" = c.ref_id
JOIN user_profile.m_user_master m 
    ON m.migrated_user_id = f."CREATED_BY"
LEFT JOIN data_pivot 
    ON f."REF_ID" = data_pivot."REFID"
WHERE f."STATUS" = '1' 
  AND f."IS_ACTIVE" = '1'
  AND f."REF_ID" = 'AUA-20210222070200';
