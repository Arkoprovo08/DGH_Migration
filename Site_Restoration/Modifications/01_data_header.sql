WITH data_pivot AS (
  SELECT
    "REFID",
    MAX(CASE WHEN "DATA_ID" = 'DDL_SRF_1999' THEN "DATA_VALUE" END) AS srf_1999_value,
    MAX(CASE WHEN "DATA_ID" = 'DDL_SRF_2018' THEN "DATA_VALUE" END) AS srf_2018_value,
    MAX(CASE WHEN "DATA_ID" = 'DDL_SRF_2021' THEN "DATA_VALUE" END) AS srf_2021_value,
    MAX(CASE WHEN "DATA_ID" = 'DDL_Profit_Petroleum' THEN "DATA_VALUE" END) AS profit_petroleum,
    MAX(CASE WHEN "DATA_ID" = 'ddl_Bank_Guarantee' THEN "DATA_VALUE" END) AS bank_guarantee,
    MAX(CASE WHEN "DATA_ID" = 'txt_Bank_Guarantee_from_date' THEN "DATA_VALUE" END) AS bg_from_date,
    MAX(CASE WHEN "DATA_ID" = 'txt_Bank_Guarantee_to_date' THEN "DATA_VALUE" END) AS bg_to_date,
    MAX(CASE WHEN "DATA_ID" = 'txt_Bank_Guarantee_date' THEN "DATA_VALUE" END) AS bg_date,
    MAX(CASE WHEN "DATA_ID" = 'txt_Bank_Guarantee_amt' THEN "DATA_VALUE" END) AS bg_amt,
    MAX(CASE WHEN "DATA_ID" = 'ddl_details_calculation' THEN "DATA_VALUE" END) AS details_calculation,
    MAX(CASE WHEN "DATA_ID" = 'TXT_REF_TOPSC_ARTICALNO' THEN "DATA_VALUE" END) AS psc_article_no
  FROM dgh_staging.form_site_restoration_data a
  join dgh_staging.frm_workitem_master_new f on a."REFID" = f.ref_id 
  WHERE "DATA_ID" IN (
    'DDL_SRF_1999', 'DDL_SRF_2018', 'DDL_SRF_2021', 'DDL_Profit_Petroleum',
    'ddl_Bank_Guarantee', 'txt_Bank_Guarantee_from_date',
    'txt_Bank_Guarantee_to_date', 'txt_Bank_Guarantee_date',
    'txt_Bank_Guarantee_amt', 'ddl_details_calculation',
    'TXT_REF_TOPSC_ARTICALNO'
  )
  GROUP BY "REFID"
)

-- 1. Add consortium to the INSERT columns list in its correct position
INSERT INTO site_restoration.t_site_restoration_abandonment_details (
    site_restoration_abandonment_application_number,
    contractor_name,
    block_category,
    eff_date_contract,
    block_name,
    consortium,  -- <-- Added consortium field here!
    ocr_available,
    ocr_unaivailability_text,
    created_by,
    creation_date,
    awarded_under,
    designation,
    dos_contract,
    name_authorized_signatory,
    current_status,
    process_id,
    is_active,
    is_migrated,
    remarks,
    declaration_checkbox,
    srf_1999_value,
    srg_2018_value,
    srg_2021_value,
    compliance_psc,
    date_amt_period_bg,
    yes_period_from,
    yes_period_to,
    date_input,
    amount_usd,
    is_calculation_of_bg,
    psc_article_no
)
SELECT
    fsr."REF_ID",
    fsr."CONTRACTNAME",
    fsr."BLOCKCATEGORY",
    fsr."DATE_EFFECTIVE",
    fsr."BLOCKNAME",
    fsr."CONSORTIUM",  -- <-- Select source consortium value here!
    CASE WHEN UPPER(COALESCE(fsr."OCR_AVAIABLE", '')) = 'YES' THEN 1 ELSE 0 END,
    fsr."OCR_UNAVAIABLE_TXT",
    mup.user_id,
    fsr."CREATED_ON",
    fsr."BID_ROUND",
    fsr."DESIGNATION",
    fsr."DOS_CONTRACT",
    fsr."NAME_AUTH_SIG_CONTRA",
    'DRAFT',
    24,
    1,
    1,
    fc.comment_data,
    '{"acceptTerm1": true}'::jsonb,
    CASE
        WHEN UPPER(data_pivot.srf_1999_value) = 'YES' THEN 'Yes'
        WHEN UPPER(data_pivot.srf_1999_value) = 'NO' THEN 'No'
        WHEN UPPER(data_pivot.srf_1999_value) = 'NA' THEN 'NA'
        ELSE NULL
    END,
    CASE
        WHEN UPPER(data_pivot.srf_2018_value) = 'YES' THEN 'Yes'
        WHEN UPPER(data_pivot.srf_2018_value) = 'NO' THEN 'No'
        WHEN UPPER(data_pivot.srf_2018_value) = 'NA' THEN 'NA'
        ELSE NULL
    END,
    CASE
        WHEN UPPER(data_pivot.srf_2021_value) = 'YES' THEN 'Yes'
        WHEN UPPER(data_pivot.srf_2021_value) = 'NO' THEN 'No'
        WHEN UPPER(data_pivot.srf_2021_value) = 'NA' THEN 'NA'
        ELSE NULL
    END,
    CASE
        WHEN UPPER(COALESCE(data_pivot.profit_petroleum, '')) = 'YES' THEN 1
        ELSE 0
    END,
    CASE
        WHEN UPPER(data_pivot.bank_guarantee) = 'YES' THEN 1
        WHEN UPPER(data_pivot.bank_guarantee) = 'NO' THEN 0
        ELSE NULL
    END,
    TO_DATE(data_pivot.bg_from_date, 'DD/MM/YYYY'),
    TO_DATE(data_pivot.bg_to_date, 'DD/MM/YYYY'),
    TO_DATE(data_pivot.bg_date, 'DD/MM/YYYY'),
    NULLIF(data_pivot.bg_amt, '')::NUMERIC,
    CASE
        WHEN UPPER(COALESCE(data_pivot.details_calculation, '')) = 'YES' THEN 1
        WHEN UPPER(data_pivot.bank_guarantee) = 'NO' THEN 0
        WHEN UPPER(data_pivot.bank_guarantee) = '0' THEN NULL
        ELSE NULL
    END,
    data_pivot.psc_article_no
FROM dgh_staging.form_site_restoration fsr
JOIN dgh_staging.frm_workitem_master_new wm
    ON fsr."REF_ID" = wm.ref_id
LEFT JOIN dgh_staging.frm_comments fc
    ON fc.comment_id = fsr."COMMENT_ID"
JOIN user_profile.m_user_master mup
    ON mup.migrated_user_id = fsr."CREATED_BY"
LEFT JOIN data_pivot
    ON fsr."REF_ID" = data_pivot."REFID"
LEFT JOIN site_restoration.t_site_restoration_abandonment_details tgt
    ON tgt.site_restoration_abandonment_application_number = fsr."REF_ID"
WHERE fsr."STATUS" = 1
  AND fsr."IS_ACTIVE" = 1
  AND tgt.site_restoration_abandonment_application_number IS NULL
;