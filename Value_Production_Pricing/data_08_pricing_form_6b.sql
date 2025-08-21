INSERT INTO financial_mgmt.t_pricing_statement_form6b_details 
(
    pricing_statement_details_header_id,
    is_sales,
    is_appropriation,
    name,
    associated_m3,
    associated_mmbtu,
    associated_price,
    associated_ref_price,
    associated_value,
    non_associated_m3,
    non_associated_mmbtu,
    non_associated_price,
    non_associated_ref_price,
    non_associated_value,
    is_active,
    is_migrated
)
SELECT 
    tp.pricing_statement_details_header_id                          AS pricing_statement_details_header_id,
    1                                                              AS is_sales,
    0                                                              AS is_appropriation,
    fps."LABEL_VALUE"                                               AS name,
    fps."GAS_SAL_ASS_M3"::NUMERIC                                            AS associated_m3,
    fps."GAS_SAL_ASS_MMBTU"  ::NUMERIC                                       AS associated_mmbtu,
    fps."GAS_SAL_ASS_PRI_MMBTU" ::NUMERIC                                    AS associated_price,
    fps."GAS_SAL_ASS_REF_TO_PRI"::NUMERIC                                    AS associated_ref_price,
    fps."GAS_SAL_ASS_VALUE"    ::NUMERIC                                     AS associated_value,
    fps."GAS_SAL_NON_ASS_M3"::NUMERIC                                        AS non_associated_m3,
    fps."GAS_SAL_NON_ASS_MMBTU" ::NUMERIC                                    AS non_associated_mmbtu,
    fps."GAS_SAL_NON_ASS_PRI_MMBTU" ::NUMERIC                                AS non_associated_price,
    fps."GAS_SAL_NON_ASS_REF_TO_PRI" ::NUMERIC                               AS non_associated_ref_price,
    fps."GAS_SAL_NON_ASS_VALUE"     ::NUMERIC                                AS non_associated_value,
    1                                                              AS is_active,
    1                                                              AS is_migrated
FROM dgh_staging.form_prod_val_sta_data_5c fps
JOIN financial_mgmt.t_value_of_production_and_pricing_statement_details_header h 
    ON h.value_production_pricing_statement_details_application_number = fps."PROD_VAL_STA_ID"
JOIN financial_mgmt.t_pricing_statement_details_header tp
    ON tp.value_of_production_and_pricing_statement_details_id = h.value_of_production_and_pricing_statement_details_id
JOIN dgh_staging.frm_workitem_master_new fwmn 
    ON fwmn.ref_id = fps."PROD_VAL_STA_ID"
WHERE fps."ACTIVE" = '17'
  AND fps."TABLE_CLASS" = 'FORM6B'
  AND fps."LABEL_NO" = '65'

union all

SELECT 
    tp.pricing_statement_details_header_id                          AS pricing_statement_details_header_id,
    0                                                              AS is_sales,
    1                                                              AS is_appropriation,
    fps."LABEL_VALUE"                                               AS name,
    fps."GAS_SAL_ASS_M3"::NUMERIC                                            AS associated_m3,
    fps."GAS_SAL_ASS_MMBTU" ::NUMERIC                                        AS associated_mmbtu,
    fps."GAS_SAL_ASS_PRI_MMBTU" ::NUMERIC                                    AS associated_price,
    fps."GAS_SAL_ASS_REF_TO_PRI" ::NUMERIC                                   AS associated_ref_price,
    fps."GAS_SAL_ASS_VALUE"::NUMERIC                                         AS associated_value,
    fps."GAS_SAL_NON_ASS_M3" ::NUMERIC                                       AS non_associated_m3,
    fps."GAS_SAL_NON_ASS_MMBTU"  ::NUMERIC                                   AS non_associated_mmbtu,
    fps."GAS_SAL_NON_ASS_PRI_MMBTU" ::NUMERIC                                AS non_associated_price,
    fps."GAS_SAL_NON_ASS_REF_TO_PRI"::NUMERIC                                AS non_associated_ref_price,
    fps."GAS_SAL_NON_ASS_VALUE"::NUMERIC                                     AS non_associated_value,
    1                                                              AS is_active,
    1                                                              AS is_migrated
FROM dgh_staging.form_prod_val_sta_data_5c fps
JOIN financial_mgmt.t_value_of_production_and_pricing_statement_details_header h 
    ON h.value_production_pricing_statement_details_application_number = fps."PROD_VAL_STA_ID"
JOIN financial_mgmt.t_pricing_statement_details_header tp
    ON tp.value_of_production_and_pricing_statement_details_id = h.value_of_production_and_pricing_statement_details_id
JOIN dgh_staging.frm_workitem_master_new fwmn 
    ON fwmn.ref_id = fps."PROD_VAL_STA_ID"
WHERE fps."ACTIVE" = '17'
  AND fps."TABLE_CLASS" = 'FORM6B'
  AND fps."LABEL_NO" = '67';
