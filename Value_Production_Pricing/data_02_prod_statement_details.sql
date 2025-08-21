INSERT INTO financial_mgmt.t_production_statement_details (
    quarterly_report_value_of_production_and_pricing_statement_deta,
    contractor_name,
    block_name,
    date_of_signing_contract,
    block_category,
    operator_name,
    other_operator_name,
    month,
    fy,
    ref_to_psc_article_no,
    verification_name,
    verification_designation,
    verification_email,
    verification_contact_number,
    creation_date,
    is_active,
    is_migrated
)
SELECT
    vp.value_of_production_and_pricing_statement_details_id           AS quarterly_report_value_of_production_and_pricing_statement_deta,
    fps."CONTRACTNAME"                                                AS contractor_name,
    fps."BLOCKNAME"                                                   AS block_name,
    fps."DOS_CONTRACT"                                                AS date_of_signing_contract,
    fps."BLOCKCATEGORY"                                               AS block_category,
    fps."OPERATOR"                                                    AS operator_name,
    fps."OTHEROPERATOR"                                               AS other_operator_name,
    fps."MONTH"                                                       AS month,
    fps."FY"                                                          AS fy,
    fps."REF_TOPSC_ARTICALNO"                                         AS ref_to_psc_article_no,
    MAX(CASE WHEN fpd."LABEL_NO" = '22' THEN fpd."LABEL_VALUE" END)   AS verification_name,
    MAX(CASE WHEN fpd."LABEL_NO" = '25' THEN fpd."LABEL_VALUE" END)   AS verification_designation,
    MAX(CASE WHEN fpd."LABEL_NO" = '26' THEN fpd."LABEL_VALUE" END)   AS verification_email,
    MAX(CASE WHEN fpd."LABEL_NO" = '27' THEN fpd."LABEL_VALUE" END)   AS verification_contact_number,
    MAX(fpd."CREATED_ON")::date                                             AS creation_date,
    1                                                                 AS is_active,
    1                                                                 AS is_migrated
FROM dgh_staging.form_production_val_statement fps
JOIN financial_mgmt.t_value_of_production_and_pricing_statement_details_header vp
    ON fps."REFID" = vp.value_production_pricing_statement_details_application_number
LEFT JOIN dgh_staging.form_prod_val_sta_data fpd
    ON fps."REFID" = fpd."PROD_VAL_STA_ID"
WHERE fpd."SEQ" = (
    SELECT MAX("SEQ")
    FROM dgh_staging.form_prod_val_sta_data 
    WHERE "ACTIVE" = '17' 
      AND "PROD_VAL_STA_ID" = fps."REFID"
--      AND fps."REFID" = 'QPR-20240604120602'
)
GROUP BY
    vp.value_of_production_and_pricing_statement_details_id,
    fps."CONTRACTNAME",
    fps."BLOCKNAME",
    fps."DOS_CONTRACT",
    fps."BLOCKCATEGORY",
    fps."OPERATOR",
    fps."OTHEROPERATOR",
    fps."MONTH",
    fps."FY",
    fps."REF_TOPSC_ARTICALNO";
