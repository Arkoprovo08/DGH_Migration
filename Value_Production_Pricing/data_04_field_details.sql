    INSERT INTO financial_mgmt.t_production_statement_field_details (
    production_statement_field_header_details_id,
    sl_no,
    activity_id,
    oil_mt,
    oil_bbl,
    cond_mt,
    cond_bbl,
    a_gas_m3,
    na_gas_m3,
    water_mt,
    remarks,
    created_by,
    creation_date,
    is_active,
    data_source,
    is_migrated
)
SELECT
    psfhd.production_statement_field_header_details_id,
    CASE
        WHEN f."LABEL_TEXT" = 'PRODUCTION' THEN 1
        WHEN f."LABEL_TEXT" = 'CONSUMPTION' THEN 2
        WHEN f."LABEL_TEXT" = 'SAVED' THEN 3
        WHEN f."LABEL_TEXT" = 'SALE DISPATCH' THEN 4
        WHEN f."LABEL_TEXT" = 'RE INJECTION' THEN 5
        WHEN f."LABEL_TEXT" = 'FLARE' THEN 6
        WHEN f."LABEL_TEXT" = 'NORMAL LOSS' THEN 7
        WHEN f."LABEL_TEXT" = 'ABNORMAL LOSS' THEN 8
        WHEN f."LABEL_TEXT" = 'OPENING STOCK' THEN 9
        WHEN f."LABEL_TEXT" = 'CLOSING STOCK' THEN 10
    END AS sl_no,
    CASE
        WHEN f."LABEL_TEXT" = 'PRODUCTION' THEN 1
        WHEN f."LABEL_TEXT" = 'CONSUMPTION' THEN 2
        WHEN f."LABEL_TEXT" = 'SAVED' THEN 3
        WHEN f."LABEL_TEXT" = 'SALE DISPATCH' THEN 4
        WHEN f."LABEL_TEXT" = 'RE INJECTION' THEN 5
        WHEN f."LABEL_TEXT" = 'FLARE' THEN 6
        WHEN f."LABEL_TEXT" = 'NORMAL LOSS' THEN 7
        WHEN f."LABEL_TEXT" = 'ABNORMAL LOSS' THEN 8
        WHEN f."LABEL_TEXT" = 'OPENING STOCK' THEN 9
        WHEN f."LABEL_TEXT" = 'CLOSING STOCK' THEN 10
    END AS activity_id,
    NULLIF(f."OIL_MT", '')::numeric,
    NULLIF(f."OIL_BBL", '')::numeric,
    NULLIF(f."COND_MT", '')::numeric,
    NULLIF(f."COND_BBL", '')::numeric,
    NULLIF(f."A_GAS_M3", '')::numeric,
    NULLIF(f."NA_GAS_M3", '')::numeric,
    NULLIF(f."WATER_MT", '')::numeric,
    f."REMARKS",
    vp.created_by,
    vp.creation_date,
    1 AS is_active,
    'MIGRATED_DATA' AS data_source,
    1 as is_migrated
FROM dgh_staging.form_prod_val_sta_data_frmate1 f
JOIN dgh_staging.form_production_val_statement fps
    ON f."PROD_VAL_STA_ID" = fps."REFID"
JOIN financial_mgmt.t_value_of_production_and_pricing_statement_details_header vp
    ON fps."REFID" = vp.value_production_pricing_statement_details_application_number
join financial_mgmt.t_production_statement_details tpsfd 
	on tpsfd.quarterly_report_value_of_production_and_pricing_statement_deta = vp.value_of_production_and_pricing_statement_details_id
JOIN financial_mgmt.t_production_statement_field_header_details psfhd
    ON psfhd.production_statement_details_id = tpsfd.production_statement_details_id
--JOIN financial_mgmt.t_production_statement_field_details p
--    ON p.production_statement_field_header_details_id = psfhd.production_statement_field_header_details_id
WHERE f."ACTIVE" = '17'
  AND f."LABEL_TEXT" IN (
        'PRODUCTION', 'CONSUMPTION', 'SAVED', 'SALE DISPATCH', 'RE INJECTION',
        'FLARE', 'NORMAL LOSS', 'ABNORMAL LOSS', 'OPENING STOCK', 'CLOSING STOCK'
  );