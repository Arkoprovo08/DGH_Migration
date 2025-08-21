INSERT INTO financial_mgmt.t_pricing_statement_form5c_field_details
(
    pricing_statement_details_header_id,
    field_name,
    particulars_id,
    hydrocarbon_type,
    unit_of_measurement,
    value,
    is_active,
    creation_date,
    is_migrated
)
select 
    tp.pricing_statement_details_header_id,
    fn."LABEL_VALUE" AS field_name,
    CASE fps."LABEL_TEXT"
        WHEN 'Crude and Natural gas' THEN 1        
    END AS particulars_id,
    25 AS hydrocarbon_type,
    28  AS unit_of_measurement,
    NULLIF(fps."MTHLY_ROY_OIL_USD",'')::numeric AS value,  
    1 AS is_active,
    tp.creation_date AS creation_date,
    1 AS is_migrated
FROM dgh_staging.form_prod_val_sta_data_5c fps
JOIN financial_mgmt.t_value_of_production_and_pricing_statement_details_header h 
    ON h.value_production_pricing_statement_details_application_number = fps."PROD_VAL_STA_ID"
JOIN financial_mgmt.t_pricing_statement_details_header tp
    ON tp.value_of_production_and_pricing_statement_details_id = h.value_of_production_and_pricing_statement_details_id
JOIN dgh_staging.frm_workitem_master_new fwmn 
    ON fwmn.ref_id = fps."PROD_VAL_STA_ID"
LEFT JOIN dgh_staging.form_prod_val_sta_data_5c fn
    ON fn."PROD_VAL_STA_ID" = fps."PROD_VAL_STA_ID"
   AND fn."LABEL_TEXT" = 'Field Name'
   AND fn."ACTIVE" = '17'
WHERE fps."ACTIVE" = '17'
  AND fps."TABLE_CLASS" = 'FORM5C'
  AND fps."LABEL_TEXT" IN ('Crude and Natural gas')
  and FPS."LABEL_NO" = '28'

union all

select 
    tp.pricing_statement_details_header_id,
    fn."LABEL_VALUE" AS field_name,
    CASE fps."LABEL_TEXT"
        WHEN 'Royalty' THEN 2
        WHEN 'CESS' THEN 3
        WHEN 'NCCD' THEN 4
        WHEN 'Ed & H.Ed Cess on cess' THEN 5
    END AS particulars_id,
    25 AS hydrocarbon_type,
    31  AS unit_of_measurement,
    NULLIF(fps."MTHLY_ROY_OIL_USD",'')::numeric AS value,  
    1 AS is_active,
    tp.creation_date AS creation_date,
    1 AS is_migrated
FROM dgh_staging.form_prod_val_sta_data_5c fps
JOIN financial_mgmt.t_value_of_production_and_pricing_statement_details_header h 
    ON h.value_production_pricing_statement_details_application_number = fps."PROD_VAL_STA_ID"
JOIN financial_mgmt.t_pricing_statement_details_header tp
    ON tp.value_of_production_and_pricing_statement_details_id = h.value_of_production_and_pricing_statement_details_id
JOIN dgh_staging.frm_workitem_master_new fwmn 
    ON fwmn.ref_id = fps."PROD_VAL_STA_ID"
LEFT JOIN dgh_staging.form_prod_val_sta_data_5c fn
    ON fn."PROD_VAL_STA_ID" = fps."PROD_VAL_STA_ID"
   AND fn."LABEL_TEXT" = 'Field Name'
   AND fn."ACTIVE" = '17'
WHERE fps."ACTIVE" = '17'
  AND fps."TABLE_CLASS" = 'FORM5C'
  AND fps."LABEL_TEXT" IN ('Royalty','CESS','NCCD','Ed & H.Ed Cess on cess')
  and FPS."LABEL_NO" = '28'

union all

select 
    tp.pricing_statement_details_header_id,
    fn."LABEL_VALUE" AS field_name,
    CASE fps."LABEL_TEXT"
        WHEN 'Crude and Natural gas' THEN 1
    END AS particulars_id,
    25 AS hydrocarbon_type,
    29  AS unit_of_measurement,
    NULLIF(fps."MTHLY_ROY_OIL_INR",'')::numeric AS value,  
    1 AS is_active,
    tp.creation_date AS creation_date,
    1 AS is_migrated
FROM dgh_staging.form_prod_val_sta_data_5c fps
JOIN financial_mgmt.t_value_of_production_and_pricing_statement_details_header h 
    ON h.value_production_pricing_statement_details_application_number = fps."PROD_VAL_STA_ID"
JOIN financial_mgmt.t_pricing_statement_details_header tp
    ON tp.value_of_production_and_pricing_statement_details_id = h.value_of_production_and_pricing_statement_details_id
JOIN dgh_staging.frm_workitem_master_new fwmn 
    ON fwmn.ref_id = fps."PROD_VAL_STA_ID"
LEFT JOIN dgh_staging.form_prod_val_sta_data_5c fn
    ON fn."PROD_VAL_STA_ID" = fps."PROD_VAL_STA_ID"
   AND fn."LABEL_TEXT" = 'Field Name'
   AND fn."ACTIVE" = '17'
WHERE fps."ACTIVE" = '17'
  AND fps."TABLE_CLASS" = 'FORM5C'
  AND fps."LABEL_TEXT" IN ('Crude and Natural gas')
  and FPS."LABEL_NO" = '28'

union all

select 
    tp.pricing_statement_details_header_id,
    fn."LABEL_VALUE" AS field_name,
    CASE fps."LABEL_TEXT"
        WHEN 'Royalty' THEN 2
        WHEN 'CESS' THEN 3
        WHEN 'NCCD' THEN 4
        WHEN 'Ed & H.Ed Cess on cess' THEN 5
    END AS particulars_id,
    25 AS hydrocarbon_type,
    32  AS unit_of_measurement,
    NULLIF(fps."MTHLY_ROY_OIL_INR",'')::numeric AS value,  
    1 AS is_active,
    tp.creation_date AS creation_date,
    1 AS is_migrated
FROM dgh_staging.form_prod_val_sta_data_5c fps
JOIN financial_mgmt.t_value_of_production_and_pricing_statement_details_header h 
    ON h.value_production_pricing_statement_details_application_number = fps."PROD_VAL_STA_ID"
JOIN financial_mgmt.t_pricing_statement_details_header tp
    ON tp.value_of_production_and_pricing_statement_details_id = h.value_of_production_and_pricing_statement_details_id
JOIN dgh_staging.frm_workitem_master_new fwmn 
    ON fwmn.ref_id = fps."PROD_VAL_STA_ID"
LEFT JOIN dgh_staging.form_prod_val_sta_data_5c fn
    ON fn."PROD_VAL_STA_ID" = fps."PROD_VAL_STA_ID"
   AND fn."LABEL_TEXT" = 'Field Name'
   AND fn."ACTIVE" = '17'
WHERE fps."ACTIVE" = '17'
  AND fps."TABLE_CLASS" = 'FORM5C'
  AND fps."LABEL_TEXT" IN ('Royalty','CESS','NCCD','Ed & H.Ed Cess on cess')
  and FPS."LABEL_NO" = '28'

union all

select 
    tp.pricing_statement_details_header_id,
    fn."LABEL_VALUE" AS field_name,
    CASE fps."LABEL_TEXT"
        WHEN 'Crude and Natural gas' THEN 1
    END AS particulars_id,
    26 AS hydrocarbon_type,
    28  AS unit_of_measurement,
    NULLIF(fps."MTHLY_ROY_GAS_USD",'')::numeric AS value,  
    1 AS is_active,
    tp.creation_date AS creation_date,
    1 AS is_migrated
FROM dgh_staging.form_prod_val_sta_data_5c fps
JOIN financial_mgmt.t_value_of_production_and_pricing_statement_details_header h 
    ON h.value_production_pricing_statement_details_application_number = fps."PROD_VAL_STA_ID"
JOIN financial_mgmt.t_pricing_statement_details_header tp
    ON tp.value_of_production_and_pricing_statement_details_id = h.value_of_production_and_pricing_statement_details_id
JOIN dgh_staging.frm_workitem_master_new fwmn 
    ON fwmn.ref_id = fps."PROD_VAL_STA_ID"
LEFT JOIN dgh_staging.form_prod_val_sta_data_5c fn
    ON fn."PROD_VAL_STA_ID" = fps."PROD_VAL_STA_ID"
   AND fn."LABEL_TEXT" = 'Field Name'
   AND fn."ACTIVE" = '17'
WHERE fps."ACTIVE" = '17'
  AND fps."TABLE_CLASS" = 'FORM5C'
  AND fps."LABEL_TEXT" IN ('Crude and Natural gas')
  and FPS."LABEL_NO" = '28'

union all

select 
    tp.pricing_statement_details_header_id,
    fn."LABEL_VALUE" AS field_name,
    CASE fps."LABEL_TEXT"
        WHEN 'Royalty' THEN 2
        WHEN 'CESS' THEN 3
        WHEN 'NCCD' THEN 4
        WHEN 'Ed & H.Ed Cess on cess' THEN 5
    END AS particulars_id,
    26 AS hydrocarbon_type,
    31  AS unit_of_measurement,
    NULLIF(fps."MTHLY_ROY_GAS_USD",'')::numeric AS value,  
    1 AS is_active,
    tp.creation_date AS creation_date,
    1 AS is_migrated
FROM dgh_staging.form_prod_val_sta_data_5c fps
JOIN financial_mgmt.t_value_of_production_and_pricing_statement_details_header h 
    ON h.value_production_pricing_statement_details_application_number = fps."PROD_VAL_STA_ID"
JOIN financial_mgmt.t_pricing_statement_details_header tp
    ON tp.value_of_production_and_pricing_statement_details_id = h.value_of_production_and_pricing_statement_details_id
JOIN dgh_staging.frm_workitem_master_new fwmn 
    ON fwmn.ref_id = fps."PROD_VAL_STA_ID"
LEFT JOIN dgh_staging.form_prod_val_sta_data_5c fn
    ON fn."PROD_VAL_STA_ID" = fps."PROD_VAL_STA_ID"
   AND fn."LABEL_TEXT" = 'Field Name'
   AND fn."ACTIVE" = '17'
WHERE fps."ACTIVE" = '17'
  AND fps."TABLE_CLASS" = 'FORM5C'
  AND fps."LABEL_TEXT" IN ('Royalty','CESS','NCCD','Ed & H.Ed Cess on cess')
  and FPS."LABEL_NO" = '28'

union all

select 
    tp.pricing_statement_details_header_id,
    fn."LABEL_VALUE" AS field_name,
    CASE fps."LABEL_TEXT"
        WHEN 'Crude and Natural gas' THEN 1
    END AS particulars_id,
    26 AS hydrocarbon_type,
    29  AS unit_of_measurement,
    NULLIF(fps."MTHLY_ROY_GAS_INR",'')::numeric AS value,  
    1 AS is_active,
    tp.creation_date AS creation_date,
    1 AS is_migrated
FROM dgh_staging.form_prod_val_sta_data_5c fps
JOIN financial_mgmt.t_value_of_production_and_pricing_statement_details_header h 
    ON h.value_production_pricing_statement_details_application_number = fps."PROD_VAL_STA_ID"
JOIN financial_mgmt.t_pricing_statement_details_header tp
    ON tp.value_of_production_and_pricing_statement_details_id = h.value_of_production_and_pricing_statement_details_id
JOIN dgh_staging.frm_workitem_master_new fwmn 
    ON fwmn.ref_id = fps."PROD_VAL_STA_ID"
LEFT JOIN dgh_staging.form_prod_val_sta_data_5c fn
    ON fn."PROD_VAL_STA_ID" = fps."PROD_VAL_STA_ID"
   AND fn."LABEL_TEXT" = 'Field Name'
   AND fn."ACTIVE" = '17'
WHERE fps."ACTIVE" = '17'
  AND fps."TABLE_CLASS" = 'FORM5C'
  AND fps."LABEL_TEXT" IN ('Crude and Natural gas')
  and FPS."LABEL_NO" = '28'

union all

select 
    tp.pricing_statement_details_header_id,
    fn."LABEL_VALUE" AS field_name,
    CASE fps."LABEL_TEXT"
        WHEN 'Royalty' THEN 2
        WHEN 'CESS' THEN 3
        WHEN 'NCCD' THEN 4
        WHEN 'Ed & H.Ed Cess on cess' THEN 5
    END AS particulars_id,
    26 AS hydrocarbon_type,
    32  AS unit_of_measurement,
    NULLIF(fps."MTHLY_ROY_GAS_INR",'')::numeric AS value,  
    1 AS is_active,
    tp.creation_date AS creation_date,
    1 AS is_migrated
FROM dgh_staging.form_prod_val_sta_data_5c fps
JOIN financial_mgmt.t_value_of_production_and_pricing_statement_details_header h 
    ON h.value_production_pricing_statement_details_application_number = fps."PROD_VAL_STA_ID"
JOIN financial_mgmt.t_pricing_statement_details_header tp
    ON tp.value_of_production_and_pricing_statement_details_id = h.value_of_production_and_pricing_statement_details_id
JOIN dgh_staging.frm_workitem_master_new fwmn 
    ON fwmn.ref_id = fps."PROD_VAL_STA_ID"
LEFT JOIN dgh_staging.form_prod_val_sta_data_5c fn
    ON fn."PROD_VAL_STA_ID" = fps."PROD_VAL_STA_ID"
   AND fn."LABEL_TEXT" = 'Field Name'
   AND fn."ACTIVE" = '17'
WHERE fps."ACTIVE" = '17'
  AND fps."TABLE_CLASS" = 'FORM5C'
  AND fps."LABEL_TEXT" IN ('Royalty','CESS','NCCD','Ed & H.Ed Cess on cess')
  and FPS."LABEL_NO" = '28'

union all

select 
    tp.pricing_statement_details_header_id,
    fn."LABEL_VALUE" AS field_name,
    CASE fps."LABEL_TEXT"
        WHEN 'Crude and Natural gas' THEN 1
    END AS particulars_id,
    27 AS hydrocarbon_type,
    30  AS unit_of_measurement,
    NULLIF(fps."MTHLY_ROY_GAS_USD",'')::numeric AS value,  
    1 AS is_active,
    tp.creation_date AS creation_date,
    1 AS is_migrated
FROM dgh_staging.form_prod_val_sta_data_5c fps
JOIN financial_mgmt.t_value_of_production_and_pricing_statement_details_header h 
    ON h.value_production_pricing_statement_details_application_number = fps."PROD_VAL_STA_ID"
JOIN financial_mgmt.t_pricing_statement_details_header tp
    ON tp.value_of_production_and_pricing_statement_details_id = h.value_of_production_and_pricing_statement_details_id
JOIN dgh_staging.frm_workitem_master_new fwmn 
    ON fwmn.ref_id = fps."PROD_VAL_STA_ID"
LEFT JOIN dgh_staging.form_prod_val_sta_data_5c fn
    ON fn."PROD_VAL_STA_ID" = fps."PROD_VAL_STA_ID"
   AND fn."LABEL_TEXT" = 'Field Name'
   AND fn."ACTIVE" = '17'
WHERE fps."ACTIVE" = '17'
  AND fps."TABLE_CLASS" = 'FORM5C'
  AND fps."LABEL_TEXT" IN ('Crude and Natural gas')
  and FPS."LABEL_NO" = '32'

union all

select 
    tp.pricing_statement_details_header_id,
    fn."LABEL_VALUE" AS field_name,
    CASE fps."LABEL_TEXT"
        WHEN 'Royalty' THEN 2
        WHEN 'CESS' THEN 3
        WHEN 'NCCD' THEN 4
        WHEN 'Ed & H.Ed Cess on cess' THEN 5
    END AS particulars_id,
    27 AS hydrocarbon_type,
    31  AS unit_of_measurement,
    NULLIF(fps."MTHLY_ROY_GAS_USD",'')::numeric AS value,  
    1 AS is_active,
    tp.creation_date AS creation_date,
    1 AS is_migrated
FROM dgh_staging.form_prod_val_sta_data_5c fps
JOIN financial_mgmt.t_value_of_production_and_pricing_statement_details_header h 
    ON h.value_production_pricing_statement_details_application_number = fps."PROD_VAL_STA_ID"
JOIN financial_mgmt.t_pricing_statement_details_header tp
    ON tp.value_of_production_and_pricing_statement_details_id = h.value_of_production_and_pricing_statement_details_id
JOIN dgh_staging.frm_workitem_master_new fwmn 
    ON fwmn.ref_id = fps."PROD_VAL_STA_ID"
LEFT JOIN dgh_staging.form_prod_val_sta_data_5c fn
    ON fn."PROD_VAL_STA_ID" = fps."PROD_VAL_STA_ID"
   AND fn."LABEL_TEXT" = 'Field Name'
   AND fn."ACTIVE" = '17'
WHERE fps."ACTIVE" = '17'
  AND fps."TABLE_CLASS" = 'FORM5C'
  AND fps."LABEL_TEXT" IN ('Royalty','CESS','NCCD','Ed & H.Ed Cess on cess')
  and FPS."LABEL_NO" = '32'

union all

select 
    tp.pricing_statement_details_header_id,
    fn."LABEL_VALUE" AS field_name,
    CASE fps."LABEL_TEXT"
        WHEN 'Crude and Natural gas' THEN 1
    END AS particulars_id,
    27 AS hydrocarbon_type,
    124  AS unit_of_measurement,
    NULLIF(fps."MTHLY_ROY_GAS_INR",'')::numeric AS value,  
    1 AS is_active,
    tp.creation_date AS creation_date,
    1 AS is_migrated
FROM dgh_staging.form_prod_val_sta_data_5c fps
JOIN financial_mgmt.t_value_of_production_and_pricing_statement_details_header h 
    ON h.value_production_pricing_statement_details_application_number = fps."PROD_VAL_STA_ID"
JOIN financial_mgmt.t_pricing_statement_details_header tp
    ON tp.value_of_production_and_pricing_statement_details_id = h.value_of_production_and_pricing_statement_details_id
JOIN dgh_staging.frm_workitem_master_new fwmn 
    ON fwmn.ref_id = fps."PROD_VAL_STA_ID"
LEFT JOIN dgh_staging.form_prod_val_sta_data_5c fn
    ON fn."PROD_VAL_STA_ID" = fps."PROD_VAL_STA_ID"
   AND fn."LABEL_TEXT" = 'Field Name'
   AND fn."ACTIVE" = '17'
WHERE fps."ACTIVE" = '17'
  AND fps."TABLE_CLASS" = 'FORM5C'
  AND fps."LABEL_TEXT" IN ('Crude and Natural gas')
  and FPS."LABEL_NO" = '32'

union all

select 
    tp.pricing_statement_details_header_id,
    fn."LABEL_VALUE" AS field_name,
    CASE fps."LABEL_TEXT"
        WHEN 'Royalty' THEN 2
        WHEN 'CESS' THEN 3
        WHEN 'NCCD' THEN 4
        WHEN 'Ed & H.Ed Cess on cess' THEN 5
    END AS particulars_id,
    27 AS hydrocarbon_type,
    32  AS unit_of_measurement,
    NULLIF(fps."MTHLY_ROY_GAS_INR",'')::numeric AS value,  
    1 AS is_active,
    tp.creation_date AS creation_date,
    1 AS is_migrated
FROM dgh_staging.form_prod_val_sta_data_5c fps
JOIN financial_mgmt.t_value_of_production_and_pricing_statement_details_header h 
    ON h.value_production_pricing_statement_details_application_number = fps."PROD_VAL_STA_ID"
JOIN financial_mgmt.t_pricing_statement_details_header tp
    ON tp.value_of_production_and_pricing_statement_details_id = h.value_of_production_and_pricing_statement_details_id
JOIN dgh_staging.frm_workitem_master_new fwmn 
    ON fwmn.ref_id = fps."PROD_VAL_STA_ID"
LEFT JOIN dgh_staging.form_prod_val_sta_data_5c fn
    ON fn."PROD_VAL_STA_ID" = fps."PROD_VAL_STA_ID"
   AND fn."LABEL_TEXT" = 'Field Name'
   AND fn."ACTIVE" = '17'
WHERE fps."ACTIVE" = '17'
  AND fps."TABLE_CLASS" = 'FORM5C'
  AND fps."LABEL_TEXT" IN ('Royalty','CESS','NCCD','Ed & H.Ed Cess on cess')
  and FPS."LABEL_NO" = '32'
