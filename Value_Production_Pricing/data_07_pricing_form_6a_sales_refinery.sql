insert into financial_mgmt.t_pricing_statement_form6a_sales_refinery_details 
(
pricing_statement_details_header_id,
is_customer,
is_refinery,
name,
crude_oil_bbl,
crude_oil_percent,
crude_oil_price,
crude_oil_value,
condensate_bbl,
condensate_percent,
condensate_price,
condensate_value,
is_active,
is_migrated 
)
select 
    tp.pricing_statement_details_header_id,
    1 as is_customer,
    0 as is_refinery,
    fps."LABEL_VALUE" AS name,
    fps."OIL_SAL_CR_BBL"::numeric as crude_oil_bbl,
	fps."OIL_SAL_PER_TOT_SAL"::numeric as crude_oil_percent,
	fps."OIL_SAL_CR_PRI_BBL"::numeric as crude_oil_price,
	fps."OIL_SAL_CR_VALUE"::numeric as crude_oil_value,
	fps."OIL_SAL_COND_BBL"::numeric as condensate_bbl,
	fps."OIL_SAL_COND_PER_TOT_SAL"::numeric as condensate_percent,
	fps."OIL_SAL_COND_PRI_BBL"::numeric as condensate_price,
	fps."OIL_SAL_COND_VALUE"::numeric as condensate_value,
    1 AS is_active,
    1 AS is_migrated
FROM dgh_staging.form_prod_val_sta_data_5c fps
JOIN financial_mgmt.t_value_of_production_and_pricing_statement_details_header h 
    ON h.value_production_pricing_statement_details_application_number = fps."PROD_VAL_STA_ID"
JOIN financial_mgmt.t_pricing_statement_details_header tp
    ON tp.value_of_production_and_pricing_statement_details_id = h.value_of_production_and_pricing_statement_details_id
JOIN dgh_staging.frm_workitem_master_new fwmn 
    ON fwmn.ref_id = fps."PROD_VAL_STA_ID"
WHERE fps."ACTIVE" = '17'
  AND fps."TABLE_CLASS" = 'FORM6A'
  and FPS."LABEL_NO" = '42'

union all

select 
    tp.pricing_statement_details_header_id,
    0 as is_customer,
    1 as is_refinery,
    fps."LABEL_VALUE" AS name,
    fps."OIL_SAL_CR_BBL"::numeric as crude_oil_bbl,
	fps."OIL_SAL_PER_TOT_SAL"::numeric as crude_oil_percent,
	fps."OIL_SAL_CR_PRI_BBL"::numeric as crude_oil_price,
	fps."OIL_SAL_CR_VALUE"::numeric as crude_oil_value,
	fps."OIL_SAL_COND_BBL"::numeric as condensate_bbl,
	fps."OIL_SAL_COND_PER_TOT_SAL"::numeric as condensate_percent,
	fps."OIL_SAL_COND_PRI_BBL"::numeric as condensate_price,
	fps."OIL_SAL_COND_VALUE"::numeric as condensate_value,
    1 AS is_active,
    1 AS is_migrated
FROM dgh_staging.form_prod_val_sta_data_5c fps
JOIN financial_mgmt.t_value_of_production_and_pricing_statement_details_header h 
    ON h.value_production_pricing_statement_details_application_number = fps."PROD_VAL_STA_ID"
JOIN financial_mgmt.t_pricing_statement_details_header tp
    ON tp.value_of_production_and_pricing_statement_details_id = h.value_of_production_and_pricing_statement_details_id
JOIN dgh_staging.frm_workitem_master_new fwmn 
    ON fwmn.ref_id = fps."PROD_VAL_STA_ID"
WHERE fps."ACTIVE" = '17'
  AND fps."TABLE_CLASS" = 'FORM6A'
  and FPS."LABEL_NO" = '50'