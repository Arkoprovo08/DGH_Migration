INSERT INTO financial_mgmt.t_production_statement_field_header_details (
    production_statement_details_id,
    field_name,
    no_of_days_operated,
    created_by,
    creation_date,
    is_active
)
SELECT
    tpsd.production_statement_details_id,
    MAX(CASE WHEN fpd."LABEL_NO" = '6' THEN fpd."LABEL_VALUE" END) AS field_name,
    MAX(CASE WHEN fpd."LABEL_NO" = '9' THEN fpd."LABEL_VALUE"::INTEGER END) AS no_of_days_operated,
    tpsd.created_by,
    tpsd.creation_date,
    1 AS is_active                         
FROM
    dgh_staging.form_production_val_statement fps
JOIN
    financial_mgmt.t_value_of_production_and_pricing_statement_details_header vp
    ON fps."REFID" = vp.value_production_pricing_statement_details_application_number
join financial_mgmt.t_production_statement_details tpsd 
	on vp.value_of_production_and_pricing_statement_details_id = tpsd.quarterly_report_value_of_production_and_pricing_statement_deta
LEFT JOIN
    dgh_staging.form_prod_val_sta_data fpd
    ON fps."REFID" = fpd."PROD_VAL_STA_ID"
where fpd."ACTIVE" = '17'
GROUP BY
    tpsd.production_statement_details_id;
