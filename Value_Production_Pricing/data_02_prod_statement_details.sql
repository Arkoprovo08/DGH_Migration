INSERT INTO financial_mgmt.t_production_statement_details (
	production_statement_details_id,
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
    created_by,
    creation_date,
    is_active,
    is_migrated
)
SELECT
    vp.value_of_production_and_pricing_statement_details_id,
    fps."CONTRACTNAME",
    fps."BLOCKNAME",
    fps."DOS_CONTRACT",
    fps."BLOCKCATEGORY",
    fps."OPERATOR",
    fps."OTHEROPERATOR",
    fps."MONTH",
    fps."FY",
    fps."REF_TOPSC_ARTICALNO",
    MAX(CASE WHEN fpd."LABEL_NO" = '22' THEN fpd."LABEL_VALUE" END) as verification_name, -- verification_name
    MAX(CASE WHEN fpd."LABEL_NO" = '25' THEN fpd."LABEL_VALUE" END) as verification_designation, -- verification_designation
    MAX(CASE WHEN fpd."LABEL_NO" = '26' THEN fpd."LABEL_VALUE" END) as verification_email, -- verification_email
    MAX(CASE WHEN fpd."LABEL_NO" = '27' THEN fpd."LABEL_VALUE" END) as verification_contact_number, -- verification_contact_number
    MAX(fpd."CREATED_BY"),
    MAX(fpd."CREATED_ON"),
    1,
    1
FROM
    dgh_staging.form_production_val_statement fps
JOIN
    financial_mgmt.t_value_of_production_and_pricing_statement_details_header vp
    ON fps."REFID"  = vp.value_production_pricing_statement_details_application_number   
LEFT JOIN
    dgh_staging.form_prod_val_sta_data fpd
    ON fps."REFID" = fpd."PROD_VAL_STA_ID" 
where FPD."SEQ" = (
select MAX("SEQ")
from dgh_staging.form_prod_val_sta_data 
where "ACTIVE" = '17' and "PROD_VAL_STA_ID" = fps."REFID" 
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
