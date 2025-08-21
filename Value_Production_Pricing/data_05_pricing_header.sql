INSERT INTO financial_mgmt.t_pricing_statement_details_header (
    value_of_production_and_pricing_statement_details_id,
    block_name,
    basin,
    contractor_name,
    dos_contract,
    state,
    lessee_or_licensee,
    is_active,
    is_migrated
)
SELECT 
    th.value_of_production_and_pricing_statement_details_id,
    src."BLOCKNAME" AS block_name,
    src."BASIN" AS basin,
    src."CONTRACTNAME" AS contractor_name,
    src."DOS_CONTRACT" AS dos_contract,
    src."STATE" AS state,
    src."LICENSEE" AS lessee_or_licensee,
    1 AS is_active,
    1 AS is_migrated
FROM dgh_staging.form_production_val_statement src
JOIN dgh_staging.frm_workitem_master_new fwmn 
    ON fwmn.ref_id = src."REFID"
JOIN financial_mgmt.t_value_of_production_and_pricing_statement_details_header th
    ON th.value_production_pricing_statement_details_application_number = src."REFID"
LEFT JOIN user_profile.m_user_master mum
    ON mum.migrated_user_id = src."CREATED_BY" 
   AND mum.is_migrated = 1
WHERE src."STATUS" = '1' and src."REFID" = 'QPR-20240604120602';
