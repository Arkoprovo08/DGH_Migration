INSERT INTO upstream_data_management.t_measurement_of_petroleum_header (
    measurement_of_petroleum_application_number,
    contractor_name,
    block_name,
    date_of_contract_signature,
    block_category,
    reference_to_article_policy,
    ocr_available,
    justification_for_ocr_unavailability,
    block_overview,
    hydrocarbon_type,
    internal_consumption_meter_applicable,
    internal_consumption_number_of_meters,
    flare_meter_applicable,
    custody_transfer_meter_applicable,
    custody_transfer_meter_number_of_meters,
    metering_through_tanks_applicable,
    metering_number_of_tanks,
    no_of_buyers,
    name_of_authorised_signatory,
    designation,
    remarks,
    form_checkbox,
    declaration_checkbox,
    is_active,
    created_by,
    creation_date,
    process_id,
    current_status,
    is_migrated
)
SELECT
    fmp.refid AS measurement_of_petroleum_application_number,
    MAX(CASE WHEN label_text = 'Name of Contractor' THEN label_value END) AS contractor_name,
    MAX(CASE WHEN label_text = 'Name of Block' THEN label_value END) AS block_name,
    MAX(CASE WHEN label_text = 'Date of signing of contract (DD/MM/YYYY)' 
             THEN TO_DATE(label_value, 'DD/MM/YYYY') END) AS date_of_contract_signature,
    MAX(CASE WHEN label_text = 'Category of Block (Pre-Nelp/Nelp)' THEN label_value END) AS block_category,
    MAX(CASE WHEN label_text = 'Reference to PSC Article No.' THEN label_value END) AS reference_to_article_policy,
    MAX(CASE 
        WHEN label_text = 'OCR Available' THEN 
            CASE WHEN UPPER(label_value) = '1' THEN 1 ELSE 0 END
        ELSE 0 
    END) AS ocr_available,
    MAX(CASE WHEN label_text = 'Please provide justification for unavailability of OCR' THEN label_value END) AS justification_for_ocr_unavailability,
    MAX(CASE WHEN label_text = 'Overview of Block' THEN label_value END) AS block_overview,
    MAX(CASE 
        WHEN label_text = 'Hydrocarbon type in the Block' THEN
            CASE 
                WHEN label_value = '0' THEN 'NA'
                WHEN label_value = '1' THEN 'Oil'
                WHEN label_value = '2' THEN 'Gas'
                WHEN label_value = '3' THEN 'Condensate'
                WHEN label_value = '4' THEN 'Oil-Gas'
                ELSE NULL
            END
        ELSE NULL
    END) AS hydrocarbon_type,
    MAX(CASE 
        WHEN label_text = 'Internal consumption meter' THEN 
            CASE WHEN label_value = '1' THEN 1 ELSE 0 END 
        ELSE 0 
    END) AS internal_consumption_meter_applicable,
    MAX(CASE WHEN label_text = 'Number of meters' AND label_no = '10.1' THEN label_value END)::INT AS internal_consumption_number_of_meters,
    MAX(CASE 
        WHEN label_text = 'Flare Meter' THEN 
            CASE WHEN label_value = '1' THEN 1 ELSE 0 END 
        ELSE 0 
    END) AS flare_meter_applicable,
    MAX(CASE 
        WHEN label_text = 'Custody Transfer Meter' THEN 
            CASE WHEN label_value = '1' THEN 1 ELSE 0 END 
        ELSE 0 
    END) AS custody_transfer_meter_applicable,
    MAX(CASE WHEN label_text = 'Number of meters' AND label_no = '12.1' THEN label_value END)::INT AS custody_transfer_meter_number_of_meters,
    MAX(CASE 
        WHEN label_text = 'Metering through Tanks' THEN 
            CASE WHEN label_value = '1' THEN 1 ELSE 0 END 
        ELSE 0 
    END) AS metering_through_tanks_applicable,
    MAX(CASE WHEN label_text = 'Number of Tanks' THEN label_value END)::INT AS metering_number_of_tanks,
    MAX(CASE WHEN label_text = 'No of Buyers' THEN label_value END)::INT AS no_of_buyers,
    MAX(CASE WHEN label_text = 'Name of Authorised Signatory for Contractor [duly authorised by OCR]' THEN label_value END) AS name_of_authorised_signatory,
    MAX(CASE WHEN label_text = 'DESIGNATION' THEN label_value END) AS designation,
    MAX(CASE WHEN label_no = '102' THEN label_value END) AS remarks,
    '{}'::JSONB AS form_checkbox,
    '{"acceptTerm1": true, "acceptTerm2": true, "acceptTerm3": true, "acceptTerm4": true, "acceptTerm5": true, "acceptTerm6": true}'::JSONB AS declaration_checkbox,
    1 AS is_active,
    mum.user_id AS created_by,
    MAX(fmp.created_on) AS creation_date,
    25 AS process_id,
    'DRAFT' AS current_status,
    1
FROM dgh_staging.form_measurement_petroleum fmp
LEFT JOIN user_profile.m_user_master mum ON mum.migrated_user_id = fmp.created_by
join dgh_staging.frm_workitem_master_new w on w.ref_id = fmp.refid
WHERE fmp.status = '1' AND mum.is_migrated = 1
GROUP BY fmp.refid, mum.user_id;
