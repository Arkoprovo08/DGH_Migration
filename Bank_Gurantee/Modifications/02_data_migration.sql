INSERT INTO financial_mgmt.t_bank_gurantee_details (
    contractor_name,
    bg_no,
    date_of_bg,
    date_of_exp_bg,
    cliam_of_exp_date_of_bg,
    applicable_perc_bg,
    amt_of_bg,
    usd_into_inr,
    exchange_rate_taken,
    exchange_rate_date,
    name_of_bank,
    scheduled_under_rbi,
    address_of_bank,   
    current_status,
    is_bg_format_psc,
    bg_revised_extended_chk,
    remarks,
    usd_inr,
    bg_submitted_by,
    consortium,
    is_migrated,
    bank_gurantee_header_id,
    creation_date,
    created_by,
    is_active
)
SELECT 
    fsbg."CONTRACTNAME"              AS contractor_name,
    fsbg."BG_NO"                     AS bg_no,
    fsbg."DATE_OF_BG"                AS date_of_bg,
    fsbg."DATE_OF_EXP_BG"           AS date_of_exp_bg,
    fsbg."CLIAM_OF_EXP_DATE_OF_BG"  AS cliam_of_exp_date_of_bg,
    fsbg."APPLICABLE_PERC_BG"       AS applicable_perc_bg,
    fsbg."AMT_OF_BG"                 AS amt_of_bg,
    fsbg."USD_INTO_INR"             AS usd_into_inr,
    fsbg."EXCHANGE_RATE_TAKEN"      AS exchange_rate_taken,
    fsbg."EXCHANGE_RATE_DATE"       AS exchange_rate_date,
    fsbg."NAME_OF_BANK"             AS name_of_bank,
    case
		WHEN fsbg."SCHEDULED_UNDER_RBI"='1' then 1
		else 0
	end AS scheduled_under_rbi,
    fsbg."ADDRESS_OF_BANK"          AS address_of_bank,   
    'DRAFT'                          AS current_status,
    fsbg."IS_BG_FORMAT_PSC"         AS is_bg_format_psc,
    fsbg."BG_REVISED_EXTENDED_CHK"  AS bg_revised_extended_chk,
    fsbg."REMARKS"                  AS remarks,
    case
		WHEN fsbg."USD_INR"='1' then 'USD'
		else 'INR'
	end AS usd_inr,
    fsbg."BG_SUBMITTED_BY"          AS bg_submitted_by,
    fsbg."CONSORTIUM"               AS consortium,
    2                                AS is_migrated,
    hdr."bank_gurantee_header_id"   AS bank_gurantee_header_id,
    fsbg."CREATED_ON"               AS creation_date,
    um."user_id"                    AS created_by,
    1                                AS is_active
FROM dgh_staging."form_sub_bg_legal_renewal" fsbg
JOIN financial_mgmt."t_bank_gurantee_header" hdr
    ON hdr."bank_gurantee_application_number" = fsbg."REFID"
LEFT JOIN user_profile."m_user_master" um
    ON fsbg."CREATED_BY" = um."migrated_user_id"
WHERE fsbg."STATUS" = '1';