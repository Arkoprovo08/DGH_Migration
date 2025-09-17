INSERT INTO upstream_data_management.t_quarterly_report_header (
    quarterly_report_application_number,
    contractor_name,
    block_category,
    date_of_contract_signature,
    block_name,
    ref_psc_article_no,
    period_of_reporting_quarter,
    period_of_reporting_year,
    period_of_reporting_currency,
    exchange_rate_considered,
    exchange_rate_date,
    created_by,
    creation_date,
    name_of_authorised_signatory,
    designation,
    remarks,
    is_goipp,
    process_id,
    is_active,
    is_migrated,
    current_status,
    declaration_checkbox
)
SELECT 
    a.refid,
    a.contractname,
    a.blockcategory,
    a.dos_contract,
    a.blockname,
    a.ref_topsc_articalno,
    a.period_of_reporting,
    a.financial_years,
    a.currency,
    a.exchange_rate,
    a.date_exchange_rate::timestamp,
    u.user_id,
    a.created_on,
    a.name_auth_sig_contra,
    a.designation,
    b.comment_data,
    a.revenue_goipp_na,
    1,              -- process_id (static)
    1,              -- is_active
    1,              -- is_migrated
    'DRAFT',        -- current_status
    '{"acceptTerm1": true}'::jsonb
FROM dgh_staging.form_progress_report a
JOIN (
    SELECT DISTINCT ON (comment_id) comment_id, comment_data
    FROM dgh_staging.frm_comments
    WHERE is_active = '1'
    ORDER BY comment_id, id
) b ON a.commentid = b.comment_id
JOIN dgh_staging.frm_workitem_master_new fwmn 
    ON fwmn.ref_id = a.refid
JOIN user_profile.m_user_master u 
    ON u.is_migrated = 1 AND u.migrated_user_id = a.created_by
WHERE a.status = 1;
--   AND a.refid = 'QPR-20221027041057';
