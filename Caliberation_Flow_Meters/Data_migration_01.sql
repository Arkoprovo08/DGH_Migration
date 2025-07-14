INSERT INTO upstream_data_management.t_caliberation_flow_meters_witness_custody_details (
    caliberation_flow_meters_witness_custody_details_application_nu,
    block_category,
    block_name,
    contractor_name,
    created_by,
    creation_date,
    current_status,
    date_contract_signature,
    designation,
    is_active,
    declaration_checkbox ,
    name_of_authorised_signatory,
    operator_location,
    ref_to_psc_article_no,
    remarks,
    testing_from,
    testing_to
)
SELECT
    fcf.refid,
    fcf.blockcategory,
    fcf.blockname,
    fcf.contractname,
    mum.user_id,
    fcf.created_on,
    'DRAFT' AS current_status,
    fcf.dos_contract,
    fcf.designation,
    1 AS is_active,
    '{}',
    fcf.name_auth_sig_contra,
    fcf.location,
    fcf.ref_topsc_articalno,
    fc.comment_data,
    fcf.sch_date_test_from,
    fcf.sch_date_test_to
FROM dgh_staging.form_calibratio_flow fcf
LEFT JOIN user_profile.m_user_master mum 
    ON mum.migrated_user_id = fcf.created_by AND mum.is_migrated = 1
LEFT JOIN dgh_staging.frm_comments fc 
    ON fc.comment_id = fcf.commentid;
