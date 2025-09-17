INSERT INTO upstream_data_management.t_quarterly_report_programme_quantity (
    quarterly_report_id,
    programme_quantity_name,
    mc_approved_programme_quantity_fy,
    planned_production,
    actual_production,
    reason_for_variance,
    cumulative_actual_production_eoq,
    percentage_achieved,
    remarks,
    created_by,
    is_active,
    is_migrated,
    unit_type
)
SELECT 
    h.quarterly_report_id,
    a.label AS programme_quantity_name,
    a.work_prog_qut_exp AS mc_approved_programme_quantity_fy,
    a.planned_qut_exp AS planned_production,
    a.actual_qut_exp AS actual_production,
    a.re_for_var_qut_exp AS reason_for_variance,
    a.cumulative_qut_exp AS cumulative_actual_production_eoq,
    a.per_ach_app_bug_qut_exp AS percentage_achieved,
    a.remark_qut_act AS remarks,
    u.user_id AS created_by,
    1 AS is_active,
    1 AS is_migrated,
    a.cumulative_qut_act AS unit_type
FROM dgh_staging.form_progress_report_sec a
JOIN dgh_staging.frm_workitem_master_new fwmn 
    ON fwmn.ref_id = a.refid
JOIN upstream_data_management.t_quarterly_report_header h 
    ON h.quarterly_report_application_number = a.refid
JOIN user_profile.m_user_master u 
    ON u.is_migrated = 1 AND u.migrated_user_id = a.created_by
WHERE a.status = 1
  AND a.label IN ('Oil', 'Gas', 'Condensate') 
--   AND a.refid = 'QPR-20221027041057';