WITH mapped AS (
    SELECT 
        a.refid,
        a.work_prog_qut_act,
        a.planned_qut_act,
        a.actual_qut_act,
        a.re_for_var_qut_act,
        a.cumulative_qut_act,
        a.remark_qut_act,
        a.work_prog_qut_exp,
        a.planned_qut_exp,
        a.actual_qut_exp,
        a.re_for_var_qut_exp,
        a.cumulative_qut_exp,
        a.per_ach_app_bug_qut_exp,
        u.user_id,
        a.label,
        CASE TRIM(a.label)
            WHEN 'Sand_Control' THEN 26
            WHEN 'Well_Completion' THEN 27
            WHEN 'Artificial_lift_Installation' THEN 28
            WHEN 'Well_Testing_DST_Pressure_Transient' THEN 29
            WHEN 'Fluid_Sampling_Analysis_PVT_Analysis' THEN 30
            WHEN 'Core_Analysis_Lab_Studies' THEN 31
            WHEN 'Reservoir_Modelling_Studies' THEN 32
            WHEN 'Development_Wells_To_Be_Drilled' THEN 33
            WHEN 'Surface_Facilities_To_Be_Deployed' THEN 34
            WHEN 'Studies_To_Be_Carried_Out' THEN 35
        END AS activity_id
    FROM dgh_staging.form_progress_report_sec a
    JOIN dgh_staging.frm_workitem_master_new fwmn ON fwmn.ref_id = a.refid
    JOIN user_profile.m_user_master u 
        ON u.is_migrated = 1 AND u.migrated_user_id = a.created_by
    WHERE a.status = 1
    --   AND a.refid = 'QPR-20221027041057'
      and a.type = 'DEVELOPMENT' 
      and a.label in
      (
      	'Sand_Control',
		'Well_Completion',
		'Artificial_lift_Installation',
		'Well_Testing_DST_Pressure_Transient',
		'Fluid_Sampling_Analysis_PVT_Analysis',
		'Core_Analysis_Lab_Studies',
		'Reservoir_Modelling_Studies',
		'Development_Wells_To_Be_Drilled',
		'Surface_Facilities_To_Be_Deployed',
		'Studies_To_Be_Carried_Out'
      )
)
INSERT INTO upstream_data_management.t_quarterly_report_developtment_activities (
    quarterly_report_id,
    activity_id,
    work_programme_mc_approved_activity_whole_fy,
    quarterly_activity_planned,
    quarterly_activity_actual,
    quarterly_activity_reason_for_variance,
    quarterly_activity_cumulative,
    quarterly_activity_remarks,
    work_programme_mc_approved_budget_whole_fy,
    quarterly_expenditure_planned,
    quarterly_expenditure_actual,
    quarterly_expenditure_reason_for_variance,
    quarterly_expenditure_cumulative,
    quarterly_expenditure_percentage_achieved,
    created_by,
    is_active,
    is_migrated
)
SELECT 
    h.quarterly_report_id,
    m.activity_id,
    m.work_prog_qut_act,
    m.planned_qut_act,
    m.actual_qut_act,
    m.re_for_var_qut_act,
    m.cumulative_qut_act,
    m.remark_qut_act,
    m.work_prog_qut_exp,
    m.planned_qut_exp,
    m.actual_qut_exp,
    m.re_for_var_qut_exp,
    m.cumulative_qut_exp,
    m.per_ach_app_bug_qut_exp,
    m.user_id,
    1,
    1
FROM mapped m
JOIN upstream_data_management.t_quarterly_report_header h
    ON h.quarterly_report_application_number = m.refid
