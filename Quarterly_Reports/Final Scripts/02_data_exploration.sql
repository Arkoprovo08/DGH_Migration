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
            WHEN '2D_LKM_Acquired' THEN 1
            WHEN '2D_LKM_Processed' THEN 2
            WHEN '2D_LKM_Interpreted' THEN 3
            WHEN '3D_Sqkm_Acquired' THEN 4
            WHEN '3D_Sqkm_Processed' THEN 5
            WHEN '3D_Sqkm_Interpreted' THEN 6
            WHEN 'High_Resolution_3D_Survey_Acquired' THEN 7
            WHEN 'High_Resolution_3D_Survey_Processed' THEN 8
            WHEN 'High_Resolution_3D_Survey_Interpreted' THEN 9
            WHEN '2D_Reprocessed_Processed' THEN 10
            WHEN '2D_Reprocessed_Interpreted' THEN 11
            WHEN 'Exploration_Wells_To_Be_Drilled' THEN 12
            WHEN 'Appraisal_Wells_To_Be_Drilled' THEN 13
            WHEN 'Well_Testing_DST_Pressure_Transient' THEN 14
            WHEN 'Fluid_Sampling_Analysis_PVT_Analysis' THEN 15
            WHEN 'Core_Analysis_Lab_Studies' THEN 16
            WHEN 'Reservoir_Modelling_Studies' THEN 17
        END AS activity_id
    FROM dgh_staging.form_progress_report_sec a
    JOIN dgh_staging.frm_workitem_master_new fwmn ON fwmn.ref_id = a.refid
    JOIN user_profile.m_user_master u 
        ON u.is_migrated = 1 AND u.migrated_user_id = a.created_by
    WHERE a.status = 1
      AND a.refid = 'QPR-20221027041057' and a.type = 'EXPLORATION' and a.label in 
      (
        '2D_LKM_Acquired',
		'2D_LKM_Processed',
		'2D_LKM_Interpreted',
		'3D_Sqkm_Acquired',
		'3D_Sqkm_Processed',
		'3D_Sqkm_Interpreted',
		'High_Resolution_3D_Survey_Acquired',
		'High_Resolution_3D_Survey_Processed',
		'High_Resolution_3D_Survey_Interpreted',
		'2D_Reprocessed_Processed',
		'2D_Reprocessed_Interpreted',
		'Exploration_Wells_To_Be_Drilled',
		'Appraisal_Wells_To_Be_Drilled',
		'Well_Testing_DST_Pressure_Transient',
		'Fluid_Sampling_Analysis_PVT_Analysis',
		'Core_Analysis_Lab_Studies',
		'Reservoir_Modelling_Studies'
      )
)
INSERT INTO upstream_data_management.t_quarterly_report_exploration_activities (
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
