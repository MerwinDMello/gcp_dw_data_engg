CREATE OR REPLACE VIEW {{ params.param_hr_stnd_views_dataset_name }}.fact_workunit_detail AS SELECT
    -- Landmark/Lawson
    q.workunit_num,
    q.activity_seq_num,
    q.lawson_company_num,
    varemp.variable_value_text AS employee_num,
    q.work_desc,
    act.activity_name,
    q.action_taken_text,
    act.start_date_time,
    act.end_date_time,
    act.user_profile_id_text AS user_34_id,
    varpl.variable_value_text AS process_level_code,
    q.source_system_code
  FROM
    {{ params.param_hr_base_views_dataset_name }}.hr_workunit_action_queue AS q
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_workunit_activity AS act ON q.workunit_num = act.workunit_num
     AND q.activity_seq_num = act.activity_seq_num
     AND upper(act.source_system_code) = 'L'
     AND date(act.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_workunit_metric AS m ON q.workunit_num = m.workunit_num
     AND q.activity_seq_num = m.activity_seq_num
     AND upper(m.source_system_code) = 'L'
     AND date(m.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_workunit_variable AS varemp ON q.workunit_num = varemp.workunit_num
     AND upper(varemp.variable_name) = 'EMPLOYEE'
     AND upper(varemp.source_system_code) = 'L'
     AND date(varemp.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_workunit_variable AS varpl ON q.workunit_num = varpl.workunit_num
     AND upper(varpl.variable_name) = 'INBPL'
     AND upper(varpl.source_system_code) = 'L'
     AND date(varpl.valid_to_date) = '9999-12-31'
  WHERE upper(q.source_system_code) = 'L'
   AND date(q.valid_to_date) = '9999-12-31'
UNION DISTINCT
SELECT DISTINCT
    -- ATS/GHR
    q.workunit_num,
    q.activity_seq_num,
    q.lawson_company_num,
    varemp.variable_value_text AS employee_num,
    q.work_desc,
    act.activity_name,
    q.action_taken_text,
    act.start_date_time,
    max(act.end_date_time) AS end_date_time,
    act.user_profile_id_text AS user_34_id,
    varprolvl.variable_value_text AS process_level_code,
    q.source_system_code
  FROM
    {{ params.param_hr_base_views_dataset_name }}.hr_workunit_action_queue AS q
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_workunit_activity AS act ON act.workunit_num = q.workunit_num
     AND act.activity_seq_num = q.activity_seq_num
     AND upper(act.source_system_code) = 'B'
     AND date(act.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_workunit_metric AS m ON m.workunit_num = q.workunit_num
     AND m.activity_seq_num = q.activity_seq_num
     AND upper(m.source_system_code) = 'B'
     AND date(m.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_workunit_variable AS varemp ON q.workunit_num = varemp.workunit_num
     AND upper(varemp.variable_name) = 'EMPLOYEE'
     AND upper(varemp.source_system_code) = 'B'
     AND date(varemp.valid_to_date) = '9999-12-31'
    LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_workunit_variable AS varprolvl ON q.workunit_num = varprolvl.workunit_num
     AND upper(varprolvl.variable_name) = 'PROCESS_LEVEL_CODE'
     AND upper(varprolvl.source_system_code) = 'B'
     AND date(varprolvl.valid_to_date) = '9999-12-31'
  WHERE upper(q.source_system_code) = 'B'
   AND date(q.valid_to_date) = '9999-12-31'
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12
;
