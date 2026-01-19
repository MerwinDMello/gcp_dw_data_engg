
  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.hire_approval_workflow AS SELECT
      -- ATS/GHR
      w.workunit_sid,
      w.workunit_num,
      can.candidate_sid,
      varcand.variable_value_text AS candidate_num,
      varreq.variable_value_text AS requisition_num,
      CAST(NULL as INT64) AS employee_num,
      CAST(NULL as INT64) AS applicant_num,
      m.action_start_date_time AS hire_action_completed_date_time,
      m.action_taken_text AS action_desc,
      m.user_profile_id_text AS approver_34_login_code,
      m.activity_seq_num,
      w.source_system_code
    FROM
      {{ params.param_hr_views_dataset_name }}.hr_workunit AS w
      LEFT OUTER JOIN {{ params.param_hr_views_dataset_name }}.hr_workunit_metric AS m ON w.workunit_sid = m.workunit_sid
       AND date(m.valid_to_date) = '9999-12-31'
       AND upper(m.source_system_code) = 'B'
      LEFT OUTER JOIN {{ params.param_hr_views_dataset_name }}.hr_workunit_variable AS varreq ON w.workunit_sid = varreq.workunit_sid
       AND upper(varreq.variable_name) = 'JOBREQUISITION'
       AND date(varreq.valid_to_date) = '9999-12-31'
       AND upper(varreq.source_system_code) = 'B'
      LEFT OUTER JOIN {{ params.param_hr_views_dataset_name }}.hr_workunit_variable AS varcand ON w.workunit_sid = varcand.workunit_sid
       AND upper(varcand.variable_name) = 'CANDIDATE'
       AND date(varcand.valid_to_date) = '9999-12-31'
       AND upper(varcand.source_system_code) = 'B'
      LEFT OUTER JOIN {{ params.param_hr_views_dataset_name }}.hr_workunit_variable AS varcomp ON w.workunit_sid = varcomp.workunit_sid
       AND upper(varcomp.variable_name) = 'HRORGANIZATIONUNIT'
       AND date(varcomp.valid_to_date) = '9999-12-31'
       AND upper(varcomp.source_system_code) = 'B'
      LEFT OUTER JOIN {{ params.param_hr_views_dataset_name }}.candidate AS can ON cast(varcand.variable_value_text as int64) = can.candidate_num
       AND date(can.valid_to_date) = '9999-12-31'
       AND upper(can.source_system_code) = 'B'
    WHERE date(w.valid_to_date) = '9999-12-31'
     AND upper(w.source_system_code) = 'B'
     AND upper(w.flow_definition_text) IN ('HIREHCA', 'REHIREHCA', 'INTERNALTRANSFERHCA')
     AND upper(m.task_name) = 'HR REPS'
  UNION ALL
  SELECT
      -- Landmark
      w.workunit_sid,
      w.workunit_num,
      NULL AS candidate_sid,
      CAST(NULL as string) AS candidate_num,
      CAST(NULL as string) AS requisition_num,
      cast(varemp.variable_value_text as int64) AS employee_num,
      cast(varapp.variable_value_text as int64) AS applicant_num,
      m.action_start_date_time AS hire_action_completed_date_time,
      m.action_taken_text AS action_desc,
      m.user_profile_id_text AS approver_34_login_code,
      m.activity_seq_num,
      w.source_system_code
    FROM
      {{ params.param_hr_views_dataset_name }}.hr_workunit AS w
      LEFT OUTER JOIN {{ params.param_hr_views_dataset_name }}.hr_workunit_metric AS m ON w.workunit_sid = m.workunit_sid
       AND date(m.valid_to_date) = '9999-12-31'
       AND upper(m.source_system_code) = 'L'
      LEFT OUTER JOIN {{ params.param_hr_views_dataset_name }}.hr_workunit_variable AS varapp ON w.workunit_sid = varapp.workunit_sid
       AND upper(varapp.variable_name) = 'APPLICANT'
       AND date(varapp.valid_to_date) = '9999-12-31'
       AND upper(varapp.source_system_code) = 'L'
      LEFT OUTER JOIN {{ params.param_hr_views_dataset_name }}.hr_workunit_variable AS varemp ON w.workunit_sid = varemp.workunit_sid
       AND upper(varemp.variable_name) = 'EMPLOYEE'
       AND date(varemp.valid_to_date) = '9999-12-31'
       AND upper(varemp.source_system_code) = 'L'
    WHERE date(w.valid_to_date) = '9999-12-31'
     AND upper(w.source_system_code) = 'L'
     AND upper(w.flow_definition_text) = '1HIREAPPL'
     AND upper(m.task_name) = 'HR REPS'
  ;

