/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.employee_disciplinary_action AS SELECT
      a.employee_sid,
      a.disciplinary_ind,
      a.disciplinary_action_num,
      a.valid_from_date,
      a.valid_to_date,
      a.disciplinary_desc,
      a.creation_date,
      a.action_category_code,
      a.report_date,
      a.reported_by_employee_num,
      a.reported_by_name,
      a.action_status_code,
      a.action_outcome_desc,
      a.action_outcome_date,
      a.days_out_cnt,
      a.department_sid,
      a.location_code,
      a.job_code_sid,
      a.comment_desc,
      a.supervisor_employee_num,
      a.last_update_date,
      a.last_update_user_34_login_code,
      a.employee_num,
      a.lawson_company_num,
      a.process_level_code,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.employee_disciplinary_action AS a
  ;

