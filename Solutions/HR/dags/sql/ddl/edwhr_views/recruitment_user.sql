/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.recruitment_user AS SELECT
      a.recruitment_user_sid,
      a.valid_from_date,
      a.recruitment_user_num,
      a.valid_to_date,
      a.employee_num,
      a.employee_34_login_code,
      a.first_name,
      a.last_name,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.recruitment_user AS a
  ;

