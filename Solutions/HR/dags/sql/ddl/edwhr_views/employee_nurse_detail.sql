/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.employee_nurse_detail AS SELECT
      a.employee_sid,
      a.license_num_text,
      a.report_date,
      a.ncsbn_id,
      a.employee_34_login_code,
      a.employee_num,
      a.lawson_company_num,
      a.process_level_code,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.employee_nurse_detail AS a
  ;

