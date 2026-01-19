/***************************************************************************************
   B A S E   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_base_views_dataset_name}}.employee_nurse_detail AS SELECT
    employee_nurse_detail.employee_sid,
    employee_nurse_detail.license_num_text,
    employee_nurse_detail.report_date,
    employee_nurse_detail.ncsbn_id,
    employee_nurse_detail.employee_34_login_code,
    employee_nurse_detail.employee_num,
    employee_nurse_detail.lawson_company_num,
    employee_nurse_detail.process_level_code,
    employee_nurse_detail.source_system_code,
    employee_nurse_detail.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.employee_nurse_detail
;
