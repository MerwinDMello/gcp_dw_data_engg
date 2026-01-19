create or replace view `{{ params.param_hr_base_views_dataset_name }}.employee_position`
AS SELECT
    employee_position.employee_sid,
    employee_position.position_sid,
    employee_position.position_level_sequence_num,
    employee_position.eff_from_date,
    employee_position.valid_from_date,
    employee_position.valid_to_date,
    employee_position.eff_to_date,
    employee_position.fte_percent,
    employee_position.working_location_code,
    employee_position.schedule_work_code,
    employee_position.job_code,
    employee_position.employee_num,
    employee_position.pay_rate_amt,
    employee_position.last_update_date,
    employee_position.dept_sid,
    employee_position.account_unit_num,
    employee_position.gl_company_num,
    employee_position.lawson_company_num,
    employee_position.process_level_code,
    employee_position.active_dw_ind,
    employee_position.delete_ind,
    employee_position.source_system_code,
    employee_position.dw_last_update_date_time
  FROM
   {{ params.param_hr_core_dataset_name }}.employee_position;